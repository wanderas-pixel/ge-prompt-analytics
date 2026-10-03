#!/usr/bin/env python3
r"""
Creates (or updates) the two Google Cloud Sensitive Data Protection (DLP) Templates
for the CME Gemini Enterprise Prompt Analytics Demo:

1. Inspection Template: `cme-ge-prompt-inspect-template`
   - Built-in InfoTypes:
     * US_SOCIAL_SECURITY_NUMBER
     * PERSON_NAME
     * EMAIL_ADDRESS
     * DATE_OF_BIRTH
     * FINANCIAL_ACCOUNT_NUMBER
     * US_BANK_ROUTING_MICR
     * MEDICAL_TERM
   - Custom InfoType Detector:
     * CME_SYNTHETIC_US_SSN (regex `\b000-\d{2}-\d{4}\b` so synthetic 000-XX-XXXX demo SSNs
       are 100% detected alongside real SSNs).

2. De-identification Template: `cme-ge-prompt-deidentify-template`
   Uses `record_transformations` (`field_transformations`) across a structured 3-column
   `Table` (`employee_name`, `employee_email`, `prompt_text`) so 100% of anonymization
   happens inside Google Cloud DLP:
   - Field Rule 1 (`employee_name`): `RedactConfig` completely removes the employee's name.
   - Field Rule 2 (`employee_email`): `CryptoHashConfig` (HMAC-SHA256) deterministically hashes
     the employee's email into `anonymous_actor_hash` (mapping 50 employees -> 50 unique IDs).
   - Field Rule 3 (`prompt_text`): `InfoTypeTransformations` masks SSNs (`***-**-7742`) and
     `FINANCIAL_ACCOUNT_NUMBER` (`******8812`), replaces `PERSON_NAME`, `EMAIL_ADDRESS`,
     `DATE_OF_BIRTH`, and `US_BANK_ROUTING_MICR` with `[INFO_TYPE]` tags, and preserves
     `MEDICAL_TERM` text intact for safety root-cause analytics.

Usage:
  python3 scripts/setup_dlp_templates.py --project YOUR_GCP_PROJECT_ID [--location global] [--test]
"""

import argparse
import base64
import hashlib
import os
import secrets
import sys
from google.api_core import exceptions
from google.cloud import dlp_v2

INSPECT_TEMPLATE_ID = "cme-ge-prompt-inspect-template"
DEIDENTIFY_TEMPLATE_ID = "cme-ge-prompt-deidentify-template"


def _resolve_dlp_crypto_key() -> bytes:
    """
    Resolves a 32-byte (256-bit) AES/HMAC key for Cloud DLP CryptoHashConfig.
    Reads from CME_DLP_CRYPTO_KEY env var if provided; otherwise generates a
    cryptographically strong 32-byte key stored inside the saved Cloud DLP template.
    """
    env_seed = os.environ.get("CME_DLP_CRYPTO_KEY")
    if env_seed:
        return hashlib.sha256(env_seed.encode("utf-8")).digest()
    return secrets.token_bytes(32)


def get_inspect_template_config() -> dict:
    """Returns the Cloud DLP InspectTemplate configuration dictionary."""
    return {
        "display_name": "CME Gemini Enterprise Prompt Inspection Template",
        "description": (
            "Inspects CME employee prompts across Native GE Chat Connectors and the "
            "10-Agent Tree for SSNs, Person Names, Emails, DOBs, Bank Accounts, and Medical PHI."
        ),
        "inspect_config": {
            "info_types": [
                {"name": "US_SOCIAL_SECURITY_NUMBER"},
                {"name": "PERSON_NAME"},
                {"name": "EMAIL_ADDRESS"},
                {"name": "DATE_OF_BIRTH"},
                {"name": "FINANCIAL_ACCOUNT_NUMBER"},
                {"name": "US_BANK_ROUTING_MICR"},
                {"name": "MEDICAL_TERM"},
            ],
            "custom_info_types": [
                {
                    "info_type": {"name": "CME_SYNTHETIC_US_SSN"},
                    "likelihood": dlp_v2.Likelihood.VERY_LIKELY,
                    "regex": {"pattern": r"\b000-\d{2}-\d{4}\b"},
                }
            ],
            "min_likelihood": dlp_v2.Likelihood.POSSIBLE,
            "include_quote": False,
        },
    }


def get_deidentify_template_config() -> dict:
    """
    Returns the Cloud DLP DeidentifyTemplate configuration dictionary using
    record_transformations (field_transformations) so a single Cloud DLP call
    redacts employee_name, crypto-hashes employee_email, and masks prompt_text.
    """
    crypto_key_bytes = _resolve_dlp_crypto_key()
    return {
        "display_name": "CME Gemini Enterprise Record & Prompt De-identification Template",
        "description": (
            "All-in-DLP Record Transformation: Redacts employee_name, crypto-hashes "
            "employee_email into anonymous_actor_hash, and masks SSNs/Bank Accounts/PII in prompt_text."
        ),
        "deidentify_config": {
            "record_transformations": {
                "field_transformations": [
                    # Field Rule 1: Completely redact employee_name
                    {
                        "fields": [{"name": "employee_name"}],
                        "primitive_transformation": {
                            "redact_config": {}
                        },
                    },
                    # Field Rule 2: Deterministically hash employee_email via Cloud DLP CryptoHashConfig
                    {
                        "fields": [{"name": "employee_email"}],
                        "primitive_transformation": {
                            "crypto_hash_config": {
                                "crypto_key": {
                                    "unwrapped": {
                                        "key": crypto_key_bytes,
                                    }
                                }
                            }
                        },
                    },
                    # Field Rule 3: Free-text InfoType transformations on prompt_text
                    {
                        "fields": [{"name": "prompt_text"}],
                        "info_type_transformations": {
                            "transformations": [
                                # 3a: Partially mask SSNs (e.g. ***-**-7742) ignoring hyphens
                                {
                                    "info_types": [
                                        {"name": "US_SOCIAL_SECURITY_NUMBER"},
                                        {"name": "CME_SYNTHETIC_US_SSN"},
                                    ],
                                    "primitive_transformation": {
                                        "character_mask_config": {
                                            "masking_character": "*",
                                            "number_to_mask": 5,
                                            "reverse_order": False,
                                            "characters_to_ignore": [
                                                {"characters_to_skip": "-"}
                                            ],
                                        }
                                    },
                                },
                                # 3b: Partially mask Financial Account numbers (leave last 4 digits)
                                {
                                    "info_types": [
                                        {"name": "FINANCIAL_ACCOUNT_NUMBER"},
                                    ],
                                    "primitive_transformation": {
                                        "character_mask_config": {
                                            "masking_character": "*",
                                            "number_to_mask": 6,
                                            "reverse_order": False,
                                        }
                                    },
                                },
                                # 3c: Replace direct personal identifiers with [INFO_TYPE]
                                {
                                    "info_types": [
                                        {"name": "PERSON_NAME"},
                                        {"name": "EMAIL_ADDRESS"},
                                        {"name": "DATE_OF_BIRTH"},
                                        {"name": "US_BANK_ROUTING_MICR"},
                                    ],
                                    "primitive_transformation": {
                                        "replace_with_info_type_config": {}
                                    },
                                },
                            ]
                        },
                    },
                ]
            }
        },
    }


def upsert_templates(project_id: str, location: str = "global") -> tuple[str, str]:
    """Creates or updates the Inspect and Deidentify templates in Cloud DLP."""
    dlp = dlp_v2.DlpServiceClient()
    parent = f"projects/{project_id}/locations/{location}"

    # 1. Upsert Inspect Template
    inspect_name = f"{parent}/inspectTemplates/{INSPECT_TEMPLATE_ID}"
    inspect_template = get_inspect_template_config()
    try:
        resp_i = dlp.create_inspect_template(
            request={
                "parent": parent,
                "inspect_template": inspect_template,
                "template_id": INSPECT_TEMPLATE_ID,
            }
        )
        print(f"[CREATED] Inspect Template: {resp_i.name}")
    except (exceptions.AlreadyExists, exceptions.InvalidArgument):
        resp_i = dlp.update_inspect_template(
            request={
                "name": inspect_name,
                "inspect_template": inspect_template,
            }
        )
        print(f"[UPDATED] Inspect Template: {resp_i.name}")

    # 2. Upsert Deidentify Template
    deid_name = f"{parent}/deidentifyTemplates/{DEIDENTIFY_TEMPLATE_ID}"
    deid_template = get_deidentify_template_config()
    try:
        resp_d = dlp.create_deidentify_template(
            request={
                "parent": parent,
                "deidentify_template": deid_template,
                "template_id": DEIDENTIFY_TEMPLATE_ID,
            }
        )
        print(f"[CREATED] De-identify Template: {resp_d.name}")
    except (exceptions.AlreadyExists, exceptions.InvalidArgument):
        resp_d = dlp.update_deidentify_template(
            request={
                "name": deid_name,
                "deidentify_template": deid_template,
            }
        )
        print(f"[UPDATED] De-identify Template: {resp_d.name}")

    print("\n=== GCP Console Links (Show to Customer) ===")
    print(f"All DLP Templates List:\n  https://console.cloud.google.com/security/sensitive-data-protection/landing/configuration/templates?project={project_id}")
    print(f"Inspect Template Detail:\n  https://console.cloud.google.com/security/sensitive-data-protection/projects/{project_id}/locations/{location}/inspectTemplates/{INSPECT_TEMPLATE_ID}?project={project_id}")
    print(f"De-identify Template Detail:\n  https://console.cloud.google.com/security/sensitive-data-protection/projects/{project_id}/locations/{location}/deidentifyTemplates/{DEIDENTIFY_TEMPLATE_ID}?project={project_id}")

    return inspect_name, deid_name


def test_templates(project_id: str, inspect_name: str, deid_name: str, location: str = "global") -> None:
    """Runs a sample 3-column CME record through both saved templates to verify."""
    dlp = dlp_v2.DlpServiceClient()
    parent = f"projects/{project_id}/locations/{location}"

    sample_employee_name = "Allison Mitchell"
    sample_employee_email = "a.mitchell@cme-mfg.com"
    sample_prompt = (
        "Draft the state Workers' Comp claim narrative for Sarah Jenkins "
        "(Email: s.jenkins@cme-mfg.com, SSN: 000-31-7742, DOB: 08/19/1984) "
        "who suffered an L4-L5 lumbar disc herniation clearing a jammed press on Line 3."
    )

    inspect_resp = dlp.inspect_content(
        request={
            "parent": parent,
            "inspect_template_name": inspect_name,
            "item": {"value": sample_prompt},
        }
    )
    info_types = sorted({f.info_type.name for f in inspect_resp.result.findings})

    table_item = {
        "table": {
            "headers": [
                {"name": "employee_name"},
                {"name": "employee_email"},
                {"name": "prompt_text"},
            ],
            "rows": [
                {
                    "values": [
                        {"string_value": sample_employee_name},
                        {"string_value": sample_employee_email},
                        {"string_value": sample_prompt},
                    ]
                }
            ],
        }
    }

    deid_resp = dlp.deidentify_content(
        request={
            "parent": parent,
            "inspect_template_name": inspect_name,
            "deidentify_template_name": deid_name,
            "item": table_item,
        }
    )

    out_values = deid_resp.item.table.rows[0].values
    redacted_name = out_values[0].string_value
    dlp_crypto_hash_b64 = out_values[1].string_value
    masked_prompt = out_values[2].string_value
    try:
        actor_id = f"usr_{base64.b64decode(dlp_crypto_hash_b64).hex()[:8]}"
    except Exception:
        actor_id = f"usr_{dlp_crypto_hash_b64[:8]}"

    print("\n=== Live Cloud DLP Record & Prompt Template Test ===")
    print(f"BEFORE (employee_name):  {sample_employee_name}")
    print(f"AFTER  (RedactConfig):   {redacted_name!r}")
    print(f"BEFORE (employee_email): {sample_employee_email}")
    print(f"AFTER  (CryptoHashConfig): {dlp_crypto_hash_b64} -> anonymous_actor_hash: {actor_id}")
    print(f"BEFORE (prompt_text):\n  {sample_prompt}")
    print(f"DETECTED INFO_TYPES:\n  {info_types}")
    print(f"AFTER  (InfoTypeTransformations):\n  {masked_prompt}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create CME Cloud DLP Inspect & De-identify Templates")
    parser.add_argument(
        "--project",
        default=os.environ.get("GOOGLE_CLOUD_PROJECT", ""),
        help="GCP Project ID (or set GOOGLE_CLOUD_PROJECT)",
    )
    parser.add_argument("--location", default="global", help="DLP Template location (default: global)")
    parser.add_argument("--test", action="store_true", help="Run a live test prompt against the templates")
    args = parser.parse_args()

    if not args.project:
        print("Error: Please specify --project YOUR_PROJECT_ID or set GOOGLE_CLOUD_PROJECT.", file=sys.stderr)
        sys.exit(1)

    inspect_name, deid_name = upsert_templates(args.project, args.location)
    if args.test:
        test_templates(args.project, inspect_name, deid_name, args.location)


if __name__ == "__main__":
    main()
