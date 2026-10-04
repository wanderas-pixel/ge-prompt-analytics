#!/usr/bin/env python3
"""
2nd Gen Cloud Run Function: `cme-dlp-log-processor`

Triggered via Cloud Pub/Sub Push from the Cloud Logging Sink (`cme-ge-prompt-sink`).
Processes BOTH types of Gemini Enterprise logs:
  - Type 1: `GE_CHAT_CONNECTOR_SEARCH` (400 Native GE Chat + Enterprise Connector logs)
  - Type 2: `AGENT_TREE_EXECUTION` (600 Vertex AI Agent Engine 10-Agent Tree logs)

Pipeline Stages in-flight before streaming to BigQuery:
1. Stage 1 — 100% Cloud DLP Record & Text De-identification (`cme-ge-prompt-deidentify-template`):
   Sends a 3-column structured `Table` (`employee_name`, `employee_email`, `prompt_text`) to
   Google Cloud Sensitive Data Protection (DLP):
     * `employee_name`  -> Redacted completely via Cloud DLP `RedactConfig`
     * `employee_email` -> Deterministically hashed via Cloud DLP `CryptoHashConfig` (`anonymous_actor_hash`)
     * `prompt_text`    -> Masked/Redacted via Cloud DLP `InfoTypeTransformations` (`prompt_text_masked`)
2. Stage 2 — Post-DLP LLM Judge & Prompt Coach (`gemini-3.8-flash` on Vertex AI):
   Evaluates ONLY the DLP-sanitized `prompt_text_masked` (never raw PII) against the
   5-Facet Multi-Dimensional Prompt Classification, ROI & Coaching Framework using Structured Outputs:
     * Facet 1: Functional Intent (`functional_intent`)
     * Facet 2: Task Complexity (`task_complexity`, `estimated_manual_minutes`)
     * Facet 3: Specification Maturity (`specification_maturity`, `maturity_multiplier`, `adjusted_minutes_saved`)
     * Facet 4: Grounding & RAG Dependency (`grounding_analysis`, `is_retrieval_gap`)
     * Facet 5: Actionable Prompt Coaching & Level 3 Rewrite (`prompt_coaching`, `potential_extra_minutes_unlocked`)
3. Stage 3 — BigQuery Streaming Insert:
   Streams the anonymized + LLM-Judge-enriched telemetry row to `cme_ge_analytics.prompt_logs`.
"""

import base64
import json
import logging
import os
from typing import Literal
import functions_framework
from cloudevents.http import CloudEvent
from google import genai
from google.genai import types
from google.cloud import bigquery
from google.cloud import dlp_v2
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "cme-manufacturing-prod")
DLP_LOCATION = os.environ.get("DLP_LOCATION", "global")
VERTEX_LOCATION = os.environ.get("VERTEX_LOCATION", "global")
LLM_JUDGE_MODEL = os.environ.get("LLM_JUDGE_MODEL", "gemini-3.8-flash")
INSPECT_TEMPLATE_ID = os.environ.get("DLP_INSPECT_TEMPLATE_ID", "cme-ge-prompt-inspect-template")
DEIDENTIFY_TEMPLATE_ID = os.environ.get("DLP_DEIDENTIFY_TEMPLATE_ID", "cme-ge-prompt-deidentify-template")
BQ_DATASET = os.environ.get("BQ_DATASET", "cme_ge_analytics")
BQ_TABLE = os.environ.get("BQ_TABLE", "prompt_logs")

MATURITY_MULTIPLIERS = {
    "level_1_underspecified": 0.5,
    "level_2_basic_directive": 0.75,
    "level_3_well_engineered": 1.0,
}

LLM_JUDGE_SYSTEM_INSTRUCTION = """You are an Enterprise AI Telemetry Judge and Prompt Engineering Coach for CME Manufacturing.
You evaluate DLP-sanitized employee prompts (`prompt_text_masked`) submitted to Gemini Enterprise.
Note that sensitive spans have already been masked by Cloud DLP (e.g., `***-**-7742`, `******8812`, `[PERSON_NAME]`, `[EMAIL_ADDRESS]`, `[DATE_OF_BIRTH]`).

Classify the prompt and provide actionable coaching across 5 orthogonal facets:

1. FUNCTIONAL INTENT (`functional_intent`):
   - `generation_drafting`: Creating new content, emails, incident narratives, CAPA reports, or SOP drafts.
   - `transformation_summary`: Summarizing, translating, reformatting, or rewriting text/logs provided in the prompt.
   - `code_scripting`: Writing, debugging, or explaining SQL, Python, PLC ladder logic, or regex.
   - `info_seeking_qa`: Asking a question to retrieve facts, policies, troubleshooting steps, or explanations.
   - `data_extraction`: Parsing unstructured text, CSV pastes, or logs into structured tables/JSON.
   - `analytical_reasoning`: Root-cause analysis, financial variance modeling, tariff scenarios, or multi-step reasoning.

2. TASK COMPLEXITY (`task_complexity`):
   - `low`: Simple lookup, single-step question, or basic rewrite (typically 5 minutes manual effort).
   - `medium`: Multi-source synthesis, structured drafting, or standard troubleshooting (typically 15 minutes manual effort).
   - `high`: Deep cross-system root-cause analysis, complex code/PLC/SQL generation, or regulatory/financial modeling (typically 45 minutes manual effort).

3. SPECIFICATION MATURITY (`specification_maturity`):
   - `level_1_underspecified`: Keyword-only, vague, single-line, or 3-to-5 word Google-style search query lacking context or output format.
   - `level_2_basic_directive`: Clear goal with basic context, but missing explicit constraints, persona, or structured format specs.
   - `level_3_well_engineered`: Rich operational context, explicit constraints, role/persona, or structured output formatting.
   Also set boolean flags: `has_persona`, `has_explicit_format`, `has_constraints`, and `is_search_query_style`.

4. GROUNDING & RAG DEPENDENCY (`grounding_analysis`):
   - `enterprise_rag`: Prompt explicitly queries or relies on internal CME enterprise connectors, systems, or proprietary records.
   - `in_context_attachment`: User pasted the raw data/CSV/log snippet directly into the prompt text for processing.
   - `parametric_general`: Relies only on the model's general pre-trained world knowledge with no connector or pasted data.
   - Set `requires_enterprise_knowledge = true` if answering accurately requires CME-specific internal policies, plant telemetry, ERP/HR records, or SOPs.
   - Map `target_corpus` to the closest domain: `hr_policy` (HR/Workday/Union/OSHA/Payroll), `product_docs` (Manufacturing SOPs/Quality/CAPA/Plant Engineering), `sales_collateral` (Finance/Controllership/CapEx/Procurement/Vendor), `it_support` (ServiceNow/IT/Plant Tickets), `codebase` (PLC/SQL/Python), or `unknown`.

5. ACTIONABLE PROMPT COACHING & LEVEL 3 REWRITE (`prompt_coaching`):
   - Identify `missing_elements` from: `persona_role`, `explicit_output_format`, `scope_constraints`, `enterprise_rag_connector`, `pii_minimization` (include `pii_minimization` whenever masked SSNs `***-**-` or bank accounts `******` appear in the prompt).
   - Set `needs_improvement = true` if `specification_maturity.level != 'level_3_well_engineered'`, or if `grounding_type == 'parametric_general'` when `requires_enterprise_knowledge == true`, or if `pii_minimization` is needed.
   - Write a concise 1-2 sentence `improvement_recommendation` explaining specifically how the employee can improve the prompt (including how to reference employee/claim IDs instead of pasting raw SSNs when applicable).
   - Provide `rewritten_level_3_prompt`: A complete, ready-to-use Gold-Standard Level 3 version of the prompt that incorporates an appropriate CME role/persona, explicit structured output format (e.g., Markdown table or checklist), clear plant/shift/lot constraints, and proper grounding without raw PII.
   - Set `recommended_target_agent_or_connector` to the most appropriate CME Specialist Agent (`1_Plant_Operations_Shift_Coordinator`, `2_Predictive_Maintenance_PLC_Agent`, `3_Supply_Chain_Expediting_Agent`, `4_ISO_Quality_CAPA_Agent`, `5_Plant_Controllership_Cost_Agent`, `6_CapEx_Automation_ROI_Agent`, `7_Direct_Procurement_Tariff_Agent`, `8_HR_Onboarding_I9_Verification_Agent`, `9_Payroll_Tax_Garnishment_Agent`, `10_Workers_Comp_FMLA_OSHA_Agent`) or CME Enterprise Connector (`SharePoint_Manufacturing_SOPs`, `ServiceNow_Plant_Tickets`, `Jira_Quality_CAPA`, `SAP_ERP_Materials`, `SAP_ERP_Financials`, `Google_Drive_Finance_Q3`, `SAP_ERP_Procurement`, `SharePoint_Vendor_Contracts`, `Workday_HR_Records`, `SharePoint_Union_Contracts`, `Workday_Payroll_Docs`, `Google_Drive_Garnishment_Orders`, `ServiceNow_HR_Case_Mgmt`, `Confluence_Safety_OSHA_Policies`).
"""


class SpecificationMaturity(BaseModel):
    level: Literal[
        "level_1_underspecified",
        "level_2_basic_directive",
        "level_3_well_engineered",
    ]
    has_persona: bool
    has_explicit_format: bool
    has_constraints: bool
    is_search_query_style: bool


class GroundingAnalysis(BaseModel):
    grounding_type: Literal[
        "enterprise_rag",
        "in_context_attachment",
        "parametric_general",
    ]
    requires_enterprise_knowledge: bool
    target_corpus: Literal[
        "hr_policy",
        "product_docs",
        "sales_collateral",
        "it_support",
        "codebase",
        "unknown",
    ]


class PromptCoaching(BaseModel):
    needs_improvement: bool = Field(
        description="True if the prompt can be improved in specification maturity, enterprise RAG grounding, or PII minimization."
    )
    missing_elements: list[
        Literal[
            "persona_role",
            "explicit_output_format",
            "scope_constraints",
            "enterprise_rag_connector",
            "pii_minimization",
        ]
    ] = Field(
        description="List of prompt engineering or governance elements missing from the original prompt."
    )
    improvement_recommendation: str = Field(
        description="Concise 1-2 sentence actionable coaching recommendation on how to improve the prompt."
    )
    rewritten_level_3_prompt: str = Field(
        description="Complete Gold-Standard Level 3 rewritten version of the prompt adding persona, explicit output format, constraints, and safe enterprise grounding."
    )
    recommended_target_agent_or_connector: str = Field(
        description="Recommended CME Specialist Agent (#1-#10) or Enterprise Connector best suited for this task."
    )


class LLMJudgeClassification(BaseModel):
    functional_intent: Literal[
        "generation_drafting",
        "transformation_summary",
        "code_scripting",
        "info_seeking_qa",
        "data_extraction",
        "analytical_reasoning",
    ]
    task_complexity: Literal["low", "medium", "high"]
    specification_maturity: SpecificationMaturity
    grounding_analysis: GroundingAnalysis
    estimated_manual_minutes: int = Field(
        description="Estimated manual minutes saved before maturity multiplier (e.g., 5 for low, 15 for medium, 45 for high)."
    )
    prompt_coaching: PromptCoaching


dlp_client = dlp_v2.DlpServiceClient()
bq_client = bigquery.Client(project=PROJECT_ID)
genai_client = genai.Client(vertexai=True, project=PROJECT_ID, location=VERTEX_LOCATION)


def format_dlp_actor_hash(dlp_crypto_hash_b64: str) -> str:
    """Formats the base64 HMAC digest returned by Cloud DLP CryptoHashConfig into `usr_<8hex>`."""
    if not dlp_crypto_hash_b64:
        return "usr_anonymous"
    try:
        hex_digest = base64.b64decode(dlp_crypto_hash_b64).hex()
        return f"usr_{hex_digest[:8]}"
    except Exception:
        clean = "".join(ch for ch in dlp_crypto_hash_b64 if ch.isalnum()).lower()
        return f"usr_{clean[:8]}"


def score_dlp_risk(info_types: list[str]) -> tuple[str, bool]:
    """Calculates DLP risk severity and checks for toxic combinations."""
    if not info_types:
        return "NONE", False
    has_ssn = "US_SOCIAL_SECURITY_NUMBER" in info_types
    has_phi = "MEDICAL_TERM" in info_types
    has_fin = "FINANCIAL_ACCOUNT_NUMBER" in info_types or "US_BANK_ROUTING_MICR" in info_types
    is_toxic = has_ssn and (has_phi or has_fin)

    if is_toxic:
        return "CRITICAL", True
    if has_ssn or has_phi or has_fin:
        return "HIGH", False
    if "DATE_OF_BIRTH" in info_types:
        return "MEDIUM", False
    return "LOW", False


def evaluate_prompt_with_llm_judge(
    masked_prompt: str,
    department: str,
    interaction_type: str,
    leaf_agent: str,
    connectors_queried: list[str],
    dlp_info_types: list[str],
) -> LLMJudgeClassification:
    """
    Invokes Gemini 3.8 Flash (`gemini-3.8-flash`) strictly AFTER Cloud DLP de-identification
    to classify the masked prompt and generate a Level 3 Coaching Rewrite.
    """
    judge_input = (
        f"Department: {department}\n"
        f"Interaction Mode: {interaction_type}\n"
        f"Invoked Agent: {leaf_agent}\n"
        f"Connectors Queried: {', '.join(connectors_queried) if connectors_queried else 'None'}\n"
        f"DLP InfoTypes Detected & Masked: {', '.join(dlp_info_types) if dlp_info_types else 'None'}\n"
        f"DLP-Masked Prompt Text:\n{masked_prompt}"
    )

    response = genai_client.models.generate_content(
        model=LLM_JUDGE_MODEL,
        contents=judge_input,
        config=types.GenerateContentConfig(
            system_instruction=LLM_JUDGE_SYSTEM_INSTRUCTION,
            temperature=0.0,
            response_mime_type="application/json",
            response_schema=LLMJudgeClassification,
        ),
    )
    if response.parsed is not None:
        return response.parsed
    return LLMJudgeClassification.model_validate_json(response.text)


@functions_framework.cloud_event
def process_prompt_log(cloud_event: CloudEvent) -> None:
    """Entry point for Pub/Sub push messages from Cloud Logging Sink."""
    pubsub_message = cloud_event.data.get("message", {})
    raw_b64 = pubsub_message.get("data", "")
    if not raw_b64:
        logger.warning("Received empty Pub/Sub message data; skipping.")
        return

    log_entry = json.loads(base64.b64decode(raw_b64).decode("utf-8"))
    payload = log_entry.get("jsonPayload", {})
    employee_name = payload.get("employee_name", "")
    employee_email = payload.get("employee_email", "")
    raw_prompt = payload.get("prompt_text", "")
    interaction_type = payload.get("interaction_type", "AGENT_TREE_EXECUTION")

    # =========================================================================
    # STAGE 1: 100% Cloud DLP Inspection & Record De-identification
    # =========================================================================
    parent = f"projects/{PROJECT_ID}/locations/{DLP_LOCATION}"
    inspect_template_name = f"{parent}/inspectTemplates/{INSPECT_TEMPLATE_ID}"
    deid_template_name = f"{parent}/deidentifyTemplates/{DEIDENTIFY_TEMPLATE_ID}"

    # 1a. Inspect prompt_text for sensitive InfoTypes
    inspect_resp = dlp_client.inspect_content(
        request={
            "parent": parent,
            "inspect_template_name": inspect_template_name,
            "item": {"value": raw_prompt},
        }
    )

    detected_types_set = set()
    for finding in inspect_resp.result.findings:
        itype = finding.info_type.name
        if itype == "CME_SYNTHETIC_US_SSN":
            itype = "US_SOCIAL_SECURITY_NUMBER"
        detected_types_set.add(itype)
    info_types = sorted(detected_types_set)

    # 1b. De-identify the 3-column record (employee_name, employee_email, prompt_text) in Cloud DLP
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
                        {"string_value": employee_name},
                        {"string_value": employee_email},
                        {"string_value": raw_prompt},
                    ]
                }
            ],
        }
    }

    deid_resp = dlp_client.deidentify_content(
        request={
            "parent": parent,
            "inspect_template_name": inspect_template_name,
            "deidentify_template_name": deid_template_name,
            "item": table_item,
        }
    )

    out_values = deid_resp.item.table.rows[0].values
    # out_values[0] is employee_name (redacted to "" by Cloud DLP RedactConfig and dropped)
    dlp_crypto_hash_b64 = out_values[1].string_value
    masked_prompt = out_values[2].string_value
    anonymous_actor_hash = format_dlp_actor_hash(dlp_crypto_hash_b64)

    risk_level, is_toxic = score_dlp_risk(info_types)
    connectors = payload.get("connectors_queried", []) or []

    if interaction_type == "GE_CHAT_CONNECTOR_SEARCH":
        root_agent = "NONE (Native GE Chat)"
        leaf_agent = "NONE (Native GE Chat)"
        agent_tree_str = "GE_Chat_Window -> " + " + ".join(connectors)
    else:
        root_agent = payload.get("root_agent", "")
        leaf_agent = payload.get("leaf_agent", "")
        tree_path = payload.get("agent_tree_path", [])
        agent_tree_str = " -> ".join(tree_path) if isinstance(tree_path, list) else str(tree_path)

    # =========================================================================
    # STAGE 2: Post-DLP LLM Judge & Prompt Coach (`gemini-3.8-flash`)
    # =========================================================================
    department = payload.get("department", "Unknown")
    judge_result = evaluate_prompt_with_llm_judge(
        masked_prompt=masked_prompt,
        department=department,
        interaction_type=interaction_type,
        leaf_agent=leaf_agent,
        connectors_queried=connectors,
        dlp_info_types=info_types,
    )

    maturity_level = judge_result.specification_maturity.level
    maturity_multiplier = MATURITY_MULTIPLIERS.get(maturity_level, 0.75)
    adjusted_minutes_saved = round(
        judge_result.estimated_manual_minutes * maturity_multiplier, 2
    )
    potential_extra_minutes_unlocked = round(
        judge_result.estimated_manual_minutes * (1.0 - maturity_multiplier), 2
    )
    is_retrieval_gap = bool(
        judge_result.grounding_analysis.requires_enterprise_knowledge
        and judge_result.grounding_analysis.grounding_type == "parametric_general"
    )

    # =========================================================================
    # STAGE 3: BigQuery Streaming Insert (`cme_ge_analytics.prompt_logs`)
    # =========================================================================
    insert_id = log_entry.get("insertId") or cloud_event["id"]
    trace_full = log_entry.get("trace", "")
    trace_id = trace_full.split("/")[-1] if "/" in trace_full else trace_full

    bq_row = {
        "insert_id": insert_id,
        "event_timestamp": log_entry.get("timestamp"),
        "interaction_type": interaction_type,
        "trace_id": trace_id,
        "span_id": log_entry.get("spanId", ""),
        "anonymous_actor_hash": anonymous_actor_hash,
        "department": department,
        "employee_role": payload.get("employee_role", "Unknown"),
        "plant_location": payload.get("plant_location", "Unknown"),
        "shift": payload.get("shift", "Unknown"),
        "pillar": payload.get("pillar", "Unknown"),
        "root_agent": root_agent,
        "leaf_agent": leaf_agent,
        "agent_tree_path": agent_tree_str,
        "connectors_queried": connectors,
        "prompt_text_masked": masked_prompt,
        "dlp_detected": len(info_types) > 0,
        "dlp_risk_level": risk_level,
        "dlp_finding_count": len(info_types),
        "dlp_info_types": info_types,
        "contains_ssn": "US_SOCIAL_SECURITY_NUMBER" in info_types,
        "contains_medical_phi": "MEDICAL_TERM" in info_types,
        "contains_financial_acct": "FINANCIAL_ACCOUNT_NUMBER" in info_types,
        "is_toxic_combination": is_toxic,
        "is_shadow_prompt": (
            department == "Operations" and "US_SOCIAL_SECURITY_NUMBER" in info_types
        ),
        "operational_entity": payload.get("operational_entity", ""),
        "topic_cluster": payload.get("topic_cluster", ""),
        # Post-DLP LLM Judge (`gemini-3.8-flash`) 5-Facet Classification, ROI & Coaching Columns
        "functional_intent": judge_result.functional_intent,
        "task_complexity": judge_result.task_complexity,
        "specification_maturity": judge_result.specification_maturity.model_dump(),
        "grounding_analysis": judge_result.grounding_analysis.model_dump(),
        "is_retrieval_gap": is_retrieval_gap,
        "estimated_manual_minutes": judge_result.estimated_manual_minutes,
        "maturity_multiplier": maturity_multiplier,
        "adjusted_minutes_saved": adjusted_minutes_saved,
        "potential_extra_minutes_unlocked": potential_extra_minutes_unlocked,
        "prompt_coaching": judge_result.prompt_coaching.model_dump(),
        "latency_ms": int(payload.get("latency_ms", 0)),
        "prompt_tokens": int(payload.get("prompt_tokens", 0)),
        "response_tokens": int(payload.get("response_tokens", 0)),
    }

    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    errors = bq_client.insert_rows_json(table_ref, [bq_row], row_ids=[insert_id])
    if errors:
        logger.error("BigQuery streaming insert failed: %s", errors)
        raise RuntimeError(f"BigQuery insert error: {errors}")

    logger.info(
        "Processed log %s (%s) -> actor=%s dlp_risk=%s intent=%s maturity=%s saved_min=%.2f extra_min=%.2f",
        insert_id,
        interaction_type,
        anonymous_actor_hash,
        risk_level,
        judge_result.functional_intent,
        maturity_level,
        adjusted_minutes_saved,
        potential_extra_minutes_unlocked,
    )
