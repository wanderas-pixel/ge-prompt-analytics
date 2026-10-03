#!/usr/bin/env python3
"""
Generator for CME (Custom Manufacturing Enterprise) Gemini Enterprise Prompt Analytics Demo.

Creates separate, easy-to-visualize folders inside `data/` for customer presentations:

1. `data/employees/cme_employees_50.json`
   - 50 synthetic CME employees across Operations (26), Finance (12), and HR (12)
     at 4 manufacturing plants, each with a unique deterministic `anonymous_actor_hash` (`usr_<8hex>`).

2. `data/raw_ge_chat_connector_logs/ge_chat_connector_logs_400.jsonl` (400 Logs)
   - Simulates users typing directly into the **Native Gemini Enterprise Chat Window**
     (`discoveryengine.googleapis.com` -> `AssistantService.StreamAssist`) where GE grounds
     responses via **Enterprise Connectors** (`SharePoint`, `ServiceNow`, `SAP`, `Google Drive`,
     `Workday`, `Jira`, `Confluence`) WITHOUT invoking a custom agent (`agent_invoked: null`).

3. `data/raw_agent_tree_logs/agent_tree_logs_600.jsonl` (600 Logs)
   - Simulates users invoking the **10 Custom ADK Agents on Vertex AI Agent Engine**
     (`aiplatform.googleapis.com/ReasoningEngine`), capturing `root_agent`, `leaf_agent`,
     and `agent_tree_path`.

4. `data/curated_bigquery_post_dlp/cme_bigquery_masked_1000.jsonl` (1,000 Logs)
   - The combined 1,000 post-DLP BigQuery rows showing how both Native GE Chat (Connector)
     logs and Agent Tree logs are anonymized via the Cloud DLP Templates
     (`cme-ge-prompt-inspect-template` & `cme-ge-prompt-deidentify-template`) and unified
     in BigQuery with 50 unique `anonymous_actor_hash` values.
"""

import datetime
import hashlib
import json
import pathlib
import random
import re

BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
EMPLOYEES_DIR = DATA_DIR / "employees"
RAW_CHAT_DIR = DATA_DIR / "raw_ge_chat_connector_logs"
RAW_AGENT_DIR = DATA_DIR / "raw_agent_tree_logs"
CURATED_BQ_DIR = DATA_DIR / "curated_bigquery_post_dlp"

PLANTS = [
    "Detroit Stamping Plant",
    "Toledo Assembly Plant",
    "Houston Precision Machining",
    "Cleveland Foundry & Casting",
]

SHIFTS = [
    "Shift 1 (Day)",
    "Shift 2 (Swing)",
    "Shift 3 (Night)",
]

FIRST_NAMES = [
    "Marcus", "Elena", "David", "Sarah", "Carlos", "Priya", "James", "Linda",
    "Robert", "Angela", "Michael", "Rachel", "William", "Fatima", "Thomas",
    "Hannah", "Daniel", "Sofia", "Kevin", "Meghan", "Anthony", "Grace",
    "Brian", "Chloe", "Jason", "Natalie", "Jeffrey", "Victoria", "Ryan",
    "Samantha", "Gary", "Lauren", "Nicholas", "Amber", "Eric", "Danielle",
    "Stephen", "Kayla", "Larry", "Brianna", "Justin", "Courtney", "Scott",
    "Rebecca", "Brandon", "Tiffany", "Benjamin", "Allison", "Samuel", "Vanessa",
]

LAST_NAMES = [
    "Thompson", "Rostova", "Kowalski", "Jenkins", "Rivera", "Patel", "Vance",
    "Chen", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
    "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson",
    "Martin", "Lee", "Perez", "White", "Harris", "Sanchez", "Clark", "Ramirez",
    "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott",
    "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams", "Nelson", "Baker",
    "Hall", "Campbell", "Mitchell", "Carter", "Roberts",
]

SUBJECT_WORKERS = [
    {"name": "Sarah Jenkins", "email": "s.jenkins@cme-mfg.com", "ssn": "000-31-7742", "dob": "08/19/1984", "acct": "4491028812", "routing": "072000326"},
    {"name": "Carlos Rivera", "email": "c.rivera.temp@staffing-partner.org", "ssn": "000-45-8921", "dob": "04/12/1988", "acct": "8831904421", "routing": "041000124"},
    {"name": "Marcus Vance", "email": "m.vance@cme-mfg.com", "ssn": "000-88-1940", "dob": "11/03/1979", "acct": "4491028899", "routing": "072000326"},
    {"name": "Devon Brooks", "email": "d.brooks@cme-mfg.com", "ssn": "000-22-6104", "dob": "02/27/1991", "acct": "7712093844", "routing": "111000614"},
    {"name": "Anita Desai", "email": "a.desai.temp@staffing-partner.org", "ssn": "000-63-4519", "dob": "09/14/1993", "acct": "5501928371", "routing": "041000124"},
    {"name": "Trevor Novak", "email": "t.novak@cme-mfg.com", "ssn": "000-19-8375", "dob": "06/05/1982", "acct": "3391827465", "routing": "072000326"},
    {"name": "Keisha Washington", "email": "k.washington@cme-mfg.com", "ssn": "000-74-2098", "dob": "12/22/1987", "acct": "9918273645", "routing": "021000021"},
    {"name": "Mateo Morales", "email": "m.morales.temp@staffing-partner.org", "ssn": "000-56-3310", "dob": "03/08/1995", "acct": "6627384910", "routing": "111000614"},
    {"name": "Raymond Kowalski", "email": "r.kowalski@cme-mfg.com", "ssn": "000-91-5482", "dob": "07/30/1976", "acct": "1182937460", "routing": "041000124"},
    {"name": "Elena Rostova", "email": "e.rostova@cme-mfg.com", "ssn": "000-38-9012", "dob": "01/17/1990", "acct": "2293847561", "routing": "072000326"},
]

AGENT_CATALOG = {
    1: {
        "id": "1_Plant_Operations_Shift_Coordinator",
        "name": "Plant Operations & Shift Coordinator",
        "pillar": "Operations",
        "reasoning_engine_id": "cme-ops-shift-coordinator-01",
        "tree_path": ["CME_Enterprise_Router", "1_Plant_Operations_Shift_Coordinator"],
    },
    2: {
        "id": "2_Predictive_Maintenance_PLC_Agent",
        "name": "Predictive Maintenance & PLC Agent",
        "pillar": "Operations",
        "reasoning_engine_id": "cme-ops-plc-maintenance-02",
        "tree_path": ["CME_Enterprise_Router", "1_Plant_Operations_Shift_Coordinator", "2_Predictive_Maintenance_PLC_Agent"],
    },
    3: {
        "id": "3_Supply_Chain_Expediting_Agent",
        "name": "Supply Chain & Expediting Agent",
        "pillar": "Operations",
        "reasoning_engine_id": "cme-ops-supply-expediting-03",
        "tree_path": ["CME_Enterprise_Router", "1_Plant_Operations_Shift_Coordinator", "3_Supply_Chain_Expediting_Agent"],
    },
    4: {
        "id": "4_ISO_Quality_CAPA_Agent",
        "name": "ISO Quality & CAPA Agent",
        "pillar": "Operations",
        "reasoning_engine_id": "cme-ops-quality-capa-04",
        "tree_path": ["CME_Enterprise_Router", "1_Plant_Operations_Shift_Coordinator", "4_ISO_Quality_CAPA_Agent"],
    },
    5: {
        "id": "5_Plant_Controllership_Cost_Agent",
        "name": "Plant Controllership & Cost Agent",
        "pillar": "Finance",
        "reasoning_engine_id": "cme-fin-controllership-05",
        "tree_path": ["CME_Enterprise_Router", "5_Plant_Controllership_Cost_Agent"],
    },
    6: {
        "id": "6_CapEx_Automation_ROI_Agent",
        "name": "CapEx & Automation ROI Agent",
        "pillar": "Finance",
        "reasoning_engine_id": "cme-fin-capex-roi-06",
        "tree_path": ["CME_Enterprise_Router", "5_Plant_Controllership_Cost_Agent", "6_CapEx_Automation_ROI_Agent"],
    },
    7: {
        "id": "7_Direct_Procurement_Tariff_Agent",
        "name": "Direct Procurement & Tariff Agent",
        "pillar": "Finance",
        "reasoning_engine_id": "cme-fin-procurement-tariff-07",
        "tree_path": ["CME_Enterprise_Router", "5_Plant_Controllership_Cost_Agent", "7_Direct_Procurement_Tariff_Agent"],
    },
    8: {
        "id": "8_HR_Onboarding_I9_Verification_Agent",
        "name": "HR Onboarding & I-9 Verification Agent",
        "pillar": "HR",
        "reasoning_engine_id": "cme-hr-onboarding-i9-08",
        "tree_path": ["CME_Enterprise_Router", "8_HR_Onboarding_I9_Verification_Agent"],
    },
    9: {
        "id": "9_Payroll_Tax_Garnishment_Agent",
        "name": "Payroll, Tax & Wage Garnishment Agent",
        "pillar": "HR",
        "reasoning_engine_id": "cme-hr-payroll-garnishment-09",
        "tree_path": ["CME_Enterprise_Router", "8_HR_Onboarding_I9_Verification_Agent", "9_Payroll_Tax_Garnishment_Agent"],
    },
    10: {
        "id": "10_Workers_Comp_FMLA_OSHA_Agent",
        "name": "Workers' Comp, FMLA & OSHA Claims Agent",
        "pillar": "HR",
        "reasoning_engine_id": "cme-hr-workers-comp-osha-10",
        "tree_path": ["CME_Enterprise_Router", "8_HR_Onboarding_I9_Verification_Agent", "10_Workers_Comp_FMLA_OSHA_Agent"],
    },
}


def compute_anonymous_actor_hash(email: str) -> str:
    """Computes a deterministic, one-way anonymous actor ID (usr_<8hex>) per employee email."""
    digest = hashlib.sha256(f"cme-ge-dlp-actor-domain-v1:{email.lower().strip()}".encode("utf-8")).hexdigest()
    return f"usr_{digest[:8]}"


def apply_dlp_masking(raw_prompt: str) -> str:
    """Applies exact Cloud DLP template transformation rules (`cme-ge-prompt-deidentify-template`)."""
    masked = raw_prompt
    masked = re.sub(r"\b000-\d{2}-(\d{4})\b", r"[US_SSN: ***-**-\1]", masked)
    masked = re.sub(r"Acct #(\d{6})(\d{4})\b", r"Acct #[BANK_ACCT: ******\2]", masked)
    masked = re.sub(r"Routing #\d{9}\b", r"Routing #[US_BANK_ROUTING_MICR]", masked)
    masked = re.sub(r"\b\d{2}/\d{2}/\d{4}\b", "[DATE_OF_BIRTH]", masked)
    masked = re.sub(
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
        "[EMAIL_ADDRESS]",
        masked,
    )
    for worker in SUBJECT_WORKERS:
        masked = masked.replace(worker["name"].title(), "[PERSON_NAME]")
    return masked


def build_employees_50() -> list[dict]:
    """Generates 50 unique CME employees across Operations (26), Finance (12), and HR (12)."""
    employees = []
    dept_specs = (
        [("Operations", "Shift Supervisor")] * 7
        + [("Operations", "PLC Maintenance Technician")] * 8
        + [("Operations", "Materials & Supply Expediter")] * 5
        + [("Operations", "ISO Quality Engineer")] * 6
        + [("Finance", "Plant Cost Controller")] * 5
        + [("Finance", "CapEx & Robotics Financial Analyst")] * 3
        + [("Finance", "Direct Procurement Specialist")] * 4
        + [("HR", "HR Onboarding & Staffing Coordinator")] * 5
        + [("HR", "Payroll & Garnishment Specialist")] * 4
        + [("HR", "HR Workers' Comp & Safety Case Manager")] * 3
    )
    assert len(dept_specs) == 50

    for idx in range(50):
        first = FIRST_NAMES[idx].strip().capitalize()
        last = LAST_NAMES[idx].strip().capitalize()
        full_name = f"{first} {last}"
        email = f"{first[0].lower()}.{last.lower()}@cme-mfg.com"
        dept, role = dept_specs[idx]

        plant = "Detroit Stamping Plant" if idx % 3 == 0 else PLANTS[idx % len(PLANTS)]
        shift = "Shift 3 (Night)" if (dept == "HR" and idx % 2 == 0) else SHIFTS[idx % len(SHIFTS)]

        employees.append({
            "employee_id": f"EMP-{1001 + idx}",
            "employee_name": full_name,
            "employee_email": email,
            "anonymous_actor_hash": compute_anonymous_actor_hash(email),
            "department": dept,
            "role": role,
            "plant_location": plant,
            "shift": shift,
        })

    hashes = {e["anonymous_actor_hash"] for e in employees}
    assert len(hashes) == 50, f"Expected 50 unique hashes, got {len(hashes)}"
    return employees


def generate_native_ge_chat_connector_prompt(emp: dict, rng: random.Random) -> dict:
    """
    Generates a prompt for Type 1: Native Gemini Enterprise Chat Window reaching out
    to Enterprise Connectors (SharePoint, ServiceNow, SAP, Google Drive, Workday, Jira, Confluence)
    WITHOUT invoking any custom agent.
    """
    subj = rng.choice(SUBJECT_WORKERS)
    subj_name = subj["name"].title()
    subj_email = subj["email"]
    subj_ssn = subj["ssn"]
    subj_dob = subj["dob"]
    subj_acct = subj["acct"]
    subj_routing = subj["routing"]
    plant = emp["plant_location"]
    line_num = "Line 3" if (plant == "Detroit Stamping Plant" or rng.random() < 0.65) else rng.choice(["Line 1", "Line 2", "Line 4"])

    info_types = []
    risk_level = "NONE"
    is_shadow_prompt = False

    if emp["department"] == "Operations":
        choice = rng.random()
        if choice < 0.45:
            connectors = ["SharePoint_Manufacturing_SOPs", "ServiceNow_Plant_Tickets"]
            err_code = "ERR-PLC-409" if line_num == "Line 3" else "ERR-HYD-302"
            raw_prompt = (
                f"Search SharePoint for the Schuler 800T Hydraulic Press maintenance manual and check open "
                f"ServiceNow tickets at {plant} for {line_num} proportional valve fault {err_code}."
            )
            topic_cluster = "Line 3 Hydraulic Press Jam & PLC Fault ERR-PLC-409" if line_num == "Line 3" else "PLC & Press Preventive Maintenance"
            operational_entity = f"{line_num} ({err_code})"
        elif choice < 0.85:
            connectors = ["Jira_Quality_CAPA", "SAP_ERP_Materials"]
            raw_prompt = (
                f"Look up the mill test certificate in SAP and open Jira CAPA tickets for Apex Cold-Rolled "
                f"Steel Lot #402 causing die jams on {line_num} at {plant}."
            )
            topic_cluster = "CAPA-2026-114 & Apex Steel Lot #402 Quality Defect"
            operational_entity = f"{line_num} / Lot #402"
        else:
            # Operations employee pasting SSN/PII directly into the main GE Chat Window while searching Google Drive
            connectors = ["Google_Drive_Shift_Logs", "Workday_HR_Records"]
            raw_prompt = (
                f"Search Google Drive shift logs and Workday for operator {subj_name} ({subj_email}, SSN: {subj_ssn}) "
                f"to verify their light-duty reassignment after the {line_num} press jam on {emp['shift']} at {plant}."
            )
            info_types = ["PERSON_NAME", "EMAIL_ADDRESS", "US_SOCIAL_SECURITY_NUMBER"]
            risk_level = "HIGH"
            topic_cluster = "Shadow Prompting: Shift Handover with Raw Employee SSN"
            operational_entity = line_num
            is_shadow_prompt = True

    elif emp["department"] == "Finance":
        if rng.random() < 0.65:
            connectors = ["SAP_ERP_Financials", "Google_Drive_Finance_Q3"]
            raw_prompt = (
                f"Search Google Drive Q3 close folders and SAP cost centers for {plant} {line_num} scrap variance "
                f"and overtime chargebacks tied to Apex Steel Lot #402."
            )
            topic_cluster = "Scrap Cost Variance & Margin Impact"
            operational_entity = f"{line_num} / Lot #402"
        else:
            connectors = ["SAP_ERP_Procurement", "SharePoint_Vendor_Contracts"]
            raw_prompt = (
                f"Find the master supplier quality agreement in SharePoint for Apex Metals account rep {subj_name} "
                f"({subj_email}) to initiate a $94,500 claim on Cold-Rolled Steel Lot #402 at {plant}."
            )
            info_types = ["PERSON_NAME", "EMAIL_ADDRESS"]
            risk_level = "LOW"
            topic_cluster = "Supplier Chargeback (Apex Steel Lot #402) & Tariffs"
            operational_entity = "Apex Steel Lot #402"

    else:  # HR users using the Native GE Chat Window to search Workday / SharePoint / Google Drive / ServiceNow
        choice = rng.random()
        if choice < 0.40:
            connectors = ["Workday_HR_Records", "SharePoint_Union_Contracts"]
            raw_prompt = (
                f"Search Workday and SharePoint for the temporary-to-permanent I-9 onboarding packet for {subj_name} "
                f"(Email: {subj_email}, SSN: {subj_ssn}, DOB: {subj_dob}) hired onto {emp['shift']} at {plant} ({line_num})."
            )
            info_types = ["PERSON_NAME", "EMAIL_ADDRESS", "US_SOCIAL_SECURITY_NUMBER", "DATE_OF_BIRTH"]
            risk_level = "HIGH"
            topic_cluster = "Temp-to-Perm Onboarding CSV Paste (SSN + DOB)"
            operational_entity = line_num
        elif choice < 0.72:
            connectors = ["Workday_Payroll_Docs", "Google_Drive_Garnishment_Orders"]
            raw_prompt = (
                f"Find the scanned wage garnishment order in Google Drive and Workday direct deposit profile for "
                f"{subj_name} ({subj_email}, SSN: {subj_ssn}, Acct #{subj_acct}, Routing #{subj_routing}) at {plant}."
            )
            info_types = [
                "PERSON_NAME",
                "EMAIL_ADDRESS",
                "US_SOCIAL_SECURITY_NUMBER",
                "FINANCIAL_ACCOUNT_NUMBER",
                "US_BANK_ROUTING_MICR",
            ]
            risk_level = "CRITICAL"
            topic_cluster = "Wage Garnishment & W-2c Payroll (SSN + Bank Acct)"
            operational_entity = line_num
        else:
            connectors = ["ServiceNow_HR_Case_Mgmt", "Confluence_Safety_OSHA_Policies"]
            err_tag = "ERR-PLC-409" if line_num == "Line 3" else "ERR-HYD-302"
            raw_prompt = (
                f"Search ServiceNow HR cases and Confluence OSHA 301 guidelines for {subj_name} "
                f"(Email: {subj_email}, SSN: {subj_ssn}, DOB: {subj_dob}) who suffered an L4-L5 lumbar disc herniation "
                f"clearing a jammed press on {line_num} (fault {err_tag}) at {plant}."
            )
            info_types = [
                "PERSON_NAME",
                "EMAIL_ADDRESS",
                "US_SOCIAL_SECURITY_NUMBER",
                "DATE_OF_BIRTH",
                "MEDICAL_TERM",
            ]
            risk_level = "CRITICAL"
            topic_cluster = (
                "Line 3 Press Jam Injury: Workers' Comp & OSHA (SSN + Medical PHI)"
                if line_num == "Line 3"
                else "Shop-Floor Injury: Workers' Comp & OSHA (SSN + Medical PHI)"
            )
            operational_entity = f"{line_num} ({err_tag})"

    masked_prompt = apply_dlp_masking(raw_prompt)
    return {
        "connectors_queried": connectors,
        "raw_prompt": raw_prompt,
        "masked_prompt": masked_prompt,
        "dlp_detected": len(info_types) > 0,
        "dlp_info_types": info_types,
        "dlp_finding_count": len(info_types),
        "dlp_risk_level": risk_level,
        "contains_ssn": "US_SOCIAL_SECURITY_NUMBER" in info_types,
        "contains_medical_phi": "MEDICAL_TERM" in info_types,
        "contains_financial_acct": "FINANCIAL_ACCOUNT_NUMBER" in info_types,
        "is_toxic_combination": (
            ("US_SOCIAL_SECURITY_NUMBER" in info_types)
            and ("MEDICAL_TERM" in info_types or "FINANCIAL_ACCOUNT_NUMBER" in info_types)
        ),
        "topic_cluster": topic_cluster,
        "operational_entity": operational_entity,
        "is_shadow_prompt": is_shadow_prompt,
    }


def generate_agent_tree_prompt(agent_num: int, emp: dict, rng: random.Random) -> dict:
    """Generates a prompt for Type 2: 10-Agent Tree execution on Vertex AI Agent Engine."""
    subj = rng.choice(SUBJECT_WORKERS)
    subj_name = subj["name"].title()
    subj_email = subj["email"]
    subj_ssn = subj["ssn"]
    subj_dob = subj["dob"]
    subj_acct = subj["acct"]
    subj_routing = subj["routing"]
    plant = emp["plant_location"]
    line_num = "Line 3" if (plant == "Detroit Stamping Plant" or rng.random() < 0.65) else rng.choice(["Line 1", "Line 2", "Line 4"])

    info_types = []
    risk_level = "NONE"
    topic_cluster = "General Manufacturing Query"
    operational_entity = line_num
    is_shadow_prompt = False

    if agent_num == 1:
        if rng.random() < 0.14:
            raw_prompt = (
                f"Summarize the {emp['shift']} handover log for {line_num} at {plant}. "
                f"Note that operator {subj_name} ({subj_email}, SSN: {subj_ssn}) was reassigned from "
                f"station 4B after the hydraulic press pressure fault and requested overtime payout."
            )
            info_types = ["PERSON_NAME", "EMAIL_ADDRESS", "US_SOCIAL_SECURITY_NUMBER"]
            risk_level = "HIGH"
            topic_cluster = "Shadow Prompting: Shift Handover with Raw Employee SSN"
            is_shadow_prompt = True
        else:
            templates = [
                f"Generate the {emp['shift']} shift handover summary for {line_num} at {plant}, highlighting OEE drop from 86% to 71% due to press stops.",
                f"What is the staffing coverage and press cycle-time target for {line_num} during {emp['shift']} at {plant}?",
                f"Compare unit throughput on {line_num} vs Line 2 at {plant} over the last 72 hours and flag top downtime reasons.",
                f"Draft a tier-2 morning standup brief for {plant} covering {line_num} changeover delays and die setup checklist completion.",
            ]
            raw_prompt = rng.choice(templates)
            topic_cluster = "Shift Handover & OEE Throughput"

    elif agent_num == 2:
        err_code = "ERR-PLC-409" if line_num == "Line 3" else rng.choice(["ERR-PLC-204", "ERR-SERVO-118", "ERR-HYD-302"])
        templates = [
            f"Analyze Siemens S7-1500 PLC fault code {err_code} on the {line_num} Schuler 800T Hydraulic Press at {plant}. Proportional pressure relief valve spiked to 310 bar before jamming.",
            f"Pull the last 48 hours of hydraulic cushion pressure telemetry for {line_num} at {plant} ({err_code}). Operators report repeated ram lockups requiring manual clearing.",
            f"What is the recommended lockout/tagout (LOTO) and solenoid replacement procedure for {err_code} on {line_num} stamping press at {plant}?",
            f"Correlate vibration sensor anomalies on {line_num} bolster plate at {plant} with fault {err_code} when stamping thicker cold-rolled steel coils.",
        ]
        raw_prompt = rng.choice(templates)
        topic_cluster = "Line 3 Hydraulic Press Jam & PLC Fault ERR-PLC-409" if line_num == "Line 3" else "PLC & Press Preventive Maintenance"
        operational_entity = f"{line_num} ({err_code})"

    elif agent_num == 3:
        if rng.random() < 0.20:
            raw_prompt = (
                f"Draft an urgent expedite notice to vendor contact {subj_name} ({subj_email}) regarding "
                f"delayed replacement Rexroth proportional valves (PO-88412) needed for {line_num} at {plant}."
            )
            info_types = ["PERSON_NAME", "EMAIL_ADDRESS"]
            risk_level = "LOW"
        else:
            raw_prompt = (
                f"Check ASN transit status for replacement hydraulic manifold valves (PO-88412) inbound to {plant} for {line_num} repair."
            )
        topic_cluster = "Spare Parts & Coil Expediting"

    elif agent_num == 4:
        templates = [
            f"Draft ISO 9001 CAPA-2026-114 for {line_num} at {plant}: excessive edge burrs and die galling traced to Apex Cold-Rolled Steel Lot #402 gauge thickness variance (+0.18mm).",
            f"Analyze Statistical Process Control (SPC) CpK drift on {line_num} stamped brackets at {plant} after switching to Apex Steel Coil Lot #402.",
            f"Generate an 8D root-cause containment report for {plant} {line_num} part jams caused by out-of-spec coil camber in Lot #402.",
        ]
        raw_prompt = rng.choice(templates)
        topic_cluster = "CAPA-2026-114 & Apex Steel Lot #402 Quality Defect"
        operational_entity = f"{line_num} / Lot #402"

    elif agent_num == 5:
        templates = [
            f"Calculate the weekly unfavorable scrap and overtime cost variance on {line_num} at {plant} ($142,400 MTD) driven by hydraulic press downtime and Lot #402 rework.",
            f"Reconcile standard vs actual machine-hour absorption for {plant} {line_num} during {emp['shift']} for the Q3 close package.",
            f"Break down the unit margin impact of 14% scrap rate on {line_num} automotive brackets at {plant} this month.",
        ]
        raw_prompt = rng.choice(templates)
        topic_cluster = "Scrap Cost Variance & Margin Impact"

    elif agent_num == 6:
        templates = [
            f"Model the 36-month NPV and IRR for a $680,000 CapEx retrofit adding automated robotic jam-clearing and servo-cushion upgrade on {line_num} at {plant}.",
            f"Calculate the payback period for replacing the legacy hydraulic press controls on {line_num} at {plant}, factoring in $142K/month scrap savings and reduced Workers' Comp claims.",
            f"Prepare the Capital Appropriation Request (CAR-2026-09) executive summary for {plant} {line_num} automated vision inspection cell.",
        ]
        raw_prompt = rng.choice(templates)
        topic_cluster = "Line 3 Press Retrofit CapEx ROI"

    elif agent_num == 7:
        if rng.random() < 0.25:
            raw_prompt = (
                f"Draft a formal supplier non-conformance chargeback letter to account rep {subj_name} ({subj_email}) "
                f"at Apex Metals for $94,500 covering off-spec Cold-Rolled Steel Lot #402 delivered to {plant}."
            )
            info_types = ["PERSON_NAME", "EMAIL_ADDRESS"]
            risk_level = "LOW"
        else:
            raw_prompt = (
                f"Evaluate Section 232 tariff impact and unit price variance for switching {plant} {line_num} "
                f"cold-rolled coil supply from Apex Metals (Lot #402) to Great Lakes Flat-Rolled Steel."
            )
        topic_cluster = "Supplier Chargeback (Apex Steel Lot #402) & Tariffs"
        operational_entity = "Apex Steel Lot #402"

    elif agent_num == 8:
        if rng.random() < 0.78:
            raw_prompt = (
                f"We are converting temp machinist {subj_name} (Email: {subj_email}, SSN: {subj_ssn}, DOB: {subj_dob}) "
                f"to full-time on {emp['shift']} at {plant} ({line_num}). Check why E-Verify flagged a tentative "
                f"nonconfirmation and draft the UAW Local 412 offer letter."
            )
            info_types = ["PERSON_NAME", "EMAIL_ADDRESS", "US_SOCIAL_SECURITY_NUMBER", "DATE_OF_BIRTH"]
            risk_level = "HIGH"
            topic_cluster = "Temp-to-Perm Onboarding CSV Paste (SSN + DOB)"
        else:
            raw_prompt = (
                f"What are the UAW Local 412 probationary seniority rules and I-9 document verification timelines "
                f"for new press operators hired onto {emp['shift']} at {plant}?"
            )
            topic_cluster = "Union Onboarding Policy & I-9 Rules"

    elif agent_num == 9:
        if rng.random() < 0.80:
            raw_prompt = (
                f"Extract the deduction terms from this court wage garnishment order for press operator {subj_name} "
                f"({subj_email}, SSN: {subj_ssn}, Direct Deposit Acct #{subj_acct}, Routing #{subj_routing}) at {plant} "
                f"and calculate the maximum bi-weekly withholding from $2,840.00 gross pay."
            )
            info_types = [
                "PERSON_NAME",
                "EMAIL_ADDRESS",
                "US_SOCIAL_SECURITY_NUMBER",
                "FINANCIAL_ACCOUNT_NUMBER",
                "US_BANK_ROUTING_MICR",
            ]
            risk_level = "CRITICAL"
            topic_cluster = "Wage Garnishment & W-2c Payroll (SSN + Bank Acct)"
        else:
            raw_prompt = (
                f"How do we calculate shift-differential overtime retro pay under the {plant} collective bargaining "
                f"agreement for {emp['shift']} maintenance technicians?"
            )
            topic_cluster = "Shift-Differential & Payroll Rules"

    elif agent_num == 10:
        injuries = [
            "an L4-L5 lumbar disc herniation",
            "a right distal radius fracture and tendon laceration",
            "an acute rotator cuff tear and shoulder impingement",
            "a bilateral crush contusion to the right hand",
        ]
        injury = rng.choice(injuries)
        err_tag = "ERR-PLC-409" if line_num == "Line 3" else "ERR-HYD-302"
        if rng.random() < 0.84:
            raw_prompt = (
                f"Draft the state Workers' Comp claim narrative and OSHA Form 301 incident entry for {subj_name} "
                f"(Email: {subj_email}, SSN: {subj_ssn}, DOB: {subj_dob}) who suffered {injury} while manually clearing "
                f"a jammed stamping die on {line_num} (fault {err_tag}) during {emp['shift']} at {plant}, and check "
                f"if a 6-week 15-lb lifting restriction qualifies for intermittent FMLA."
            )
            info_types = [
                "PERSON_NAME",
                "EMAIL_ADDRESS",
                "US_SOCIAL_SECURITY_NUMBER",
                "DATE_OF_BIRTH",
                "MEDICAL_TERM",
            ]
            risk_level = "CRITICAL"
            topic_cluster = (
                "Line 3 Press Jam Injury: Workers' Comp & OSHA (SSN + Medical PHI)"
                if line_num == "Line 3"
                else "Shop-Floor Injury: Workers' Comp & OSHA (SSN + Medical PHI)"
            )
            operational_entity = f"{line_num} ({err_tag})"
        else:
            raw_prompt = (
                f"Summarize OSHA 300A recordable vs DART rate reporting requirements for {plant} following "
                f"recent {line_num} ergonomic and press safety incidents."
            )
            topic_cluster = "OSHA 300A Recordable Compliance"

    masked_prompt = apply_dlp_masking(raw_prompt)
    return {
        "raw_prompt": raw_prompt,
        "masked_prompt": masked_prompt,
        "dlp_detected": len(info_types) > 0,
        "dlp_info_types": info_types,
        "dlp_finding_count": len(info_types),
        "dlp_risk_level": risk_level,
        "contains_ssn": "US_SOCIAL_SECURITY_NUMBER" in info_types,
        "contains_medical_phi": "MEDICAL_TERM" in info_types,
        "contains_financial_acct": "FINANCIAL_ACCOUNT_NUMBER" in info_types,
        "is_toxic_combination": (
            ("US_SOCIAL_SECURITY_NUMBER" in info_types)
            and ("MEDICAL_TERM" in info_types or "FINANCIAL_ACCOUNT_NUMBER" in info_types)
        ),
        "topic_cluster": topic_cluster,
        "operational_entity": operational_entity,
        "is_shadow_prompt": is_shadow_prompt,
    }


def main() -> None:
    rng = random.Random(20261001)
    for d in (EMPLOYEES_DIR, RAW_CHAT_DIR, RAW_AGENT_DIR, CURATED_BQ_DIR):
        d.mkdir(parents=True, exist_ok=True)

    employees = build_employees_50()
    employees_path = EMPLOYEES_DIR / "cme_employees_50.json"
    with open(employees_path, "w", encoding="utf-8") as f:
        json.dump(employees, f, indent=2)

    end_dt = datetime.datetime(2026, 10, 1, 18, 0, 0, tzinfo=datetime.timezone.utc)
    start_dt = end_dt - datetime.timedelta(days=14)

    # Guarantee all 50 employees appear across the 1,000 logs
    emp_schedule = []
    for emp in employees:
        emp_schedule.extend([emp] * 8)
    weights = [rng.uniform(0.5, 3.5) for _ in range(len(employees))]
    emp_schedule.extend(rng.choices(employees, weights=weights, k=1000 - len(emp_schedule)))
    rng.shuffle(emp_schedule)

    # Assign exactly 400 logs to Native GE Chat + Connectors and 600 logs to the 10-Agent Tree
    interaction_modes = (["GE_CHAT_CONNECTOR_SEARCH"] * 400) + (["AGENT_TREE_EXECUTION"] * 600)
    rng.shuffle(interaction_modes)

    offsets_sec = sorted(rng.randint(0, 14 * 86400) for _ in range(1000))

    raw_chat_logs = []
    raw_agent_logs = []
    bq_rows = []

    chat_seq = 0
    agent_seq = 0

    for i in range(1000):
        emp = emp_schedule[i]
        mode = interaction_modes[i]
        ts_dt = start_dt + datetime.timedelta(seconds=offsets_sec[i], milliseconds=rng.randint(0, 999))
        ts_iso = ts_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        trace_hex = hashlib.sha256(f"trace-{i}-{ts_iso}".encode()).hexdigest()[:32]
        span_hex = hashlib.sha256(f"span-{i}".encode()).hexdigest()[:16]
        latency_ms = rng.randint(580, 2450)

        if mode == "GE_CHAT_CONNECTOR_SEARCH":
            chat_seq += 1
            insert_id = f"cme-ge-chat-{200000 + chat_seq}"
            pdata = generate_native_ge_chat_connector_prompt(emp, rng)
            prompt_tokens = len(pdata["raw_prompt"].split()) * 2 + rng.randint(15, 40)
            response_tokens = rng.randint(130, 480)
            severity = "WARNING" if pdata["dlp_detected"] else "INFO"

            # Raw Cloud Logging Entry for Native GE Chat Window + Enterprise Connectors
            raw_chat_entry = {
                "insertId": insert_id,
                "logName": "projects/cme-manufacturing-prod/logs/cloudaudit.googleapis.com%2Fdata_access",
                "timestamp": ts_iso,
                "severity": severity,
                "trace": f"projects/cme-manufacturing-prod/traces/{trace_hex}",
                "spanId": span_hex,
                "resource": {
                    "type": "audited_resource",
                    "labels": {
                        "project_id": "cme-manufacturing-prod",
                        "service": "discoveryengine.googleapis.com",
                        "method": "google.cloud.discoveryengine.v1.AssistantService.StreamAssist",
                    },
                },
                "jsonPayload": {
                    "event_type": "GE_CHAT_CONNECTOR_SEARCH",
                    "interaction_type": "GE_CHAT_CONNECTOR_SEARCH",
                    "ge_app_id": "cme-enterprise-search-assistant",
                    "session_id": f"chat_sess_{trace_hex[:12]}",
                    "employee_id": emp["employee_id"],
                    "employee_name": emp["employee_name"],
                    "employee_email": emp["employee_email"],
                    "department": emp["department"],
                    "employee_role": emp["role"],
                    "plant_location": emp["plant_location"],
                    "shift": emp["shift"],
                    "pillar": emp["department"],
                    "agent_invoked": None,
                    "connectors_queried": pdata["connectors_queried"],
                    "prompt_text": pdata["raw_prompt"],
                    "operational_entity": pdata["operational_entity"],
                    "topic_cluster": pdata["topic_cluster"],
                    "latency_ms": latency_ms,
                    "prompt_tokens": prompt_tokens,
                    "response_tokens": response_tokens,
                },
            }
            raw_chat_logs.append(raw_chat_entry)

            bq_row = {
                "insert_id": insert_id,
                "event_timestamp": ts_iso,
                "interaction_type": "GE_CHAT_CONNECTOR_SEARCH",
                "trace_id": trace_hex,
                "span_id": span_hex,
                "anonymous_actor_hash": emp["anonymous_actor_hash"],
                "department": emp["department"],
                "employee_role": emp["role"],
                "plant_location": emp["plant_location"],
                "shift": emp["shift"],
                "pillar": emp["department"],
                "root_agent": "NONE (Native GE Chat)",
                "leaf_agent": "NONE (Native GE Chat)",
                "agent_tree_path": "GE_Chat_Window -> " + " + ".join(pdata["connectors_queried"]),
                "connectors_queried": pdata["connectors_queried"],
                "prompt_text_masked": pdata["masked_prompt"],
                "dlp_detected": pdata["dlp_detected"],
                "dlp_risk_level": pdata["dlp_risk_level"],
                "dlp_finding_count": pdata["dlp_finding_count"],
                "dlp_info_types": pdata["dlp_info_types"],
                "contains_ssn": pdata["contains_ssn"],
                "contains_medical_phi": pdata["contains_medical_phi"],
                "contains_financial_acct": pdata["contains_financial_acct"],
                "is_toxic_combination": pdata["is_toxic_combination"],
                "is_shadow_prompt": pdata["is_shadow_prompt"],
                "operational_entity": pdata["operational_entity"],
                "topic_cluster": pdata["topic_cluster"],
                "latency_ms": latency_ms,
                "prompt_tokens": prompt_tokens,
                "response_tokens": response_tokens,
            }
            bq_rows.append(bq_row)

        else:
            agent_seq += 1
            insert_id = f"cme-ge-agent-{100000 + agent_seq}"
            if emp["department"] == "Operations":
                agent_num = 8 if rng.random() < 0.05 else rng.choices([1, 2, 3, 4], weights=[25, 38, 17, 20])[0]
            elif emp["department"] == "Finance":
                agent_num = rng.choices([5, 6, 7], weights=[42, 28, 30])[0]
            else:
                agent_num = rng.choices([8, 9, 10], weights=[38, 30, 32])[0]

            agent = AGENT_CATALOG[agent_num]
            pdata = generate_agent_tree_prompt(agent_num, emp, rng)
            prompt_tokens = len(pdata["raw_prompt"].split()) * 2 + rng.randint(15, 45)
            response_tokens = rng.randint(140, 520)
            severity = "WARNING" if pdata["dlp_detected"] else "INFO"

            # Raw Cloud Logging Entry for Vertex AI Agent Engine (10-Agent Tree)
            raw_agent_entry = {
                "insertId": insert_id,
                "logName": "projects/cme-manufacturing-prod/logs/gemini-enterprise-agent-telemetry",
                "timestamp": ts_iso,
                "severity": severity,
                "trace": f"projects/cme-manufacturing-prod/traces/{trace_hex}",
                "spanId": span_hex,
                "resource": {
                    "type": "aiplatform.googleapis.com/ReasoningEngine",
                    "labels": {
                        "project_id": "cme-manufacturing-prod",
                        "location": "us-central1",
                        "reasoning_engine_id": agent["reasoning_engine_id"],
                    },
                },
                "jsonPayload": {
                    "event_type": "AGENT_PROMPT_EXECUTION",
                    "interaction_type": "AGENT_TREE_EXECUTION",
                    "session_id": f"agent_sess_{trace_hex[:12]}",
                    "employee_id": emp["employee_id"],
                    "employee_name": emp["employee_name"],
                    "employee_email": emp["employee_email"],
                    "department": emp["department"],
                    "employee_role": emp["role"],
                    "plant_location": emp["plant_location"],
                    "shift": emp["shift"],
                    "pillar": agent["pillar"],
                    "root_agent": agent["tree_path"][1],
                    "leaf_agent": agent["tree_path"][-1],
                    "agent_tree_path": agent["tree_path"],
                    "connectors_queried": [],
                    "prompt_text": pdata["raw_prompt"],
                    "operational_entity": pdata["operational_entity"],
                    "topic_cluster": pdata["topic_cluster"],
                    "latency_ms": latency_ms,
                    "prompt_tokens": prompt_tokens,
                    "response_tokens": response_tokens,
                },
            }
            raw_agent_logs.append(raw_agent_entry)

            bq_row = {
                "insert_id": insert_id,
                "event_timestamp": ts_iso,
                "interaction_type": "AGENT_TREE_EXECUTION",
                "trace_id": trace_hex,
                "span_id": span_hex,
                "anonymous_actor_hash": emp["anonymous_actor_hash"],
                "department": emp["department"],
                "employee_role": emp["role"],
                "plant_location": emp["plant_location"],
                "shift": emp["shift"],
                "pillar": agent["pillar"],
                "root_agent": agent["tree_path"][1],
                "leaf_agent": agent["tree_path"][-1],
                "agent_tree_path": " -> ".join(agent["tree_path"]),
                "connectors_queried": [],
                "prompt_text_masked": pdata["masked_prompt"],
                "dlp_detected": pdata["dlp_detected"],
                "dlp_risk_level": pdata["dlp_risk_level"],
                "dlp_finding_count": pdata["dlp_finding_count"],
                "dlp_info_types": pdata["dlp_info_types"],
                "contains_ssn": pdata["contains_ssn"],
                "contains_medical_phi": pdata["contains_medical_phi"],
                "contains_financial_acct": pdata["contains_financial_acct"],
                "is_toxic_combination": pdata["is_toxic_combination"],
                "is_shadow_prompt": pdata["is_shadow_prompt"],
                "operational_entity": pdata["operational_entity"],
                "topic_cluster": pdata["topic_cluster"],
                "latency_ms": latency_ms,
                "prompt_tokens": prompt_tokens,
                "response_tokens": response_tokens,
            }
            bq_rows.append(bq_row)

    chat_logs_path = RAW_CHAT_DIR / "ge_chat_connector_logs_400.jsonl"
    with open(chat_logs_path, "w", encoding="utf-8") as f:
        for entry in raw_chat_logs:
            f.write(json.dumps(entry) + "\n")

    agent_logs_path = RAW_AGENT_DIR / "agent_tree_logs_600.jsonl"
    with open(agent_logs_path, "w", encoding="utf-8") as f:
        for entry in raw_agent_logs:
            f.write(json.dumps(entry) + "\n")

    bq_rows_path = CURATED_BQ_DIR / "cme_bigquery_masked_1000.jsonl"
    with open(bq_rows_path, "w", encoding="utf-8") as f:
        for row in bq_rows:
            f.write(json.dumps(row) + "\n")

    unique_actors = {r["anonymous_actor_hash"] for r in bq_rows}
    ssn_count = sum(1 for r in bq_rows if r["contains_ssn"])
    dlp_count = sum(1 for r in bq_rows if r["dlp_detected"])
    toxic_count = sum(1 for r in bq_rows if r["is_toxic_combination"])

    print(f"1. Employees Roster (50)        -> {employees_path}")
    print(f"2. Raw GE Chat + Connectors (400)-> {chat_logs_path}")
    print(f"3. Raw 10-Agent Tree Logs (600)  -> {agent_logs_path}")
    print(f"4. Curated BigQuery Rows (1,000) -> {bq_rows_path}")
    print(f"   Unique anonymous_actor_hash   : {len(unique_actors)} (expected 50)")
    print(f"   Total DLP-flagged logs        : {dlp_count} / 1000")
    print(f"   Logs with SSNs                : {ssn_count} | Toxic combinations: {toxic_count}")


if __name__ == "__main__":
    main()
