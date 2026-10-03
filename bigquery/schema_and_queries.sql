-- ============================================================================
-- CME Manufacturing — Gemini Enterprise Prompt Analytics Demo
-- BigQuery Schema & Executive Analytical Queries (`cme_ge_analytics.prompt_logs`)
-- Covers:
--   1. Native GE Chat + Enterprise Connectors (`GE_CHAT_CONNECTOR_SEARCH` - 400 logs)
--   2. Vertex AI Agent Engine 10-Agent Tree (`AGENT_TREE_EXECUTION` - 600 logs)
--   3. 100% Cloud DLP Record & Prompt De-identification (`cme-ge-prompt-deidentify-template`)
--   4. Post-DLP LLM Judge (`gemini-3.8-flash`) 4-Facet Classification & ROI Framework
-- ============================================================================

-- 1. Create Schema & Curated Prompt Telemetry Table
CREATE SCHEMA IF NOT EXISTS `cme_ge_analytics`
OPTIONS (
  location = 'US',
  description = 'CME Gemini Enterprise Curated, DLP-Deidentified & LLM-Judge-Classified Prompt Analytics'
);

CREATE OR REPLACE TABLE `cme_ge_analytics.prompt_logs` (
  insert_id STRING NOT NULL OPTIONS(description="Cloud Logging insertId for idempotent deduplication"),
  event_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the prompt was submitted"),
  interaction_type STRING NOT NULL OPTIONS(description="GE_CHAT_CONNECTOR_SEARCH (400 logs) or AGENT_TREE_EXECUTION (600 logs)"),
  trace_id STRING OPTIONS(description="Cloud Trace ID"),
  span_id STRING OPTIONS(description="Span ID"),
  anonymous_actor_hash STRING NOT NULL OPTIONS(description="Deterministic cryptographic hash of employee_email produced by Cloud DLP CryptoHashConfig (50 unique employees)"),
  department STRING OPTIONS(description="Employee Business Unit / department: 'Operations', 'Finance', or 'HR'"),
  employee_role STRING OPTIONS(description="Employee job title at CME"),
  plant_location STRING OPTIONS(description="CME manufacturing plant location: 'Detroit Stamping Plant', 'Toledo Assembly Plant', 'Houston Precision Machining', or 'Cleveland Foundry & Casting'"),
  shift STRING OPTIONS(description="Manufacturing shift: 'Shift 1 (Day)', 'Shift 2 (Swing)', or 'Shift 3 (Night)'"),
  pillar STRING OPTIONS(description="Business Pillar: 'Operations', 'Finance', or 'HR'"),
  root_agent STRING OPTIONS(description="Top-level Pillar Lead Agent ('1_Plant_Operations_Shift_Coordinator', '5_Plant_Controllership_Cost_Agent', '8_HR_Onboarding_I9_Verification_Agent') or 'NONE (Native GE Chat)'"),
  leaf_agent STRING OPTIONS(description="Delegated Specialist Agent ('1_Plant_Operations_Shift_Coordinator', '2_Predictive_Maintenance_PLC_Agent', '3_Supply_Chain_Expediting_Agent', '4_ISO_Quality_CAPA_Agent', '5_Plant_Controllership_Cost_Agent', '6_CapEx_Automation_ROI_Agent', '7_Direct_Procurement_Tariff_Agent', '8_HR_Onboarding_I9_Verification_Agent', '9_Payroll_Tax_Garnishment_Agent', '10_Workers_Comp_FMLA_OSHA_Agent') or 'NONE (Native GE Chat)'"),
  agent_tree_path STRING OPTIONS(description="Full hop path across the 10-Agent Tree or GE Chat Connectors"),
  connectors_queried ARRAY<STRING> OPTIONS(description="Enterprise Data Connectors queried in Native GE Chat ('SharePoint_Manufacturing_SOPs', 'ServiceNow_Plant_Tickets', 'Jira_Quality_CAPA', 'SAP_ERP_Materials', 'SAP_ERP_Financials', 'Google_Drive_Finance_Q3', 'SAP_ERP_Procurement', 'SharePoint_Vendor_Contracts', 'Workday_HR_Records', 'SharePoint_Union_Contracts', 'Workday_Payroll_Docs', 'Google_Drive_Garnishment_Orders', 'Google_Drive_Shift_Logs', 'ServiceNow_HR_Case_Mgmt', 'Confluence_Safety_OSHA_Policies')"),
  prompt_text_masked STRING OPTIONS(description="De-identified prompt text after Cloud DLP InfoTypeTransformations"),
  dlp_detected BOOL OPTIONS(description="True if Cloud DLP Inspect Template matched any sensitive infoType in prompt_text"),
  dlp_risk_level STRING OPTIONS(description="DLP severity level: 'NONE', 'LOW', 'MEDIUM', 'HIGH', or 'CRITICAL'"),
  dlp_finding_count INT64 OPTIONS(description="Number of distinct DLP infoTypes detected in the prompt"),
  dlp_info_types ARRAY<STRING> OPTIONS(description="Array of Cloud DLP infoTypes found ('US_SOCIAL_SECURITY_NUMBER', 'PERSON_NAME', 'EMAIL_ADDRESS', 'DATE_OF_BIRTH', 'FINANCIAL_ACCOUNT_NUMBER', 'US_BANK_ROUTING_MICR', 'MEDICAL_TERM')"),
  contains_ssn BOOL OPTIONS(description="True if US_SOCIAL_SECURITY_NUMBER was detected and masked"),
  contains_medical_phi BOOL OPTIONS(description="True if MEDICAL_TERM / clinical injury PHI was detected"),
  contains_financial_acct BOOL OPTIONS(description="True if FINANCIAL_ACCOUNT_NUMBER was detected"),
  is_toxic_combination BOOL OPTIONS(description="True if prompt combines SSN + Medical PHI or SSN + Bank Account"),
  is_shadow_prompt BOOL OPTIONS(description="True if an Operations employee pasted sensitive HR/SSN data into a non-HR agent or shift log search"),
  operational_entity STRING OPTIONS(description="Extracted factory entity (e.g. 'Line 3', 'Line 3 (ERR-PLC-409)', 'Line 3 / Lot #402', 'Apex Steel Lot #402')"),
  topic_cluster STRING OPTIONS(description="Semantic prompt cluster (e.g. 'Line 3 Hydraulic Press Jam & PLC Fault ERR-PLC-409', 'Line 3 Press Jam Injury: Workers\\' Comp & OSHA (SSN + Medical PHI)', 'Temp-to-Perm Onboarding CSV Paste (SSN + DOB)', 'Wage Garnishment & W-2c Payroll (SSN + Bank Acct)', 'CAPA-2026-114 & Apex Steel Lot #402 Quality Defect')"),

  -- ==========================================================================
  -- Post-DLP LLM Judge (`gemini-3.8-flash`) 4-Facet Classification & ROI Fields
  -- ==========================================================================
  functional_intent STRING OPTIONS(description="Facet 1 (Functional Intent): 'generation_drafting', 'transformation_summary', 'code_scripting', 'info_seeking_qa', 'data_extraction', or 'analytical_reasoning'"),
  task_complexity STRING OPTIONS(description="Facet 2 (Task Complexity): 'low' (~5 min manual baseline), 'medium' (~15 min manual baseline), or 'high' (~45 min manual baseline)"),
  specification_maturity STRUCT<
    level STRING OPTIONS(description="Facet 3 (Specification Maturity Level): 'level_1_underspecified', 'level_2_basic_directive', or 'level_3_well_engineered'"),
    has_persona BOOL OPTIONS(description="True if the user specified an explicit role or persona"),
    has_explicit_format BOOL OPTIONS(description="True if the user specified an explicit output format (table, JSON, bullet list, etc.)"),
    has_constraints BOOL OPTIONS(description="True if the user provided explicit boundaries, length, or scope constraints"),
    is_search_query_style BOOL OPTIONS(description="True if the user typed a short 3-5 word Google-style keyword search instead of a conversational prompt")
  > OPTIONS(description="Facet 3: Prompt Engineering & Specification Maturity breakdown from the Post-DLP LLM Judge"),
  grounding_analysis STRUCT<
    grounding_type STRING OPTIONS(description="Facet 4 (Grounding Type): 'enterprise_rag' (uses connectors/enterprise search), 'in_context_attachment' (user pasted data/logs into prompt), or 'parametric_general' (model pre-trained knowledge only)"),
    requires_enterprise_knowledge BOOL OPTIONS(description="True if answering the prompt accurately requires internal CME policies, plant telemetry, ERP/HR records, or SOPs"),
    target_corpus STRING OPTIONS(description="Target internal knowledge domain: 'hr_policy', 'product_docs', 'sales_collateral', 'it_support', 'codebase', or 'unknown'")
  > OPTIONS(description="Facet 4: Grounding & RAG Dependency analysis from the Post-DLP LLM Judge"),
  is_retrieval_gap BOOL OPTIONS(description="True when grounding_analysis.requires_enterprise_knowledge = TRUE AND grounding_analysis.grounding_type = 'parametric_general' (signals hallucination risk / missing connector invocation)"),
  estimated_manual_minutes INT64 OPTIONS(description="Unadjusted baseline manual minutes saved based on task_complexity (5 for low, 15 for medium, 45 for high)"),
  maturity_multiplier FLOAT64 OPTIONS(description="Quality/re-prompt multiplier based on specification_maturity.level (0.5 for Level 1, 0.75 for Level 2, 1.0 for Level 3)"),
  adjusted_minutes_saved FLOAT64 OPTIONS(description="Net productivity minutes saved per prompt = estimated_manual_minutes * maturity_multiplier"),

  latency_ms INT64 OPTIONS(description="End-to-end execution latency in milliseconds"),
  prompt_tokens INT64 OPTIONS(description="Input token count"),
  response_tokens INT64 OPTIONS(description="Output token count")
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY interaction_type, pillar, functional_intent, dlp_risk_level;


-- ============================================================================
-- QUERY 1 (Objective 1): Workforce Prompting Maturity Index (WMI) &
-- "Search-Engine Habit" Rate by Business Unit
-- ============================================================================
SELECT
  department AS business_unit,
  COUNT(*) AS total_prompts,
  COUNT(DISTINCT anonymous_actor_hash) AS unique_employees,
  ROUND(AVG(
    CASE specification_maturity.level
      WHEN 'level_1_underspecified' THEN 1.0
      WHEN 'level_2_basic_directive' THEN 2.0
      WHEN 'level_3_well_engineered' THEN 3.0
    END
  ), 2) AS workforce_maturity_index_1_to_3,
  ROUND(SAFE_DIVIDE(COUNTIF(specification_maturity.level = 'level_1_underspecified'), COUNT(*)) * 100, 1) AS level_1_underspecified_pct,
  ROUND(SAFE_DIVIDE(COUNTIF(specification_maturity.level = 'level_2_basic_directive'), COUNT(*)) * 100, 1) AS level_2_basic_directive_pct,
  ROUND(SAFE_DIVIDE(COUNTIF(specification_maturity.level = 'level_3_well_engineered'), COUNT(*)) * 100, 1) AS level_3_well_engineered_pct,
  ROUND(SAFE_DIVIDE(COUNTIF(specification_maturity.is_search_query_style), COUNT(*)) * 100, 1) AS search_query_habit_pct
FROM `cme_ge_analytics.prompt_logs`
GROUP BY business_unit
ORDER BY workforce_maturity_index_1_to_3 ASC;


-- ============================================================================
-- QUERY 2 (Objective 2): Grounding, Connector Utilization & Retrieval Gaps
-- Pinpoints where employees ask CME-specific questions without RAG grounding
-- ============================================================================
SELECT
  department AS business_unit,
  grounding_analysis.target_corpus AS target_knowledge_corpus,
  COUNT(*) AS total_prompts,
  COUNTIF(grounding_analysis.grounding_type = 'enterprise_rag') AS enterprise_rag_prompts,
  COUNTIF(grounding_analysis.grounding_type = 'in_context_attachment') AS pasted_in_context_prompts,
  COUNTIF(is_retrieval_gap) AS retrieval_gap_prompts,
  ROUND(SAFE_DIVIDE(COUNTIF(is_retrieval_gap), COUNT(*)) * 100, 1) AS retrieval_gap_rate_pct
FROM `cme_ge_analytics.prompt_logs`
GROUP BY business_unit, target_knowledge_corpus
ORDER BY retrieval_gap_prompts DESC, total_prompts DESC;


-- ============================================================================
-- QUERY 3 (Objective 3): Business Unit ROI & Productivity Value Formula
-- Value(BU) = SUM(Estimated_Minutes(Task_Complexity) * Maturity_Multiplier)
-- ============================================================================
SELECT
  department AS business_unit,
  functional_intent,
  COUNT(*) AS prompt_count,
  ROUND(SUM(estimated_manual_minutes) / 60.0, 1) AS raw_manual_hours_baseline,
  ROUND(AVG(maturity_multiplier), 2) AS avg_maturity_multiplier,
  ROUND(SUM(adjusted_minutes_saved) / 60.0, 1) AS net_adjusted_hours_saved,
  ROUND(AVG(adjusted_minutes_saved), 1) AS avg_net_minutes_saved_per_prompt
FROM `cme_ge_analytics.prompt_logs`
GROUP BY business_unit, functional_intent
ORDER BY business_unit, net_adjusted_hours_saved DESC;


-- ============================================================================
-- QUERY 4 (CISO Governance View): Sensitive Data (DLP) & Toxic Combinations
-- Across All 10 Agents + Native GE Chat Connectors
-- ============================================================================
SELECT
  interaction_type,
  pillar,
  leaf_agent,
  COUNT(*) AS total_prompts,
  COUNTIF(dlp_detected) AS dlp_flagged_prompts,
  COUNTIF(contains_ssn) AS ssn_prompts,
  COUNTIF(is_toxic_combination) AS toxic_combination_prompts,
  ROUND(SAFE_DIVIDE(COUNTIF(contains_ssn), COUNT(*)) * 100, 1) AS ssn_leak_rate_pct
FROM `cme_ge_analytics.prompt_logs`
GROUP BY interaction_type, pillar, leaf_agent
ORDER BY ssn_prompts DESC, total_prompts DESC;


-- ============================================================================
-- QUERY 5 (Cross-Pillar Story): "The Line 3 Hydraulic Press Story"
-- Correlates PLC Faults, Lot #402 Steel, Finance Scrap, and HR Injury Claims
-- ============================================================================
SELECT
  interaction_type,
  pillar,
  leaf_agent,
  topic_cluster,
  functional_intent,
  COUNT(*) AS prompt_volume,
  COUNTIF(contains_ssn AND contains_medical_phi) AS ssn_plus_injury_phi_claims,
  ROUND(SUM(adjusted_minutes_saved) / 60.0, 1) AS hours_saved_on_incident,
  ANY_VALUE(prompt_text_masked) AS sample_masked_prompt
FROM `cme_ge_analytics.prompt_logs`
WHERE operational_entity LIKE '%Line 3%'
   OR operational_entity LIKE '%Lot #402%'
GROUP BY interaction_type, pillar, leaf_agent, topic_cluster, functional_intent
ORDER BY pillar, interaction_type, prompt_volume DESC;
