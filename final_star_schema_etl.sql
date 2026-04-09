-- ============================================================
-- ETL SCRIPT: OLTP → STAR SCHEMA
-- ============================================================
-- Steps:
-- 1. Reset tables
-- 2. Load dimensions
-- 3. Load fact table
-- 4. Load bridge tables


-- ============================================================
-- STEP 0: RESET TABLES
-- ============================================================
TRUNCATE TABLE 
    bridge_encounter_procedure,
    bridge_encounter_diagnosis,
    fact_encounter,
    dim_provider,
    dim_patient,
    dim_date,
    dim_department,
    dim_specialty,
    dim_diagnosis,
    dim_procedure,
    dim_encounter_type
CASCADE;


-- ============================================================
-- STEP 1: LOAD DIM_DATE
-- ============================================================
INSERT INTO dim_date
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d AS full_date,
    EXTRACT(DAY FROM d) AS day,
    EXTRACT(MONTH FROM d) AS month,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(QUARTER FROM d) AS quarter,
    EXTRACT(WEEK FROM d) AS week_of_year,
    EXTRACT(DOW FROM d) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
    TRIM(TO_CHAR(d, 'Month')) AS month_name,
    'Q' || EXTRACT(QUARTER FROM d) AS quarter_name
FROM generate_series('2010-01-01'::date, '2030-12-31', interval '1 day') d
ON CONFLICT (date_key) DO NOTHING;


-- ============================================================
-- STEP 2: LOAD CORE DIMENSIONS
-- ============================================================

-- Specialty
INSERT INTO dim_specialty (specialty_id, specialty_name)
SELECT DISTINCT specialty_id, specialty_name 
FROM specialties;

-- Department
INSERT INTO dim_department (department_id, department_name)
SELECT DISTINCT department_id, department_name 
FROM departments;

-- Patient
INSERT INTO dim_patient (patient_id, first_name, last_name, gender, age_group)
SELECT 
    patient_id,
    first_name,
    last_name,
    gender,
    CASE 
        WHEN DATE_PART('year', AGE(date_of_birth)) < 18 THEN 'Child'
        WHEN DATE_PART('year', AGE(date_of_birth)) < 65 THEN 'Adult'
        ELSE 'Senior'
    END
FROM patients;


-- ============================================================
-- STEP 3: LOAD DIM_PROVIDER (SCD TYPE 2 STRUCTURE)
-- ============================================================
-- NOTE:
-- This implementation includes SCD Type 2 structure.
-- Full change tracking logic is not implemented due to static dataset,
-- but the schema supports future updates.

INSERT INTO dim_provider (
    provider_id,
    full_name,
    specialty_key,
    department_key,
    effective_date,
    end_date,
    is_current
)
SELECT 
    p.provider_id,
    p.first_name || ' ' || p.last_name AS full_name,
    ds.specialty_key,
    dd.department_key,
    CURRENT_DATE,
    NULL,
    TRUE
FROM providers p
JOIN dim_specialty ds 
    ON p.specialty_id = ds.specialty_id
JOIN dim_department dd 
    ON p.department_id = dd.department_id;


-- ============================================================
-- STEP 4: LOAD LOOKUP DIMENSIONS
-- ============================================================

-- Encounter type
INSERT INTO dim_encounter_type (encounter_type_name)
SELECT DISTINCT encounter_type 
FROM encounters;

-- Diagnosis
INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, description)
SELECT DISTINCT diagnosis_id, icd10_code, icd10_description 
FROM diagnoses;

-- Procedure
INSERT INTO dim_procedure (procedure_id, cpt_code, description)
SELECT DISTINCT procedure_id, cpt_code, cpt_description 
FROM procedures;


-- ============================================================
-- STEP 5: LOAD FACT_ENCOUNTER
-- ============================================================
INSERT INTO fact_encounter (
    encounter_id,
    patient_key,
    provider_key,
    encounter_type_key,
    date_key,
    total_allowed_amount,
    diagnosis_count,
    procedure_count
)
SELECT 
    e.encounter_id,
    dp.patient_key,
    dpr.provider_key,
    det.encounter_type_key,
    TO_CHAR(e.encounter_date, 'YYYYMMDD')::INT AS date_key,
    COALESCE(SUM(b.allowed_amount), 0) AS total_allowed_amount,
    COUNT(DISTINCT ed.diagnosis_id) AS diagnosis_count,
    COUNT(DISTINCT ep.procedure_id) AS procedure_count
FROM encounters e
JOIN dim_patient dp 
    ON e.patient_id = dp.patient_id
JOIN dim_provider dpr 
    ON e.provider_id = dpr.provider_id
JOIN dim_encounter_type det 
    ON e.encounter_type = det.encounter_type_name
LEFT JOIN billing b 
    ON e.encounter_id = b.encounter_id
LEFT JOIN encounter_diagnoses ed 
    ON e.encounter_id = ed.encounter_id
LEFT JOIN encounter_procedures ep 
    ON e.encounter_id = ep.encounter_id
GROUP BY 
    e.encounter_id,
    dp.patient_key,
    dpr.provider_key,
    det.encounter_type_key,
    e.encounter_date;


-- ============================================================
-- STEP 6: LOAD BRIDGE TABLES
-- ============================================================

-- Encounter ↔ Diagnosis
INSERT INTO bridge_encounter_diagnosis (encounter_key, diagnosis_key)
SELECT DISTINCT
    f.encounter_key,
    d.diagnosis_key
FROM encounter_diagnoses ed
JOIN fact_encounter f 
    ON ed.encounter_id = f.encounter_id
JOIN dim_diagnosis d 
    ON ed.diagnosis_id = d.diagnosis_id;

-- Encounter ↔ Procedure
INSERT INTO bridge_encounter_procedure (encounter_key, procedure_key)
SELECT DISTINCT
    f.encounter_key,
    p.procedure_key
FROM encounter_procedures ep
JOIN fact_encounter f 
    ON ep.encounter_id = f.encounter_id
JOIN dim_procedure p 
    ON ep.procedure_id = p.procedure_id;


-- ============================================================
-- STEP 7: VALIDATION CHECKS
-- ============================================================

-- Row counts
SELECT COUNT(*) AS fact_rows FROM fact_encounter;
SELECT COUNT(*) AS diagnosis_bridge_rows FROM bridge_encounter_diagnosis;
SELECT COUNT(*) AS procedure_bridge_rows FROM bridge_encounter_procedure;

-- NULL checks
SELECT COUNT(*) AS null_patient_keys 
FROM fact_encounter 
WHERE patient_key IS NULL;

SELECT COUNT(*) AS null_provider_keys 
FROM fact_encounter 
WHERE provider_key IS NULL;

-- Data integrity checks
SELECT COUNT(*) AS negative_revenue 
FROM fact_encounter 
WHERE total_allowed_amount < 0;

-- Duplicate detection
SELECT encounter_id, COUNT(*) 
FROM fact_encounter
GROUP BY encounter_id
HAVING COUNT(*) > 1;