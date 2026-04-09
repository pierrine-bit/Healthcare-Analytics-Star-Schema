
# Healthcare Analytics: OLTP to Star Schema

## Project Overview 

In this project, I transformed a normalized healthcare database (OLTP) into a star schema (OLAP) to improve analytical query performance. The original system was designed for transactional operations, but analytical queries were slow due to multiple joins and complex logic. To address this, I redesigned the schema and implemented an ETL pipeline that supports faster and more efficient reporting.

## Objectives

The main objective of this project was to analyze the performance limitations of a normalized OLTP schema and identify bottlenecks in analytical queries. Based on this analysis, I designed a star schema using the Kimball dimensional modeling approach and implemented an ETL pipeline to transform the data. Finally, I evaluated the performance improvements between the OLTP system and the new star schema.


## OLTP Schema 

The original database is fully normalized (3NF) and includes tables such as patients, providers, specialties, departments, encounters, diagnoses, procedures, encounter_diagnoses, encounter_procedures, and billing. While this design ensures high data integrity and reduces redundancy, it introduces performance challenges for analytical queries due to multiple joins and runtime computations.

## Star Schema Design 

The redesigned system follows a star schema centered around the **fact_encounter** table, which is defined at the grain of one row per encounter. This table stores key measures such as total_allowed_amount, diagnosis_count, and procedure_count, allowing efficient aggregation for reporting.

The schema includes several dimension tables, including dim_patient, dim_provider, dim_specialty, dim_department, dim_date, dim_encounter_type, dim_diagnosis, and dim_procedure. The dim_provider table is designed to support Slowly Changing Dimension (SCD Type 2) for future tracking of historical changes. The dim_date table contains precomputed attributes such as year, month, and quarter, eliminating the need for runtime date transformations.

To handle many-to-many relationships, bridge tables (bridge_encounter_diagnosis and bridge_encounter_procedure) are used. This design prevents duplication in the fact table while preserving the relationships between encounters, diagnoses, and procedures.

## ETL Pipeline

The ETL pipeline was implemented in a structured manner, where dimension tables are loaded first, followed by the fact table and then the bridge tables. During transformation, key metrics such as total revenue and counts of diagnoses and procedures are pre-aggregated to improve query performance. The pipeline uses surrogate keys for all dimensions, enforces referential integrity, and is designed to be idempotent, allowing safe re-execution without creating duplicates.

## Performance Summary

The following table summarizes the performance comparison between OLTP and star schema queries based on execution plans.

| Query | OLTP | Star Schema | Improvement |
|------|------|------------|------------|
| Encounters by Specialty | ~20–40 ms | ~5 ms | 4x–8x faster |
| Diagnosis–Procedure Pairs | ~16 ms | ~68 ms | Slower (row explosion) |
| Readmission Rate | ~3–8 ms | ~3.5 ms | Slight improvement |
| Revenue by Month | ~24 ms | ~7 ms | ~3x faster |

## Key Insights

From this analysis, I observed that the star schema significantly improves performance for aggregation queries by reducing join complexity and enabling pre-aggregation. The use of a date dimension simplifies time-based analysis, while bridge tables effectively manage many-to-many relationships. However, performance improvements are not universal. Queries involving combinations of diagnoses and procedures remain expensive due to inherent row explosion, demonstrating that schema design alone cannot eliminate all performance challenges.


## Trade-offs

The project highlights the trade-offs between normalized and denormalized designs. The OLTP schema provides high data integrity, minimal redundancy, and efficient transaction processing, but results in complex and slower analytical queries. In contrast, the star schema simplifies query structure and improves performance for analytics, at the cost of some data redundancy and the need for ETL processes to maintain consistency.

## Technologies Used

This project was implemented using PostgreSQL and SQL (DDL, DML, and ETL operations), with pgAdmin used for query execution and analysis. The design follows the Kimball dimensional modeling methodology.

## Project Structure

```

Healthcare-Analytics-Star-Schema/
│
├── query_analysis.txt
├── design_decisions.txt
├── star_schema.sql
├── star_schema_queries.txt
├── etl_design.txt
├── reflection.md
├── final_star_schema_etl.sql
└── README.md

```


## Key Achievements

In this project, I successfully designed a star schema aligned with analytical requirements and reduced query execution time by up to 4–8x for key aggregation queries. I implemented bridge tables to correctly model many-to-many relationships and built a structured ETL pipeline with proper load sequencing and data integrity. Additionally, I applied pre-aggregation techniques to improve performance and incorporated an SCD Type 2 structure to support future scalability. Importantly, I also identified and explained limitations of the star schema, particularly in cases involving row explosion.


## Conclusion

This project demonstrates how transforming an OLTP database into a star schema improves analytical performance while highlighting important trade-offs. It reflects practical data engineering concepts and provides insight into how real-world healthcare analytics systems are designed and optimized.

