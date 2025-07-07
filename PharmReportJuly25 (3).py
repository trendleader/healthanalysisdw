# Databricks notebook source
df = spark.sql("""
SELECT 
  try_cast(c.member_id AS BIGINT) AS member_id, 
  p.claim_id, 
  p.drug_name, 
  p.billed_amount, 
  p.paid_amount,
  p.fill_date
FROM workspace.default.claims c
JOIN workspace.default.fact_pharm p
  ON try_cast(c.member_id AS BIGINT) = try_cast(p.member_id AS BIGINT)
LIMIT 5
""")
display(df)

# COMMAND ----------

df = spark.sql("""
SELECT 
  try_cast(c.member_id AS BIGINT) AS member_id, 
  p.claim_id, 
  COUNT(*) AS claim_count,
  p.drug_name, 
  ROUND(SUM(p.billed_amount),2) AS total_billed, 
  ROUND(SUM(p.paid_amount),2) AS total_paid,
  MONTHNAME(p.fill_date) AS fill_month
FROM workspace.default.claims c
JOIN workspace.default.fact_pharm p
  ON try_cast(c.member_id AS BIGINT) = try_cast(p.member_id AS BIGINT)
  GROUP BY TRY_CAST(c.member_id AS BIGINT), p.claim_id, p.drug_name, p.fill_date
LIMIT 5
""")
display(df)

# COMMAND ----------

df = spark.sql("""
WITH ranked_drugs AS (
  SELECT 
    try_cast(c.member_id AS BIGINT) AS member_id, 
    p.claim_id, 
    COUNT(*) AS claim_count,
    p.drug_name, 
    ROUND(SUM(p.billed_amount),2) AS total_billed, 
    ROUND(SUM(p.paid_amount),2) AS total_paid,
    MONTHNAME(p.fill_date) AS fill_month,
    ROW_NUMBER() OVER (ORDER BY SUM(p.billed_amount) DESC) AS rn
  FROM workspace.default.claims c
  JOIN workspace.default.fact_pharm p
    ON try_cast(c.member_id AS BIGINT) = try_cast(p.member_id AS BIGINT)
  GROUP BY TRY_CAST(c.member_id AS BIGINT), p.claim_id, p.drug_name, p.fill_date
)
SELECT 
  member_id,
  claim_id,
  claim_count,
  drug_name,
  total_billed,
  total_paid,
  fill_month
FROM ranked_drugs
WHERE rn = 1
--LIMIT 5
""")
display(df)