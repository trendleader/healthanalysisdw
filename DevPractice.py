# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM claims LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH DruGTotals AS 
# MAGIC (
# MAGIC   SELECT 
# MAGIC     c.member_id, 
# MAGIC     p.drug_name, 
# MAGIC     COUNT(*) AS DrugCount
# MAGIC   FROM 
# MAGIC     claims c
# MAGIC   INNER JOIN 
# MAGIC     pharmacy_claims p
# MAGIC   ON 
# MAGIC     try_cast(c.member_id AS BIGINT) = try_cast(p.member_id AS BIGINT)
# MAGIC   GROUP BY 
# MAGIC     c.member_id, 
# MAGIC     p.drug_name
# MAGIC )
# MAGIC SELECT 
# MAGIC DISTINCT
# MAGIC   member_id,
# MAGIC   drug_name,
# MAGIC   DrugCount
# MAGIC FROM
# MAGIC   DruGTotals
# MAGIC ORDER BY
# MAGIC   DrugCount DESC;

# COMMAND ----------

query = """
SELECT 
    c.member_id, 
    p.drug_name, 
    pr.provider_id,
    p.fill_date,
    COUNT(*) AS DrugCount
FROM 
    claims c
INNER JOIN 
    pharmacy_claims p
ON 
    try_cast(c.member_id AS BIGINT) = try_cast(p.member_id AS BIGINT)
INNER JOIN 
    providers pr
ON
    pr.Provider_id = c.ProviderID
GROUP BY 
    c.member_id, 
    p.drug_name, 
    pr.provider_id,
    p.fill_date
LIMIT 5
"""

result_df = spark.sql(query)
display(result_df)

# COMMAND ----------

query = """
SELECT 
    c.member_id, 
    p.drug_name, 
    pr.provider_id,
    p.fill_date,
    py.payor_type,
    SUM(p.billed_amount) as total_billed,
    SUM(p.paid_amount) as total_paid
FROM 
    claims c
INNER JOIN 
    pharmacy_claims p
ON 
    try_cast(c.member_id AS BIGINT) = try_cast(p.member_id AS BIGINT)
INNER JOIN 
    providers pr
ON
    pr.Provider_id = c.ProviderID

INNER JOIN 
    payor py
    ON try_cast(c.member_id AS BIGINT) = try_cast(py.member_id AS BIGINT)

GROUP BY 
    c.member_id, 
    p.drug_name, 
    pr.provider_id,
    p.fill_date,
    py.payor_type
LIMIT 20
"""

result_df = spark.sql(query)
display(result_df)

# COMMAND ----------

claims_df = spark.table("default.claims")



# COMMAND ----------

claims_df_with_age_groups = claims_df.withColumn(
    "age_group",
    when(col("AGE") < 18, "Under 18")
    .when(col("AGE").between(18, 34), "18-34")
    .when(col("AGE").between(35, 54), "35-54")
    .when(col("AGE").between(55, 64), "55-64")
    .otherwise("65+")
)

age_analysis = claims_df_with_age_groups.groupBy("age_group", "Sex") \
    .agg(
        count("*").alias("claim_count"),
        countDistinct("member_id").alias("unique_members"),
        avg("billed_amount").alias("avg_billed")
    ).orderBy("age_group", "Sex")

age_analysis.show()

# State-wise analysis
state_analysis = claims_df.groupBy("State") \
    .agg(
        count("*").alias("claim_count"),
        countDistinct("member_id").alias("unique_members"),
        sum("billed_amount").alias("total_billed")
    ).orderBy(desc("total_billed"))

state_analysis.show()