# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM claims LIMIT 10;
# MAGIC

# COMMAND ----------

_sqldf = spark.sql("SELECT * FROM claims LIMIT 10")
df = _sqldf
display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session (if not already initialized in your Databricks notebook)
# In a Databricks notebook, 'spark' is usually pre-defined, so you might not need this line.
spark = SparkSession.builder.appName("ClaimsAnalysis").getOrCreate()

# Read the claims table directly into a DataFrame
df = spark.table("claims")

# PySpark equivalent of the SQL query
result_df = df.groupBy("Provider_Type", "State") \
              .agg(
                  F.count("*").alias("Total_Claims"),
                  F.round(F.sum("Paid_Amount"), 2).alias("Total_Paid_Amount"),
                  F.round(F.avg("Paid_Amount"), 2).alias("Average_Paid_Amount")
              ) \
              .orderBy("Provider_Type", "State")

# Show the results
result_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, when, to_date, year, month, quarter, concat, lit, datediff

df_enhanced = df.withColumn("date_of_service", 
                    when((col("date_of_service") == "NULL") | (col("date_of_service") == ""), None)
                    .otherwise(to_date(col("date_of_service"), "yyyy-MM-dd"))) \
                .withColumn("admission_date", 
                    when((col("admission_date") == "NULL") | (col("admission_date") == "") | col("admission_date").isNull(), None)
                    .otherwise(to_date(col("admission_date"), "M/d/yyyy"))) \
                .withColumn("discharge_date", 
                    when((col("discharge_date") == "NULL") | (col("discharge_date") == "") | col("discharge_date").isNull(), None)
                    .otherwise(to_date(col("discharge_date"), "M/d/yyyy"))) \
                .withColumn("readmission_date", 
                    when((col("readmission_date") == "NULL") | (col("readmission_date") == "") | col("readmission_date").isNull(), None)
                    .otherwise(to_date(col("readmission_date"), "M/d/yyyy"))) \
                .withColumn("paid_date", 
                    when((col("paid_date") == "NULL") | (col("paid_date") == "") | col("paid_date").isNull(), None)
                    .otherwise(to_date(col("paid_date"), "M/d/yyyy"))) \
                .withColumn("claim_received_date", 
                    when((col("claim_received_date") == "NULL") | (col("claim_received_date") == "") | col("claim_received_date").isNull(), None)
                    .otherwise(to_date(col("claim_received_date"), "yyyy-MM-dd"))) \
                .withColumn("payment_rate", 
                    when((col("billed_amount").isNull()) | (col("billed_amount") == 0), None)
                    .otherwise(col("paid_amount") / col("billed_amount"))) \
                .withColumn("denial_amount", 
                    when(col("billed_amount").isNull() | col("paid_amount").isNull(), None)
                    .otherwise(col("billed_amount") - col("paid_amount"))) \
                .withColumn("processing_days", 
                    when(col("paid_date").isNull() | col("claim_received_date").isNull(), None)
                    .otherwise(datediff(col("paid_date"), col("claim_received_date")))) \
                .withColumn("service_year", year(col("date_of_service"))) \
                .withColumn("service_month", month(col("date_of_service"))) \
                .withColumn("service_quarter", 
                    when(col("date_of_service").isNull(), None)
                    .otherwise(concat(year(col("date_of_service")), lit("-Q"), quarter(col("date_of_service"))))) \
                .withColumn("age_group", 
                    when(col("AGE").isNull(), "Unknown")
                    .when(col("AGE") < 18, "Pediatric")
                    .when(col("AGE") < 65, "Adult")
                    .otherwise("Senior")) \
                .withColumn("is_inpatient", when(col("Type_of_Service") == "Inpatient", 1).otherwise(0)) \
                .withColumn("is_readmission", when(col("readmit_indicator") == 1, 1).otherwise(0)) \
                .withColumn("has_denial", 
                    when((col("denial_reason").isNull()) | (col("denial_reason") == "NULL") | (col("denial_reason") == ""), 0)
                    .otherwise(1)) \
                .withColumn("is_paid", when(col("payment_status") == "Paid", 1).otherwise(0))

# COMMAND ----------

df_enhanced = df_enhanced.withColumn("billed_amount", 
                                    when(col("billed_amount").isNull() | (col("billed_amount") < 0), 0)
                                    .otherwise(col("billed_amount"))) \
                        .withColumn("paid_amount", 
                                    when(col("paid_amount").isNull() | (col("paid_amount") < 0), 0)
                                    .otherwise(col("paid_amount"))) \
                        .withColumn("Length_of_Stay", 
                                    when(col("Length_of_Stay").isNull() | (col("Length_of_Stay") < 0), None)
                                    .otherwise(col("Length_of_Stay"))) \
                        .withColumn("denial_reason", 
                                    when((col("denial_reason") == "NULL") | (col("denial_reason") == ""), None)
                                    .otherwise(col("denial_reason"))) \
                        .withColumn("facility_name", 
                                    when((col("facility_name") == "NULL") | (col("facility_name") == ""), None)
                                    .otherwise(col("facility_name")))

# COMMAND ----------

claims = spark.table("workspace.default.pharmacy_claims").limit(5)
display(claims)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM claims LIMIT 5;

# COMMAND ----------

display(_sqldf.limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claims

# COMMAND ----------

df=_sqldf

# COMMAND ----------

from pyspark.sql.functions import col, to_date, when, year, month, quarter, concat, lit, datediff

df_enhanced = df.withColumn("date_of_service", to_date(col("date_of_service"), "yyyy-MM-dd")) \
                .withColumn("admission_date", 
                    when(col("admission_date").isNotNull() & (col("admission_date") != "NULL"), 
                         to_date(col("admission_date"), "M/d/yyyy")).otherwise(None)) \
                .withColumn("discharge_date", 
                    when(col("discharge_date").isNotNull() & (col("discharge_date") != "NULL"), 
                         to_date(col("discharge_date"), "M/d/yyyy")).otherwise(None)) \
                .withColumn("readmission_date", 
                    when(col("readmission_date").isNotNull() & (col("readmission_date") != "NULL"), 
                         to_date(col("readmission_date"), "M/d/yyyy")).otherwise(None)) \
                .withColumn("paid_date", to_date(col("paid_date"), "M/d/yyyy")) \
                .withColumn("claim_received_date", to_date(col("claim_received_date"), "yyyy-MM-dd")) \
                .withColumn("payment_rate", col("paid_amount") / col("billed_amount")) \
                .withColumn("denial_amount", col("billed_amount") - col("paid_amount")) \
                .withColumn("processing_days", datediff(col("paid_date"), col("claim_received_date"))) \
                .withColumn("service_year", year(col("date_of_service"))) \
                .withColumn("service_month", month(col("date_of_service"))) \
                .withColumn("service_quarter", concat(year(col("date_of_service")), lit("-Q"), quarter(col("date_of_service")))) \
                .withColumn("age_group", 
                    when(col("AGE") < 18, "Pediatric")
                    .when(col("AGE") < 65, "Adult")
                    .otherwise("Senior")) \
                .withColumn("is_inpatient", when(col("Type_of_Service") == "Inpatient", 1).otherwise(0)) \
                .withColumn("is_readmission", when(col("readmit_indicator") == 1, 1).otherwise(0)) \
                .withColumn("has_denial", when(col("denial_reason").isNotNull() & (col("denial_reason") != "NULL"), 1).otherwise(0)) \
                .withColumn("is_paid", when(col("payment_status") == "Paid", 1).otherwise(0))

# Handle potential data quality issues
df_enhanced = df_enhanced.withColumn("billed_amount", when(col("billed_amount") < 0, 0).otherwise(col("billed_amount"))) \
                        .withColumn("paid_amount", when(col("paid_amount") < 0, 0).otherwise(col("paid_amount"))) \
                        .withColumn("Length_of_Stay", when(col("Length_of_Stay") < 0, None).otherwise(col("Length_of_Stay")))

# Cache enhanced dataframe for multiple operations
#df_enhanced.cache()

print("=== DATA QUALITY AND OVERVIEW ===")

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, sum, avg, desc, when

provider_analysis = df_enhanced.groupBy("ProviderID", "Provider_Type") \
    .agg(
        count("*").alias("total_claims"),
        countDistinct("member_id").alias("unique_patients"),
        sum("billed_amount").alias("total_billed"),
        sum("paid_amount").alias("total_paid"),
        avg("billed_amount").alias("avg_claim_cost"),
        avg(when(col("processing_days").isNotNull(), col("processing_days"))).alias("avg_processing_days"),
        sum("has_denial").alias("denied_claims"),
        sum("is_readmission").alias("readmissions")
    ) \
    .withColumn("denial_rate", col("denied_claims") / col("total_claims")) \
    .withColumn("readmission_rate", col("readmissions") / col("total_claims")) \
    .withColumn("payment_rate", col("total_paid") / col("total_billed")) \
    .orderBy(desc("total_claims"))

print("Top providers by volume:")
#display(provider_analysis.limit(15))

# COMMAND ----------

member_utilization = df_enhanced.groupBy("member_id") \
    .agg(
        count("*").alias("total_claims"),
        countDistinct("ProviderID").alias("unique_providers"),
        countDistinct("facility_name").alias("unique_facilities"),
        sum("billed_amount").alias("total_cost"),
        sum("paid_amount").alias("total_paid"),
        sum("is_inpatient").alias("inpatient_claims"),
        sum("is_readmission").alias("readmissions"),
        avg("Length_of_Stay").alias("avg_los")
    ) \
    .withColumn("avg_cost_per_claim", col("total_cost") / col("total_claims")) \
    .withColumn("inpatient_rate", col("inpatient_claims") / col("total_claims"))


# COMMAND ----------

service_trends = df_enhanced.groupBy("service_quarter", "Type_of_Service") \
    .agg(
        count("*").alias("claim_count"),
        sum("billed_amount").alias("total_billed"),
        countDistinct("member_id").alias("unique_members")
    ) \
    .orderBy("service_quarter", "Type_of_Service")

print("\nQuarterly service utilization trends:")
service_trends.show(20)

print("\n=== 3. PROVIDER AND FACILITY ANALYTICS ===")

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, sum, avg

# Age group analysis
age_analysis = df_enhanced.groupBy("age_group") \
    .agg(
        count("*").alias("claim_count"),
        countDistinct("member_id").alias("unique_members"),
        sum("billed_amount").alias("total_cost"),
        avg("billed_amount").alias("avg_cost_per_claim"),
        sum("is_inpatient").alias("inpatient_claims"),
        sum("is_readmission").alias("readmissions")
    ) \
    .withColumn("claims_per_member", col("claim_count") / col("unique_members")) \
    .withColumn("cost_per_member", col("total_cost") / col("unique_members")) \
    .withColumn("inpatient_rate", col("inpatient_claims") / col("claim_count"))

print("Healthcare utilization by age group:")
display(age_analysis)

# COMMAND ----------

facility_analysis = df_enhanced.filter(col("facility_name").isNotNull() & (col("facility_name") != "NULL")) \
    .groupBy("facility_name", "State") \
    .agg(
        count("*").alias("total_claims"),
        countDistinct("member_id").alias("unique_patients"),
        sum("billed_amount").alias("total_revenue"),
        avg("Length_of_Stay").alias("avg_los"),
        sum("is_readmission").alias("readmissions"),
        avg("processing_days").alias("avg_processing_time")
    ) \
    .withColumn("readmission_rate", col("readmissions") / col("total_claims")) \
    .orderBy(desc("total_revenue"))

print("\nTop facilities by revenue:")
facility_analysis.limit(10).show()

# COMMAND ----------

network_analysis = df_enhanced.groupBy("Network_Status") \
    .agg(
        count("*").alias("claim_count"),
        sum("billed_amount").alias("total_billed"),
        sum("paid_amount").alias("total_paid"),
        avg("billed_amount").alias("avg_billed"),
        avg("paid_amount").alias("avg_paid"),
        avg("payment_rate").alias("avg_payment_rate")
    )

print("\nNetwork vs Out-of-Network comparison:")
network_analysis.show()

print("\n=== 4. DEMOGRAPHIC ANALYTICS ===")

# COMMAND ----------

age_analysis = df_enhanced.groupBy("age_group") \
    .agg(
        count("*").alias("claim_count"),
        countDistinct("member_id").alias("unique_members"),
        sum("billed_amount").alias("total_cost"),
        avg("billed_amount").alias("avg_cost_per_claim"),
        sum("is_inpatient").alias("inpatient_claims"),
        sum("is_readmission").alias("readmissions")
    ) \
    .withColumn("claims_per_member", col("claim_count") / col("unique_members")) \
    .withColumn("cost_per_member", col("total_cost") / col("unique_members")) \
    .withColumn("inpatient_rate", col("inpatient_claims") / col("claim_count"))

print("Healthcare utilization by age group:")
age_analysis.show()

# COMMAND ----------

gender_analysis = df_enhanced.groupBy("Sex") \
    .agg(
        count("*").alias("claim_count"),
        countDistinct("member_id").alias("unique_members"),
        sum("billed_amount").alias("total_cost"),
        avg("billed_amount").alias("avg_cost_per_claim"),
        avg("AGE").alias("avg_age")
    ) \
    .withColumn("cost_per_member", col("total_cost") / col("unique_members"))

print("\nHealthcare utilization by gender:")
gender_analysis.show()

# COMMAND ----------

race_analysis = df_enhanced.groupBy("race") \
    .agg(
        count("*").alias("claim_count"),
        countDistinct("member_id").alias("unique_members"),
        sum("billed_amount").alias("total_cost"),
        avg("billed_amount").alias("avg_cost_per_claim"),
        sum("has_denial").alias("denials")
    ) \
    .withColumn("denial_rate", col("denials") / col("claim_count")) \
    .orderBy(desc("claim_count"))

print("\nHealthcare utilization by race/ethnicity:")
race_analysis.show()

# COMMAND ----------

geographic_analysis = df_enhanced.groupBy("State") \
    .agg(
        count("*").alias("claim_count"),
        countDistinct("member_id").alias("unique_members"),
        sum("billed_amount").alias("total_cost"),
        avg("billed_amount").alias("avg_cost_per_claim"),
        sum("is_inpatient").alias("inpatient_claims")
    ) \
    .withColumn("cost_per_member", col("total_cost") / col("unique_members")) \
    .withColumn("inpatient_rate", col("inpatient_claims") / col("claim_count")) \
    .orderBy(desc("total_cost"))

print("\nHealthcare utilization by state:")
geographic_analysis.show()

print("\n=== 5. READMISSION ANALYTICS ===")

# COMMAND ----------

readmission_analysis = df_enhanced.filter(col("is_inpatient") == 1) \
    .groupBy("Provider_Type", "age_group") \
    .agg(
        count("*").alias("inpatient_claims"),
        sum("is_readmission").alias("readmissions"),
        avg("Length_of_Stay").alias("avg_los"),
        sum("billed_amount").alias("total_cost")
    ) \
    .withColumn("readmission_rate", col("readmissions") / col("inpatient_claims")) \
    .orderBy(desc("readmission_rate"))

print("Readmission rates by provider type and age group:")
readmission_analysis.show()

# COMMAND ----------

denial_analysis = df_enhanced.filter(col("has_denial") == 1) \
    .groupBy("denial_reason") \
    .agg(
        count("*").alias("denial_count"),
        sum("billed_amount").alias("denied_amount"),
        avg("billed_amount").alias("avg_denied_amount")
    ) \
    .orderBy(desc("denial_count"))

print("Denial reasons analysis:")
denial_analysis.show()

# COMMAND ----------

variance_analysis = df_enhanced.filter(col("Variance").isNotNull()) \
    .groupBy("Type_of_Service") \
    .agg(
        count("*").alias("claim_count"),
        avg("Variance").alias("avg_variance"),
        sum(when(col("Variance") > 0, 1).otherwise(0)).alias("positive_variance_count"),
        sum(when(col("Variance") < 0, 1).otherwise(0)).alias("negative_variance_count")
    ) \
    .withColumn("positive_variance_rate", col("positive_variance_count") / col("claim_count"))

print("\nPayment variance analysis:")
variance_analysis.show()

print("\n=== 8. CPT AND REVENUE CODE ANALYTICS ===")

# COMMAND ----------

cpt_analysis = df_enhanced.filter(col("cpt_code").isNotNull()) \
    .groupBy("cpt_code") \
    .agg(
        count("*").alias("procedure_count"),
        sum("billed_amount").alias("total_billed"),
        avg("billed_amount").alias("avg_cost"),
        countDistinct("ProviderID").alias("provider_count"),
        sum("has_denial").alias("denials")
    ) \
    .withColumn("denial_rate", col("denials") / col("procedure_count")) \
    .orderBy(desc("procedure_count"))

print("Top CPT codes by frequency:")
cpt_analysis.limit(15).show()

# COMMAND ----------

total_records = df_enhanced.count()
print(f"Total records: {total_records:,}")


# COMMAND ----------

completeness_by_service = df_enhanced.groupBy("Type_of_Service") \
    .agg(
        count("*").alias("total_claims"),
        count(when(col("admission_date").isNotNull(), 1)).alias("admission_date_complete"),
        count(when(col("Length_of_Stay").isNotNull(), 1)).alias("los_complete"),
        count(when(col("cpt_code").isNotNull(), 1)).alias("cpt_complete")
    ) \
    .withColumn("admission_completeness", 
                when(col("total_claims") > 0, col("admission_date_complete") / col("total_claims"))
                .otherwise(0)) \
    .withColumn("los_completeness", 
                when(col("total_claims") > 0, col("los_complete") / col("total_claims"))
                .otherwise(0)) \
    .withColumn("cpt_completeness", 
                when(col("total_claims") > 0, col("cpt_complete") / col("total_claims"))
                .otherwise(0))

print("\nData completeness by service type:")
#completeness_by_service.show()

print("\n=== 1. FINANCIAL ANALYTICS ===")


# COMMAND ----------

from pyspark.sql.functions import udf, col, count, sum, avg
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def age_group_udf(age):
    """
    Age group UDF for use in Spark DataFrames
    """
    if age is None:
        return "Unknown"
    elif age < 0 or age > 120:
        return "Invalid"
    elif age < 18:
        return "Pediatric (0-17)"
    elif 18 <= age <= 34:
        return "Young Adult (18-34)"
    elif 35 <= age <= 54:
        return "Adult (35-54)"
    elif 55 <= age <= 64:
        return "Mature Adult (55-64)"
    elif age >= 65:
        return "Senior (65+)"
    else:
        return "Unknown"

# Usage with Spark DataFrame
claims_df = spark.table("default.claims")

# Apply the UDF
claims_with_age_group = claims_df.withColumn("age_group", age_group_udf(col("AGE")))

# Show results
display(claims_with_age_group.select("member_id", "AGE", "age_group"))

# Analyze by age group
claims_with_age_group.groupBy("age_group") \
    .agg(
        count("*").alias("claim_count"),
        sum("billed_amount").alias("total_billed"),
        avg("billed_amount").alias("avg_billed")
    ).orderBy("age_group").show()

# COMMAND ----------

def age_group(age):
    # Your function logic here
    pass

# Register so you can use it in SQL
spark.udf.register("age_group", age_group, StringType())

# Now use in SQL
spark.sql("""
    SELECT age_group(AGE) as age_group, COUNT(*) as count
    FROM default.claims
    GROUP BY age_group(AGE)
""").show()

# COMMAND ----------

from pyspark.sql.functions import udf, col, count, sum, avg
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def age_group_udf(age):
    """
    Age group UDF for use in Spark DataFrames
    """
    if age is None:
        return "Unknown"
    elif age < 0 or age > 120:
        return "Invalid"
    elif age < 18:
        return "Pediatric (0-17)"
    elif 18 <= age <= 34:
        return "Young Adult (18-34)"
    elif 35 <= age <= 54:
        return "Adult (35-54)"
    elif 55 <= age <= 64:
        return "Mature Adult (55-64)"
    elif age >= 65:
        return "Senior (65+)"
    else:
        return "Unknown"

# Usage with Spark DataFrame
claims_df = spark.table("default.claims")

# Apply the UDF
claims_with_age_group = claims_df.withColumn("age_group", age_group_udf(col("AGE")))

# Show results
claims_with_age_group.select("member_id", "AGE", "age_group").show(10)

# Analyze by age group
claims_with_age_group.groupBy("age_group") \
    .agg(
        count("*").alias("claim_count"),
        sum("billed_amount").alias("total_billed"),
        avg("billed_amount").alias("avg_billed")
    ).orderBy("age_group").show()