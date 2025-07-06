# Databricks notebook source
claims_df = spark.table("default.claims")


# COMMAND ----------

claims_df.show(5)

# COMMAND ----------

def analyze_claims_by_date_range(start_date, end_date, service_type=None, min_amount=None):
    """
    Analyze claims within a date range with optional filters
    
    Parameters:
    - start_date: str, format 'YYYY-MM-DD' or 'M/d/yyyy'
    - end_date: str, format 'YYYY-MM-DD' or 'M/d/yyyy'  
    - service_type: str, optional filter for Type_of_Service
    - min_amount: float, optional minimum billed amount filter
    """
    from pyspark.sql.functions import to_date, col, lit, count, sum, avg
    
    # Base query
    claims_df = spark.table("default.claims")
    
    # Convert date strings to date objects for comparison
    filtered_df = claims_df.filter(
        (to_date(col("date_of_service"), "M/d/yyyy") >= lit(start_date)) &
        (to_date(col("date_of_service"), "M/d/yyyy") <= lit(end_date))
    )
    
    # Apply optional filters
    if service_type:
        filtered_df = filtered_df.filter(col("Type_of_Service") == service_type)
    
    if min_amount:
        filtered_df = filtered_df.filter(col("billed_amount") >= min_amount)
    
    # Perform analysis
    result = filtered_df.groupBy("Type_of_Service", "payment_status") \
        .agg(
            count("*").alias("claim_count"),
            sum("billed_amount").alias("total_billed"),
            sum("paid_amount").alias("total_paid"),
            avg("billed_amount").alias("avg_billed")
        ).orderBy("Type_of_Service", "payment_status")
    
    return result

# COMMAND ----------

jan_2024_claims = analyze_claims_by_date_range("2024-01-01", "2024-01-31")
jan_2024_claims.show()

inpatient_high_cost = analyze_claims_by_date_range(
    "2024-01-01", "2024-12-31", 
    service_type="Inpatient",
    min_amount=10000
)
inpatient_high_cost.show()

# COMMAND ----------

config = {
    "date_range": {
        "start_date": "2024-01-01",
        "end_date": "2024-12-31"
    },
    "filters": {
        "states": ["CA", "NY", "TX"],
        "age_min": 18,
        "age_max": 65,
        "service_types": ["Inpatient", "Outpatient"],
        "network_status": ["In Network"]
    },
    "aggregation": {
        "group_by": ["State", "Type_of_Service"],
        "metrics": ["count", "sum", "avg"]
    }
}

# COMMAND ----------

from pyspark.sql.functions import *

def dynamic_claims_analysis(config):
    """
    Perform dynamic claims analysis based on configuration
    """
    claims_df = spark.table("default.claims")
    
    # Apply date filters
    if "date_range" in config:
        claims_df = claims_df.filter(
            (to_date(col("date_of_service"), "M/d/yyyy") >= config["date_range"]["start_date"]) &
            (to_date(col("date_of_service"), "M/d/yyyy") <= config["date_range"]["end_date"])
        )

         # Apply demographic filters
    filters = config.get("filters", {})
    
    if "states" in filters:
        claims_df = claims_df.filter(col("State").isin(filters["states"]))
    
    if "age_min" in filters:
        claims_df = claims_df.filter(col("AGE") >= filters["age_min"])
    
    if "age_max" in filters:
        claims_df = claims_df.filter(col("AGE") <= filters["age_max"])
    
    if "service_types" in filters:
        claims_df = claims_df.filter(col("Type_of_Service").isin(filters["service_types"]))
    
    if "network_status" in filters:
        claims_df = claims_df.filter(col("Network_Status").isin(filters["network_status"]))
    
    # Dynamic grouping and aggregation
    agg_config = config.get("aggregation", {})
    group_cols = agg_config.get("group_by", ["Type_of_Service"])
    
    # Build aggregation expressions
    agg_exprs = []
    if "count" in agg_config.get("metrics", []):
        agg_exprs.append(count("*").alias("record_count"))
    if "sum" in agg_config.get("metrics", []):
        agg_exprs.extend([
            sum("billed_amount").alias("total_billed"),
            sum("paid_amount").alias("total_paid")
        ])
    if "avg" in agg_config.get("metrics", []):
        agg_exprs.extend([
            avg("billed_amount").alias("avg_billed"),
            avg("paid_amount").alias("avg_paid")
        ])
    
    result = claims_df.groupBy(*group_cols).agg(*agg_exprs)
    return result

# Usage
result = dynamic_claims_analysis(config)
result.show()

# COMMAND ----------

def create_parameterized_sql(start_date, end_date, states=None, min_amount=None):
    """
    Create parameterized SQL query using string formatting
    """
    base_query = """
    SELECT 
        State,
        Type_of_Service,
        COUNT(*) as claim_count,
        SUM(billed_amount) as total_billed,
        AVG(billed_amount) as avg_billed,
        COUNT(DISTINCT member_id) as unique_members
    FROM default.claims
    WHERE date_of_service >= '{start_date}'
      AND date_of_service <= '{end_date}'
    """
    
    # Add optional filters
    if states:
        state_list = "', '".join(states)
        base_query += f" AND State IN ('{state_list}')"
    
    if min_amount:
        base_query += f" AND billed_amount >= {min_amount}"
    
    base_query += """
    GROUP BY State, Type_of_Service
    ORDER BY total_billed DESC
    """
    
    return base_query.format(start_date=start_date, end_date=end_date)


# COMMAND ----------

query = create_parameterized_sql(
    start_date="2024-01-01",
    end_date="2024-03-31",
    states=["CA", "NY", "TX"],
    min_amount=1000
)

result = spark.sql(query)
result.show()

# COMMAND ----------

# Create interactive widgets in Databricks notebook
dbutils.widgets.dropdown("service_type", "All", ["All", "Inpatient", "Outpatient", "Emergency"])
dbutils.widgets.text("start_date", "2024-01-01")
dbutils.widgets.text("end_date", "2024-12-31")
dbutils.widgets.multiselect("states", "CA", ["CA", "NY", "TX", "FL", "IL"])
dbutils.widgets.text("min_amount", "0")

# Get widget values
service_type = dbutils.widgets.get("service_type")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
states = dbutils.widgets.get("states").split(",")
min_amount = float(dbutils.widgets.get("min_amount"))

# Build dynamic query
def build_widget_query():
    where_conditions = [
        f"date_of_service >= '{start_date}'",
        f"date_of_service <= '{end_date}'",
        f"billed_amount >= {min_amount}"
    ]
    
    if service_type != "All":
        where_conditions.append(f"Type_of_Service = '{service_type}'")
    
    if states and states != ['']:
        state_list = "', '".join(states)
        where_conditions.append(f"State IN ('{state_list}')")
    
    where_clause = " AND ".join(where_conditions)
    
    query = f"""
    SELECT 
        State,
        Type_of_Service,
        payment_status,
        COUNT(*) as claim_count,
        SUM(billed_amount) as total_billed,
        SUM(paid_amount) as total_paid,
        ROUND(AVG(billed_amount), 2) as avg_billed,
        COUNT(DISTINCT member_id) as unique_members
    FROM default.claims
    WHERE {where_clause}
    GROUP BY State, Type_of_Service, payment_status
    ORDER BY total_billed DESC
    """
    
    return query

# Execute parameterized query
result = spark.sql(build_widget_query())
result.display()

# COMMAND ----------

# Set SQL variables
start_date = '2024-01-01'
end_date = '2024-12-31'
min_amount = 5000

# Create parameterized SQL using variables
parameterized_query = f"""
SELECT 
    Type_of_Service,
    Provider_Type,
    COUNT(*) as claim_count,
    SUM(billed_amount) as total_billed,
    AVG(billed_amount) as avg_billed,
    COUNT(DISTINCT member_id) as unique_members
FROM default.claims
WHERE date_of_service >= '{start_date}'
  AND date_of_service <= '{end_date}'
  AND billed_amount >= {min_amount}
GROUP BY Type_of_Service, Provider_Type
ORDER BY total_billed DESC
"""

result = spark.sql(parameterized_query)
display(result)

# Alternative: Create temporary view with parameters
def create_filtered_view(view_name, **kwargs):
    """
    Create a temporary view with applied filters
    """
    base_sql = "SELECT * FROM default.claims WHERE 1=1"
    
    if 'start_date' in kwargs:
        base_sql += f" AND date_of_service >= '{kwargs['start_date']}'"
    
    if 'end_date' in kwargs:
        base_sql += f" AND date_of_service <= '{kwargs['end_date']}'"
    
    if 'states' in kwargs:
        state_list = "', '".join(kwargs['states'])
        base_sql += f" AND State IN ('{state_list}')"
    
    if 'min_amount' in kwargs:
        base_sql += f" AND billed_amount >= {kwargs['min_amount']}"
    
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {view_name} AS {base_sql}")

# Usage
create_filtered_view(
    "filtered_claims",
    start_date="2024-01-01",
    end_date="2024-12-31",
    states=["CA", "NY"],
    min_amount=1000
)

# Now use the filtered view in any SQL query
spark.sql("""
SELECT 
    State,
    COUNT(*) as claim_count,
    SUM(billed_amount) as total_billed
FROM filtered_claims
GROUP BY State
ORDER BY total_billed DESC
""").show()

# COMMAND ----------

from string import Template

# Define query templates
CLAIMS_ANALYSIS_TEMPLATE = Template("""
SELECT 
    $group_columns,
    COUNT(*) as claim_count,
    SUM(billed_amount) as total_billed,
    SUM(paid_amount) as total_paid,
    ROUND(AVG(billed_amount), 2) as avg_billed,
    COUNT(DISTINCT member_id) as unique_members
FROM default.claims
WHERE $where_conditions
GROUP BY $group_columns
ORDER BY $order_by
""")

def generate_claims_query(group_by, filters, order_by="total_billed DESC"):
    """
    Generate SQL query from template
    """
    # Build WHERE conditions
    where_conditions = []
    for field, value in filters.items():
        if isinstance(value, list):
            value_str = "', '".join(str(v) for v in value)
            where_conditions.append(f"{field} IN ('{value_str}')")
        elif isinstance(value, dict) and 'min' in value:
            where_conditions.append(f"{field} >= {value['min']}")
        elif isinstance(value, dict) and 'max' in value:
            where_conditions.append(f"{field} <= {value['max']}")
        else:
            where_conditions.append(f"{field} = '{value}'")
    
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    group_columns = ", ".join(group_by)
    
    return CLAIMS_ANALYSIS_TEMPLATE.substitute(
        group_columns=group_columns,
        where_conditions=where_clause,
        order_by=order_by
    )

# Usage
query = generate_claims_query(
    group_by=["State", "Type_of_Service"],
    filters={
        "State": ["CA", "NY", "TX"],
        "billed_amount": {"min": 1000},
        "payment_status": "Paid"
    }
)

print(query)
result = spark.sql(query)
result.show()

# COMMAND ----------

import json

# Define configuration in JSON
config_json = """
{
    "queries": {
        "monthly_summary": {
            "group_by": ["YEAR(date_of_service)", "MONTH(date_of_service)", "Type_of_Service"],
            "metrics": ["COUNT(*)", "SUM(billed_amount)", "AVG(billed_amount)"],
            "filters": {
                "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
                "states": ["CA", "NY", "TX"]
            },
            "order_by": "YEAR(date_of_service), MONTH(date_of_service)"
        },
        "provider_performance": {
            "joins": ["providerlist ON claims.ProviderID = providerlist.provider_id"],
            "group_by": ["providerlist.name", "providerlist.specialty"],
            "metrics": ["COUNT(*)", "SUM(billed_amount)", "COUNT(DISTINCT member_id)"],
            "order_by": "SUM(billed_amount) DESC"
        }
    }
}
"""

def execute_config_query(config, query_name):
    """
    Execute query based on configuration
    """
    query_config = json.loads(config)["queries"][query_name]
    
    # Build base query
    select_clause = ", ".join(query_config["group_by"] + query_config["metrics"])
    from_clause = "default.claims"
    
    # Add joins if specified
    if "joins" in query_config:
        for join in query_config["joins"]:
            from_clause += f" LEFT JOIN default.{join}"
    
    # Build WHERE clause
    where_conditions = []
    if "filters" in query_config:
        filters = query_config["filters"]
        if "date_range" in filters:
            where_conditions.append(
                f"date_of_service >= '{filters['date_range']['start']}' AND "
                f"date_of_service <= '{filters['date_range']['end']}'"
            )
        if "states" in filters:
            state_list = "', '".join(filters["states"])
            where_conditions.append(f"State IN ('{state_list}')")
    
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    group_clause = ", ".join(query_config["group_by"])
    order_clause = query_config.get("order_by", "")
    
    query = f"""
    SELECT {select_clause}
    FROM {from_clause}
    WHERE {where_clause}
    GROUP BY {group_clause}
    ORDER BY {order_clause}
    """
    
    return spark.sql(query)

# Usage
result = execute_config_query(config_json, "monthly_summary")
result.show()

# COMMAND ----------

def create_parameterized_sql(start_date, end_date, states=None, min_amount=None, 
                           service_types=None, payment_statuses=None):
    """
    Create parameterized SQL query with proper escaping
    """
    base_query = """
    SELECT 
        State,
        Type_of_Service,
        payment_status,
        COUNT(*) as claim_count,
        SUM(billed_amount) as total_billed,
        SUM(paid_amount) as total_paid,
        ROUND(AVG(billed_amount), 2) as avg_billed,
        COUNT(DISTINCT member_id) as unique_members
    FROM default.claims
    WHERE date_of_service >= '{start_date}'
      AND date_of_service <= '{end_date}'
    """
    
    # Add optional filters with proper escaping
    if states:
        escaped_states = [state.replace("'", "''") for state in states]  # Escape single quotes
        state_list = "', '".join(escaped_states)
        base_query += f" AND State IN ('{state_list}')"
    
    if min_amount is not None:
        base_query += f" AND billed_amount >= {min_amount}"
    
    if service_types:
        escaped_services = [svc.replace("'", "''") for svc in service_types]
        service_list = "', '".join(escaped_services)
        base_query += f" AND Type_of_Service IN ('{service_list}')"
    
    if payment_statuses:
        escaped_statuses = [status.replace("'", "''") for status in payment_statuses]
        status_list = "', '".join(escaped_statuses)
        base_query += f" AND payment_status IN ('{status_list}')"
    
    base_query += """
    GROUP BY State, Type_of_Service, payment_status
    ORDER BY total_billed DESC
    """
    
    return base_query.format(start_date=start_date, end_date=end_date)

def validate_and_execute_query(start_date, end_date, **kwargs):
    """
    Validate parameters and execute query with comprehensive error handling
    """
    from datetime import datetime
    import re
    
    # Input validation
    def validate_date_format(date_str, param_name):
        """Validate date format"""
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            raise ValueError(f"{param_name} must be in YYYY-MM-DD format, got: {date_str}")
    
    def validate_states(states):
        """Validate state codes"""
        if not isinstance(states, list):
            raise ValueError("States must be provided as a list")
        
        valid_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
        }
        
        for state in states:
            if not isinstance(state, str):
                raise ValueError(f"State codes must be strings, got: {type(state)}")
            if state.upper() not in valid_states:
                raise ValueError(f"Invalid state code: {state}")
        
        return [state.upper() for state in states]  # Normalize to uppercase
    
    def validate_numeric_param(value, param_name, min_val=0):
        """Validate numeric parameters"""
        try:
            num_val = float(value)
            if num_val < min_val:
                raise ValueError(f"{param_name} must be >= {min_val}, got: {num_val}")
            return num_val
        except (ValueError, TypeError):
            raise ValueError(f"{param_name} must be a valid number, got: {value}")
    
    def validate_string_list(values, param_name, valid_options=None):
        """Validate list of string parameters"""
        if not isinstance(values, list):
            raise ValueError(f"{param_name} must be provided as a list")
        
        for value in values:
            if not isinstance(value, str):
                raise ValueError(f"All {param_name} must be strings, got: {type(value)}")
            if valid_options and value not in valid_options:
                raise ValueError(f"Invalid {param_name}: {value}. Valid options: {valid_options}")
        
        return values
    
    # Perform validations
    try:
        # Validate required parameters
        validate_date_format(start_date, "start_date")
        validate_date_format(end_date, "end_date")
        
        # Validate date range
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        if start_dt > end_dt:
            raise ValueError("start_date must be before or equal to end_date")
        
        # Validate optional parameters
        validated_kwargs = {}
        
        if 'states' in kwargs and kwargs['states']:
            validated_kwargs['states'] = validate_states(kwargs['states'])
        
        if 'min_amount' in kwargs and kwargs['min_amount'] is not None:
            validated_kwargs['min_amount'] = validate_numeric_param(
                kwargs['min_amount'], 'min_amount', min_val=0
            )
        
        if 'service_types' in kwargs and kwargs['service_types']:
            valid_service_types = ['Inpatient', 'Outpatient', 'Emergency', 'Urgent Care']
            validated_kwargs['service_types'] = validate_string_list(
                kwargs['service_types'], 'service_types', valid_service_types
            )
        
        if 'payment_statuses' in kwargs and kwargs['payment_statuses']:
            valid_payment_statuses = ['Paid', 'Pending', 'Denied', 'Partial']
            validated_kwargs['payment_statuses'] = validate_string_list(
                kwargs['payment_statuses'], 'payment_statuses', valid_payment_statuses
            )
        
    except ValueError as e:
        print(f"Validation error: {e}")
        return None
    
    # Execute query with error handling
    try:
        query = create_parameterized_sql(start_date, end_date, **validated_kwargs)
        print("Generated SQL Query:")
        print("-" * 50)
        print(query)
        print("-" * 50)
        
        result = spark.sql(query)
        print(f"Query executed successfully. Result contains {result.count()} rows.")
        return result
        
    except Exception as e:
        print(f"Query execution failed: {str(e)}")
        print("Generated query was:")
        print(query if 'query' in locals() else "Query generation failed")
        return None

# Advanced validation with custom error messages
class QueryValidationError(Exception):
    """Custom exception for query validation errors"""
    pass

def robust_validate_and_execute(start_date, end_date, **kwargs):
    """
    More robust validation with detailed error reporting
    """
    errors = []
    warnings = []
    
    # Collect all validation errors instead of failing on first error
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
    except ValueError:
        errors.append(f"Invalid start_date format: {start_date}. Expected YYYY-MM-DD")
    
    try:
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        errors.append(f"Invalid end_date format: {end_date}. Expected YYYY-MM-DD")
    
    # Check date range logic
    try:
        if datetime.strptime(start_date, '%Y-%m-%d') > datetime.strptime(end_date, '%Y-%m-%d'):
            errors.append("start_date must be before or equal to end_date")
    except ValueError:
        pass  # Already caught above
    
    # Validate optional parameters
    if 'states' in kwargs:
        if not isinstance(kwargs['states'], list):
            errors.append("states parameter must be a list")
        elif len(kwargs['states']) == 0:
            warnings.append("Empty states list provided")
    
    if 'min_amount' in kwargs:
        try:
            amount = float(kwargs['min_amount'])
            if amount < 0:
                errors.append("min_amount cannot be negative")
            elif amount > 1000000:
                warnings.append(f"min_amount is very high: ${amount:,.2f}")
        except (ValueError, TypeError):
            errors.append(f"min_amount must be a number, got: {kwargs['min_amount']}")
    
    # Report all issues
    if errors:
        error_msg = "Validation failed with the following errors:\n" + "\n".join(f"- {err}" for err in errors)
        if warnings:
            error_msg += "\n\nWarnings:\n" + "\n".join(f"- {warn}" for warn in warnings)
        raise QueryValidationError(error_msg)
    
    if warnings:
        print("Validation warnings:")
        for warning in warnings:
            print(f"- {warning}")
    
    # Execute if validation passes
    try:
        return validate_and_execute_query(start_date, end_date, **kwargs)
    except Exception as e:
        raise QueryValidationError(f"Query execution failed: {str(e)}")

# Usage examples with proper error handling
def demo_validation_examples():
    """
    Demonstrate various validation scenarios
    """
    print("=== VALIDATION EXAMPLES ===\n")
    
    # Example 1: Valid query
    print("1. Valid query example:")
    try:
        result = validate_and_execute_query(
            "2024-01-01", 
            "2024-12-31",
            states=["CA", "NY"],
            min_amount=1000,
            service_types=["Inpatient", "Outpatient"]
        )
        if result:
            print("✓ Query executed successfully")
            # result.show(5)  # Uncomment to show results
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
    
    print("\n" + "="*50 + "\n")
    
    # Example 2: Invalid date format
    print("2. Invalid date format example:")
    try:
        result = validate_and_execute_query(
            "01/01/2024",  # Wrong format
            "2024-12-31",
            states=["CA"]
        )
    except Exception as e:
        print(f"✓ Correctly caught error: {e}")
    
    print("\n" + "="*50 + "\n")
    
    # Example 3: Invalid state codes
    print("3. Invalid state codes example:")
    try:
        result = validate_and_execute_query(
            "2024-01-01",
            "2024-12-31",
            states=["CALIFORNIA", "XX"]  # Invalid state codes
        )
    except Exception as e:
        print(f"✓ Correctly caught error: {e}")
    
    print("\n" + "="*50 + "\n")
    
    # Example 4: Robust validation with multiple errors
    print("4. Multiple validation errors example:")
    try:
        result = robust_validate_and_execute(
            "2024-13-01",  # Invalid month
            "2024-01-01",  # End before start
            states="CA",   # Not a list
            min_amount=-100,  # Negative amount
            service_types=["InvalidService"]  # Invalid service type
        )
    except QueryValidationError as e:
        print(f"✓ Correctly caught multiple errors:\n{e}")

# Run the demo (uncomment to test)
# demo_validation_examples()