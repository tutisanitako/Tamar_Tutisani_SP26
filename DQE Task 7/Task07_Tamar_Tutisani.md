# Task 7 - AWS Lambda & Step Function

## 1. AWS Lambda Service

### a. JSON Parsing Lambda

- **Lambda name:** `tamar_tutisani_pt1a`
- **Link:** [Lambda Link](https://eu-central-1.console.aws.amazon.com/lambda/home?region=eu-central-1#/functions/tamar_tutisani_pt1a?tab=code)
- **IAM Role attached:** `dilab-lambda-sf-role`

**Code:**
```python
def lambda_handler(event, context):
    for key, value in event.items():
        print(f"Value for the key {key} is - {value}")
    return event
```

**Test input:**
```json
{
  "key1": "value1",
  "key2": "value2",
  "key3": "value3"
}
```

**Result:** See `01_lambda_json_parsing_result.png`. Response echoes the input event, Function Logs print each key/value pair in the required format.

### b. Athena Integration (Optional)

- **Lambda name:** `tamar_tutisani_pt1a`
- **Link:** [Lambda Link](https://eu-central-1.console.aws.amazon.com/lambda/home?region=eu-central-1#/functions/tamar_tutisani_pt1a?tab=code)
- Created an Iceberg table `tamar_tutisani_db.tamar_tutisani_iceberg_table` in Athena.
```sql
CREATE TABLE dim_products_iceberg (
    product_id int,
    product_name string,
    category string,
    price string
)
LOCATION 's3://tamar-tutisani-dqe-task7/iceberg-tables/dim_products/'
TBLPROPERTIES ('table_type' = 'ICEBERG');
```
- Function selects rows (`SELECT ... LIMIT 10`), performs an `UPDATE` on one row, then re-selects that row to confirm the change — all logged to Function Logs.

```python
import boto3
import time

athena = boto3.client('athena')
DATABASE = 'tamar_tutisani_db'
TABLE = 'dim_products_iceberg'
S3_OUTPUT = 's3://tamar-tutisani-dqe-task7/athena-query-results/'

def run_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    query_id = response['QueryExecutionId']
    
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
        
    if state != 'SUCCEEDED':
        raise Exception(f"Query failed with state: {state}")
        
    return athena.get_query_results(QueryExecutionId=query_id)

def lambda_handler(event, context):
    # i. Select rows and show them
    select_result = run_query(f"SELECT * FROM {TABLE} LIMIT 10")
    print("Rows before update:")
    for row in select_result['ResultSet']['Rows']:
        print([col.get('VarCharValue', '') for col in row['Data']])
        
    # ii. Update a value
    run_query(f"UPDATE {TABLE} SET price = '999.99' WHERE product_id = 100")
    print("Update executed for product_id = 100")
    
    # iii. Select the changed row to verify
    changed_result = run_query(f"SELECT * FROM {TABLE} WHERE product_id = 100")
    print("Row after update:")
    for row in changed_result['ResultSet']['Rows']:
        print([col.get('VarCharValue', '') for col in row['Data']])
        
    return {"status": "done"}
```
- No VPC.
- See: `02_lambda_athena_function_logs.png`.

### c. S3 File Parsing (Optional)

- **Lambda name:** `tamar-tutisani-dqe-task7`
- **Link:** [Lambda Link](https://eu-central-1.console.aws.amazon.com/s3/buckets/tamar-tutisani-dqe-task7?region=eu-central-1&tab=objects)
- Uploaded `query.sql` to S3 bucket `tamar-tutisani-dqe-task7`. `query.sql` content:
```sql
SELECT * FROM dim_products_iceberg LIMIT 10
```
- Lambda fetches the query text via `s3.get_object`, decodes it, and executes it against Athena instead of using a hardcoded query string.
```python
import boto3
import time

s3 = boto3.client('s3')
athena = boto3.client('athena')

DATABASE = 'tamar_tutisani_db'
S3_OUTPUT = 's3://tamar-tutisani-dqe-task7/athena-query-results/'
QUERY_BUCKET = 'tamar-tutisani-dqe-task7'
QUERY_KEY = 'query.sql'

def get_query_from_s3():
    response = s3.get_object(Bucket=QUERY_BUCKET, Key=QUERY_KEY)
    return response['Body'].read().decode('utf-8').strip()

def run_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    query_id = response['QueryExecutionId']
    
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
        
    if state != 'SUCCEEDED':
        raise Exception(f"Query failed: {state}")
        
    return athena.get_query_results(QueryExecutionId=query_id)

def lambda_handler(event, context):
    query = get_query_from_s3()
    print(f"Query fetched from S3: {query}")
    
    result = run_query(query)
    print("Query result:")
    for row in result['ResultSet']['Rows']:
        print([col.get('VarCharValue', '') for col in row['Data']])
        
    return {"status": "done"}
```
- No VPC attached.
- Result: `03_lambda_s3_query_result.png`.

## 2.  AWS Step Function

- **State machine name:** `tamar_tutisani_dwh_step_function`
- **Link:** [State Machine Link](https://eu-central-1.console.aws.amazon.com/states/home?region=eu-central-1#/statemachines/view/arn%3Aaws%3Astates%3Aeu-central-1%3A260586643565%3AstateMachine%3Atamar_tutisani_dwh_step_function?type=standard)
- **IAM Role attached:** `dilab-lambda-sf-role`
- **Lambdas used inside the Step Function:**
  - `tamar_tutisani_test1_glue_table`: [Lambda Link](https://eu-central-1.console.aws.amazon.com/lambda/home?region=eu-central-1#/functions/tamar_tutisani_test1_glue_table?tab=code)
  - `tamar_tutisani_update_data`: [Lambda Link](https://eu-central-1.console.aws.amazon.com/lambda/home?region=eu-central-1#/functions/tamar_tutisani_update_data?tab=code)
  - `tamar_tutisani_test2_glue_table`: [Lambda Link](https://eu-central-1.console.aws.amazon.com/lambda/home?region=eu-central-1#/functions/tamar_tutisani_test2_glue_table?tab=code)
- **VPC Configuration Note:** While these Step Function Lambdas were initially configured with the same VPC, subnets and security group as the reference function `dq_lambda_example` per task instructions, they continuously experienced network timeouts even after increasing the timeout threshold to 1 minute. Consequently, the VPC attachment was removed to allow direct public/service endpoint routing for Athena communication.

**Flow:**
1. First test (`Test1`) - checks a value in the Glue-cataloged table via Athena and saves output via `ResultPath` (`$.test1_result`).
2. Update (`UpdateData`) - changes that value and preserves output via `ResultPath` (`$.update_result`).
3. Second test (`Test2`) - re-checks the table to confirm the value changed as expected and saves output via `ResultPath` (`$.test2_result`).
4. Choice state (`CheckBothTests`) — evaluates both `$.test1_result.test_passed` and `$.test2_result.test_passed`; if both are `true`, transitions to the Succeed state (`BothTestsPassed`), otherwise transitions to the Fail state (`SomeTestsFailed`).

**References:**
- `04_test1_output.png` - execution output for `tamar_tutisani_test1_glue_table` confirming initial test passed.
- `05_update_output.png` - execution output for `tamar_tutisani_update_data` confirming the value update.
- `06_test2_output.png` - execution output for `tamar_tutisani_test2_glue_table` confirming post-update verification passed.
- `07_stepfunction_visual_design.png` - the state machine visual flow diagram.
- `08_stepfunction_execution_success.png` - successful execution showing the green path through `BothTestsPassed`.
- `09_stepfunction_execution_failure.png` - failed execution branch demonstrating error handling via `SomeTestsFailed`.

## 3. Key Points / Design Notes

- **Event vs. dict parsing:** Lambda's runtime automatically deserializes the incoming JSON event payload into a native Python dictionary before execution, and serializes the return value back into the standard JSON response format.
- **Athena asynchronous connection handling:** Athena queries execute asynchronously. The functions initiate queries using `start_query_execution`, poll status via `get_query_execution` in a loop, and retrieve the tabular results with `get_query_results` once completed.
- **Apache Iceberg requirement:** Standard S3-backed Athena tables are read-only; switching to an Iceberg table format was necessary to enable and support the `UPDATE` operations.
- **Query externalization via S3:** Task 1c decouples business logic from the SQL statement by retrieving the `query.sql` script dynamically from the S3 bucket using `s3.get_object` instead of hardcoding queries.
- **VPC timeout and configuration adjustment:** While the Step Function's Lambdas were initially configured with the exact VPC, subnets, security group, and Layer as the reference function `dq_lambda_example`, they consistently experienced network timeouts even after increasing the function timeout limit to 1 minute. Consequently, the VPC attachment was removed to allow direct public and service endpoint routing for smooth Athena communication.
- **State preservation in Step Functions:** Using `ResultPath` on each Step Function state was crucial to save individual Lambda responses into the workflow's evolving state JSON, ensuring previous outputs weren't overwritten and could be properly evaluated together by the Choice state.