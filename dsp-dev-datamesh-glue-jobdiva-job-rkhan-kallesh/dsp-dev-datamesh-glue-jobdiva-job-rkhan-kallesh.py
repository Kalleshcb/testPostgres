import csv
import io
import json
import datetime
import math
import numpy as np

import pytz
from pytz import timezone
import time
import sys
import traceback

import boto3
import psycopg2
import requests
import pandas as pd

# import ListEmrClusters as lec

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

def get_secret(p_env):
    secret_name = f"arn:aws:secretsmanager:us-east-1:596212449348:secret:dsp-{p_env}-datamesh-secrets-1-7kMDZ7"

    if p_env == "prod":
        secret_name = "dsp-prod-datamesh-secrets"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)
    return secret


def get_warehouse_connection(p_secrets):
    warehouse_dbname = p_secrets["warehouse_dbname"]
    warehouse_host = p_secrets["warehouse_host"]
    warehouse_port = p_secrets["warehouse_port"]
    warehouse_user = p_secrets["warehouse_user"]
    warehouse_pw = p_secrets["warehouse_pw"]

    rs_conn = psycopg2.connect(dbname=warehouse_dbname,
                               host=warehouse_host,
                               port=warehouse_port,
                               user=warehouse_user, password=warehouse_pw)
    return rs_conn


def get_token_from_credentials(p_env):
    r_secrets = get_secret(p_env)
    l_token_url = r_secrets["JOB_DIVA_TOKEN_URL"]
    l_token_response = requests.get(l_token_url, verify=True)
    l_token = l_token_response.text

    return l_token


def upload_to_s3(p_env, p_column_names, p_data_rows, p_table_name):
    tz = timezone('America/New_York')
    now = str(datetime.datetime.now(tz)).replace(' ', '/')
    now = now.replace(':', '.')
    l_bucket_name = f"dsp-{p_env}-datamesh-s3"
    s3_key = f"RAW/JobDiva/{p_table_name}/{now}/{p_table_name}.csv"
    if p_table_name == 'Activities':
        my_list = ['Security_Clearance_Required']
        if not all(x in p_column_names for x in my_list):
            p_column_names = p_column_names + my_list
            
    with io.StringIO() as csvf:
        writer = csv.writer(csvf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(p_column_names)
        for a_dict_row in p_data_rows:
            writer.writerow(a_dict_row)

        buff = io.BytesIO(csvf.getvalue().encode())
        client = boto3.client('s3', verify=True)
        client.upload_fileobj(buff, l_bucket_name, s3_key)

    return l_bucket_name, s3_key, now


def get_user_defined_fields(p_job_diva_udf_list_name, p_token):
    if p_job_diva_udf_list_name is None or p_job_diva_udf_list_name == "":
        return []

    l_udf_url = "https://api.jobdiva.com/api/bi/UserfieldsList"

    l_response = requests.get(l_udf_url, headers={'Authorization': 'Bearer %s' % p_token}, verify=True)
    user_fields_list = json.loads(l_response.text)["data"]

    # print(user_fields_list)
    field_name_index = user_fields_list[0].index("FIELDNAME")
    field_type_index = user_fields_list[0].index("FIELDTYPE")
    field_size_index = user_fields_list[0].index("FIELDSIZE")
    field_for_index = user_fields_list[0].index("FIELDFOR")

    l_udf_column_names_with_no_spaces = []
    l_udf_column_names_with_spaces = []
    l_udf_field_types = []
    l_udf_field_sizes = []
    for user_field in user_fields_list[1:]:
        l_field_name = user_field[field_name_index]
        if not (l_field_name=='(UDF) Program Specialist' or l_field_name=='(UDF) Enrollment Manager' or l_field_name=='https://pricing-dev.mbopartners.com/'):
            l_udf_column_name_with_spaces = user_field[field_name_index]
            l_field_name = l_field_name.replace(" ", "_")
            l_field_type = user_field[field_type_index]
            l_field_type = "VARCHAR"
            l_field_size = user_field[field_size_index]
            if int(l_field_size) < 100:
                l_field_size = '256'
            l_field_for = user_field[field_for_index]
            # print(l_field_for)
            # print(f"{l_field_name} : {l_field_type} {l_field_size} for {l_field_for}")
            if p_job_diva_udf_list_name == l_field_for:
                l_udf_column_names_with_no_spaces.append("userFieldsName=" + l_field_name)
                l_udf_column_names_with_spaces.append("userFieldsName=" + l_udf_column_name_with_spaces)
                l_udf_field_types.append(l_field_type)
                l_udf_field_sizes.append(l_field_size)
                # print(f"{l_field_name} : {l_field_type} {l_field_size} for  {l_field_for}")
                # print(user_field)

    return l_udf_column_names_with_spaces, l_udf_column_names_with_no_spaces, l_udf_field_types, l_udf_field_sizes


def source_a_job_diva_reference_data(p_job_diva_end_point, p_job_diva_credential_token):
    # print(f"Working on {p_job_diva_end_point}")

    l_url = f"https://api.jobdiva.com/api/bi/{p_job_diva_end_point}"
    print(l_url)
    response = requests.get(l_url, headers={'Authorization': 'Bearer %s' % p_job_diva_credential_token}, verify=True)

    l_column_names = []
    l_data_rows = []

    l_candidates_list = json.loads(response.text)["data"]
    l_candidates_list = json.loads(response.text)["data"]
    l_candidates_list = json.loads(response.text)["data"]
    l_candidates_list = json.loads(response.text)["data"]

    if len(l_candidates_list) <= 0:
        return l_column_names, l_data_rows

    l_column_names = l_candidates_list[0]
    l_column_names = remove_spaces_from_column_names(l_column_names)
    # print(l_column_names)
    l_data_rows = []
    for l_candidate in l_candidates_list[1:]:
        l_data_row = l_candidate
        # print(l_data_row)
        l_data_rows.append(l_data_row)

    return l_column_names, l_data_rows


def remove_spaces_from_column_names(p_column_names):
    if p_column_names is None:
        return p_column_names

    if len(p_column_names) <= 0:
        return p_column_names

    l_tmp_column_names = []
    for l_column_name in p_column_names:
        l_column_name = l_column_name.replace(" ", "_")
        l_tmp_column_names.append(l_column_name)

    return l_tmp_column_names


def source_job_diva_records_for_date_range(p_job_diva_config, p_job_diva_credential_token
                                           , p_from_date, p_to_date, p_user_name_fields):
    p_job_diva_end_point_base_url = p_job_diva_config['job_diva_end_point_base_url']
    p_job_diva_end_point = p_job_diva_config["job_diva_end_point"]
    l_from_date_string = p_from_date.strftime("%m/%d/%Y %H:%M:%S")
    l_to_date_string = p_to_date.strftime("%m/%d/%Y %H:%M:%S")

    # print(f"Working on {p_job_diva_end_point} for {l_from_date_string} to {l_to_date_string}")

    l_date_range = f"fromDate={l_from_date_string}&toDate={l_to_date_string}"
    l_url = f"{p_job_diva_end_point_base_url}{p_job_diva_end_point}?{l_date_range}{p_user_name_fields}"

    print(l_url)
    time.sleep(2)
    response = requests.get(l_url, headers={'Authorization': 'Bearer %s' % p_job_diva_credential_token}, verify=True)

    l_column_names = []
    l_data_rows = []

    l_candidates_list = json.loads(response.text)["data"]
    if l_candidates_list is None:
        print(response.text)
    if len(l_candidates_list) <= 0:
        print("Received 0 rows from JobDiva")
        return l_column_names, l_data_rows
    if p_job_diva_end_point == "NewUpdatedTimesheetRecordsDailyBreakdown":
        read_response_json_df = spark.read.option("multiline", "true").json(sc.parallelize([response.text]))
        parse_json_df = read_response_json_df.select(explode("data.WEEKENDINGDATES").alias("WEEKENDINGDATES")) \
            .select(col("WEEKENDINGDATES.WEEKENDINGDATE").alias("WEEKENDINGDATE"),explode("WEEKENDINGDATES.EMPLOYEES").alias("EMPLOYEES")) \
            .select(col("WEEKENDINGDATE"),col("EMPLOYEES.EMPLOYEEID").alias("EMPLOYEEID"),col("EMPLOYEES.FIRSTNAME").alias("FIRSTNAME"),col("EMPLOYEES.LASTNAME").alias("LASTNAME"),explode("EMPLOYEES.JOBS").alias("JOBS")) \
            .select(col("WEEKENDINGDATE"),col("EMPLOYEEID"),col("FIRSTNAME"),col("LASTNAME"),col("JOBS.JOBID").alias("JOBID"),col("JOBS.TIMESHEETID").alias("TIMESHEETID"),col("JOBS.BILLING_RECID").alias("BILLING_RECID"),col("JOBS.SALARY_RECID").alias("SALARY_RECID"),col("JOBS.EXTERNALID").alias("EXTERNALID"),col("JOBS.EXTERNAL_INTEGRATIONID").alias("EXTERNAL_INTEGRATIONID"),col("JOBS.ECP_INTEGRATIONID").alias("ECP_INTEGRATIONID"),col("JOBS.BILLABLE").alias("BILLABLE"),col("JOBS.BILLINGEFFDATE").alias("BILLINGEFFDATE"),col("JOBS.SALARYEFFDATE").alias("SALARYEFFDATE"),col("JOBS.DATECREATED").alias("DATECREATED"),col("JOBS.DATEAPPROVED").alias("DATEAPPROVED"),col("JOBS.DATEUPDATED").alias("DATEUPDATED"),col("JOBS.STATUS").alias("STATUS"),col("JOBS.CUSTOMER_REFERENCE").alias("CUSTOMER_REFERENCE"),col("JOBS.DIVISION").alias("DIVISION"),col("JOBS.DIVISION_NAME").alias("DIVISION_NAME"),explode("JOBS.TIMESHEETS").alias("TIMESHEETS")) \
            .select(col("WEEKENDINGDATE"),col("EMPLOYEEID"),col("FIRSTNAME"),col("LASTNAME"),col("JOBID"),col("TIMESHEETID"),col("BILLING_RECID"),col("SALARY_RECID"),col("EXTERNALID"),col("EXTERNAL_INTEGRATIONID"),col("ECP_INTEGRATIONID"),col("BILLABLE"),col("BILLINGEFFDATE"),col("SALARYEFFDATE"),col("DATECREATED"),col("DATEAPPROVED"),col("DATEUPDATED"),col("STATUS"),col("CUSTOMER_REFERENCE"),col("DIVISION"),col("DIVISION_NAME"),col("TIMESHEETS.BILLRATE_NAME").alias("BILLRATE_NAME"),col("TIMESHEETS.HOURS_DESCRIPTION").alias("HOURS_DESCRIPTION"),col("TIMESHEETS.TDATE").alias("TDATE"),col("TIMESHEETS.HOURSWORKED").alias("HOURSWORKED"),col("TIMESHEETS.REGHOURS").alias("REGHOURS"),col("TIMESHEETS.OTHOURS").alias("OTHOURS"),col("TIMESHEETS.DTHOURS").alias("DTHOURS"),col("TIMESHEETS.REG_PAY_HOURS").alias("REG_PAY_HOURS"),col("TIMESHEETS.OT_PAY_HOURS").alias("OT_PAY_HOURS"),col("TIMESHEETS.DT_PAY_HOURS").alias("DT_PAY_HOURS"))
        parse_json_df = parse_json_df.withColumn("TIMESHEET_INDEX", row_number().over(Window.partitionBy("TIMESHEETID").orderBy("TDATE")))
        l_column_names =  parse_json_df.columns
        for l_data_row in parse_json_df.rdd.collect():
            l_data_rows.append(l_data_row)
        # print(l_column_names)
        # print(l_data_rows)
        
    else:
        
        if "apiv2" in p_job_diva_end_point_base_url:
            l_column_names = list(l_candidates_list[0].keys())
        else:
            l_column_names = l_candidates_list[0]
    
        if not "apiv2" in p_job_diva_end_point_base_url:
            if len(l_candidates_list) <= 1:
                return l_column_names, l_data_rows
    
        # l_column_names = l_column_names_tmp[:-1]
    
        l_column_names = remove_spaces_from_column_names(l_column_names)
    
        l_data_rows = []
        if not "apiv2" in p_job_diva_end_point_base_url:
            l_candidates_list = l_candidates_list[1:]
        for l_candidate in l_candidates_list:
            if "apiv2" in p_job_diva_end_point_base_url:
                l_data_row = list(l_candidate.values())
            else:
                l_data_row = l_candidate
            # l_data_row = l_data_row_tmp[:-1]
            # print(l_data_row)
            l_debug_id = l_data_row[0]
            if l_debug_id == '21219748':
                print("Got '21219748'")
            for index in range(len(l_data_row)):
                l_tmp_data = l_data_row[index]
                if "\n" in l_tmp_data:
                    l_tmp_data = l_tmp_data.replace("\n", "<BR>")
                    l_tmp_data = l_tmp_data.replace("\r", "<BR>")
                    l_data_row[index] = l_tmp_data
                    # print(l_tmp_data)
            l_data_rows.append(l_data_row)

    return l_column_names, l_data_rows


def update_last_source_ts(p_wh_connection, p_jobdiva_endpoint, p_last_source_ts):
    l_last_source_ts_string = str(p_last_source_ts)
    print(p_last_source_ts)
    l_update_stmt = f"""update datamesh_config.jobdiva_elt_config
    set last_source_ts = %s
    where jobdiva_end_point = '{p_jobdiva_endpoint}'
"""

    cur = p_wh_connection.cursor()
    cur.execute(l_update_stmt, (l_last_source_ts_string,))
    cur.close()
    p_wh_connection.commit()


def get_last_source_ts(p_env, p_jobdiva_endpoint):
    l_sql_stmt = f"""select last_source_ts 
    from datamesh_config.jobdiva_elt_config 
    where jobdiva_end_point = '{p_jobdiva_endpoint}'
"""

    l_secrets = get_secret(p_env)
    p_wh_connection = get_warehouse_connection(l_secrets)
    cur = p_wh_connection.cursor()
    cur.execute(l_sql_stmt)

    records = cur.fetchall()
    for row in records:
        l_last_source_ts = row[0]
        l_last_source_ts = pytz.utc.localize(l_last_source_ts)
        return l_last_source_ts
    cur.close()

    p_wh_connection.commit()

    l_last_source_ts = datetime.datetime(2020, 1, 1, timezone("America/New_York"))
    return l_last_source_ts


def source_job_diva_records(p_env, p_now, p_job_diva_config, p_job_diva_credential_token):
    p_job_diva_end_point = p_job_diva_config["job_diva_end_point"]
    p_job_diva_udf_list_name = p_job_diva_config["job_diva_udf_list_name"]
    r_user_name_fields_with_spaces, r_user_name_fields, r_udf_field_types, r_udf_field_sizes = get_user_defined_fields(
        p_job_diva_udf_list_name
        , p_job_diva_credential_token)
    if len(r_user_name_fields) > 0:
        r_user_name_fields = "&" + "&".join(r_user_name_fields_with_spaces)
    else:
        r_user_name_fields = ""

    # r_user_name_fields = "&Worker_Type"
    # r_user_name_fields = r_user_name_fields.replace(' ', '%20')

    l_column_names = None
    l_consolidated_data_rows = []
    l_to_date = get_last_source_ts(p_env, p_job_diva_end_point)
    l_from_date = l_to_date + datetime.timedelta(seconds=1)
    l_reached_now_flag = False

    while l_from_date < p_now:
        l_to_date = l_from_date + datetime.timedelta(days=13, hours=23, minutes=59, seconds=59)
        if l_to_date > p_now:
            l_to_date = p_now.replace(microsecond=0)
            l_reached_now_flag = True

        r_column_names, r_data_rows = source_job_diva_records_for_date_range(p_job_diva_config
                                                                             , p_job_diva_credential_token
                                                                             , l_from_date, l_to_date
                                                                             , r_user_name_fields)
        l_column_names = r_column_names
        l_consolidated_data_rows.extend(r_data_rows)
        if len(r_data_rows) > 0:
            break

        if l_reached_now_flag:
            break

        l_from_date = l_to_date + datetime.timedelta(seconds=1)

    return l_column_names, l_consolidated_data_rows, l_to_date


def get_column_names_in_table(p_wh_connection, p_temp_schema_name, p_consumption_table_name):
    l_column_names_with_casting = []
    l_column_names = []
    selected_columns = """column_name, udt_name, character_maximum_length, numeric_precision, numeric_scale"""
    # selected_columns = "*"

    l_sql_stmt = f"""SELECT {selected_columns}
    FROM information_schema.columns
    WHERE table_schema = '{p_temp_schema_name}'
    AND table_name   = '{p_consumption_table_name.lower()}_tmp2'
    """
    cur = p_wh_connection.cursor()
    cur.execute(l_sql_stmt)
    # colnames = [desc[0] for desc in cur.description]
    # print(colnames)
    records = cur.fetchall()
    for row in records:
        l_column_name = row[0]
        if l_column_name.lower() == "primary":
            l_column_name = f'"{l_column_name}"'
        l_column_names_with_casting.append(f"{l_column_name}::{row[1]}")
        l_column_names.append(f"{l_column_name}")
    cur.close()

    return l_column_names, l_column_names_with_casting


def transfer_from_temp1_to_temp2(p_wh_connection, p_temp_schema_name, p_consumption_table_name):
    r_column_names, r_column_names_with_casting = get_column_names_in_table(p_wh_connection, p_temp_schema_name
                                                                            , p_consumption_table_name)

    l_column_names = []
    for l_column_name in r_column_names:
        if l_column_name.lower() == "primary":
            l_column_name = f'"{l_column_name}"'
        l_column_names.append(l_column_name)
    truncate_stmt = f"""truncate table {p_temp_schema_name}.{p_consumption_table_name}_tmp2"""
    cur = p_wh_connection.cursor()
    cur.execute(truncate_stmt)

    insert_stmt = f"""insert into {p_temp_schema_name}.{p_consumption_table_name}_tmp2({",".join(l_column_names)})
select {",".join(r_column_names_with_casting)}
from {p_temp_schema_name}.{p_consumption_table_name}_tmp1 """
    print(insert_stmt)
    cur.execute(insert_stmt)

    cur.close()
    p_wh_connection.commit()


def transfer_from_temp2_to_consumption_table(p_wh_connection, p_temp_schema_name, p_consumption_schema_name
                                             , p_consumption_table_name, p_primary_keys):
    r_column_names, r_column_names_with_casting = get_column_names_in_table(p_wh_connection, p_temp_schema_name
                                                                            , p_consumption_table_name)
    delete_stmt = f"""delete from {p_consumption_schema_name}.{p_consumption_table_name}
where ({",".join(p_primary_keys)}) in
(
  select {",".join(p_primary_keys)}
  from {p_temp_schema_name}.{p_consumption_table_name}_tmp2
)
"""
    cur = p_wh_connection.cursor()
    cur.execute(delete_stmt)
    row_count = cur.rowcount
    # print(f"Deleted {row_count} rows from {p_consumption_schema_name}.{p_consumption_table_name}")

    insert_stmt = f"""insert into {p_consumption_schema_name}.{p_consumption_table_name}({",".join(r_column_names)})
select {",".join(r_column_names)}
from {p_temp_schema_name}.{p_consumption_table_name}_tmp2"""

    cur.execute(insert_stmt)
    row_count = cur.rowcount
    # print(f"Inserted {row_count} rows into {p_consumption_schema_name}.{p_consumption_table_name}")

    cur.close()
    p_wh_connection.commit()


def load_ingested_data_to_warehouse_temp1_table(p_wh_connection, s3_key, p_temp_schema_name, p_consumption_table_name
                                                , p_column_names, p_secrets, p_bucket_name):
    l_column_names = []
    for l_column_name in p_column_names:
        if l_column_name.lower() == "primary":
            l_column_name = f'"{l_column_name}"'
        l_column_names.append(l_column_name)

    l_full_table_name = f'{p_temp_schema_name}.{p_consumption_table_name}_tmp1'
    sql_stmt = f"""SELECT aws_s3.table_import_from_s3(
    '{l_full_table_name}'
    , '{','.join(l_column_names).replace(" ", "_")}'
    , '(format csv, header true)',
    '{p_bucket_name}',
    '{s3_key}',
    'us-east-1',
    '{p_secrets["aws_access_key_id"]}', '{p_secrets["aws_secret_access_key"]}'
    )"""

    # print(sql_stmt)
    cur = p_wh_connection.cursor()
    cur.execute(f"truncate table {l_full_table_name}")
    cur.execute(sql_stmt)
    records = cur.fetchall()
    for row in records:
        print("db = ", row)
    cur.close()
    cur.close()
    p_wh_connection.commit()


def push_ingested_data_to_warehouse(p_wh_connection, s3_key, p_temp_schema_name, p_consumption_schema_name
                                    , p_consumption_table_name, p_primary_keys, p_column_names
                                    , p_secrets, p_bucket_name):
    load_ingested_data_to_warehouse_temp1_table(p_wh_connection, s3_key, p_temp_schema_name, p_consumption_table_name
                                                , p_column_names, p_secrets, p_bucket_name)

    transfer_from_temp1_to_temp2(p_wh_connection, p_temp_schema_name, p_consumption_table_name)

    transfer_from_temp2_to_consumption_table(p_wh_connection, p_temp_schema_name, p_consumption_schema_name
                                             , p_consumption_table_name, p_primary_keys)


def ingest_raw_file_in_emr(p_env, p_emr_cluster_id, p_raw_bucket_name, p_s3_key, p_delta_lake_table_name):
    l_emr_workspace_id = 'e-DAKDX3VYM2MDETRSMJFPXFPDD'
    l_emr_workspace_id = 'e-5WAU29XXIWP9Z3UTXVSLYSZ9N'

    if p_env == "prod":
        l_emr_workspace_id = "e-40M1G54K0LK7PL1EH8R8NY4P7"
    l_notebook_name = 'ingest_into_deltalake_table.ipynb'

    # response = ingest_raw_file_in_emr(p_s3_key, p_delta_lake_table_name)

    emr = boto3.client('emr', region_name='us-east-1')

    l_full_s3_key = f"s3://{p_raw_bucket_name}/{p_s3_key}"
    params = {'p_s3_key': l_full_s3_key, 'p_delta_lake_table_name': p_delta_lake_table_name,
              'p_delta_lake_schema_name': 'CONS_JOBDIVA'}

    start_resp = emr.start_notebook_execution(
        EditorId=l_emr_workspace_id,
        RelativePath=l_notebook_name,
        ExecutionEngine={'Id': p_emr_cluster_id},
        NotebookParams=str(params),
        ServiceRole='EMR_Notebooks_DefaultRole'
    )

    execution_id = start_resp["NotebookExecutionId"]

    if start_resp['ResponseMetadata']['HTTPStatusCode'] == 200:
        print(start_resp)
        print("\n")

        r_notebook_execution_id = start_resp['NotebookExecutionId']
        log_emr_notebook_execution_url(p_env, r_notebook_execution_id, l_notebook_name, l_emr_workspace_id)

        wait_on_emr(emr, execution_id)

        describe_response = emr.describe_notebook_execution(NotebookExecutionId=execution_id)

        print(describe_response)
        print("\n")

        r_notebook_execution_id = start_resp['NotebookExecutionId']
        log_emr_notebook_execution_url(p_env, r_notebook_execution_id, l_notebook_name, l_emr_workspace_id)

        return start_resp

    error_message = f"Error launching EMR job with id {execution_id} : {start_resp['ResponseMetadata']}"
    raise Exception(error_message)


def log_emr_notebook_execution_url(p_env, p_notebook_execution_id, p_notebook_name, p_emr_workspace_id):
    l_log_uri = f"https://s3.console.aws.amazon.com/s3/object/dsp-{p_env}-datamesh-s3"
    l_log_params = "?region=us-east-1&prefix=EMR/STUDIO/WORKSPACE/"
    log_url = f"{l_log_uri}{l_log_params}{p_emr_workspace_id}/executions/{p_notebook_execution_id}/{p_notebook_name}"

    print("Notebook Logs available at:")
    print(log_url)


def wait_on_emr(emr, execution_id):
    life_cycle_state = 'RUNNING'
    remaining_sleep_time_in_seconds = 30 * 60 * 60
    total_sleep_time_in_seconds = 0

    first_time_flag = True

    while life_cycle_state != 'FINISHED':
        if first_time_flag:
            first_time_flag = False
        else:
            time.sleep(10)
            total_sleep_time_in_seconds += 10
            remaining_sleep_time_in_seconds -= 10

        response = emr.describe_notebook_execution(NotebookExecutionId=execution_id)

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            life_cycle_state = response['NotebookExecution']['Status']
            print(f"{total_sleep_time_in_seconds} seconds - {life_cycle_state}")

            if life_cycle_state in ['STARTING', 'START_PENDING', 'RUNNING', 'FAILING', 'FINISHING']:
                continue
            if life_cycle_state == 'FAILED':
                error_string = f"Job id {execution_id} did not succeed, it is in {life_cycle_state} state."
                raise Exception(error_string)
        else:
            print("Error looking up job run status")
            print(response['ResponseMetadata'])

        if remaining_sleep_time_in_seconds <= 0:
            raise Exception("Job still running after 30 mins")


def standardize_date_formats(p_column_names, p_data_rows, p_job_diva_config):
    if len(p_data_rows) <= 0:
        return

    if "date_types" not in p_job_diva_config:
        return
    
    l_date_column_names = p_job_diva_config["date_types"]["date_type_columns"]
    l_date_format = p_job_diva_config["date_types"]["date_format"]
    
    min_allowed_date = datetime.datetime(year=1678, month=9, day=21)

    for l_column_name in l_date_column_names:
        index = p_column_names.index(l_column_name)
        for p_data_row in p_data_rows:
            if p_data_row[index] is None:
                continue
            if p_data_row[index] == "":
                continue
            # print(p_data_row[index])
            try:
                date_time_obj = datetime.datetime.strptime(p_data_row[index], l_date_format)
                if date_time_obj < min_allowed_date:
                    date_time_obj = min_allowed_date
            except Exception:
                print(traceback.format_exc())
                str_date = p_data_row[index]
                print(str_date)
                raise
            p_data_row[index] = date_time_obj.strftime('%Y-%m-%d')
            # print(p_data_row[index])


def print_debug_data(p_job_diva_config, p_column_names, p_data_rows):
    l_job_diva_end_point = p_job_diva_config["job_diva_end_point"]
    if l_job_diva_end_point == "NewUpdatedCompanyRecords":
        l_column_name = "S4_Company_ID"
    elif l_job_diva_end_point == "NewUpdatedCandidateRecords":
        l_column_name = "CANDIDATEID"
    else:
        return

    field_name_index = p_column_names.index(l_column_name)
    l_all_blanks = True
    for l_data_row in p_data_rows:
        if "12912273396834" == l_data_row[0]:
            print("Gotcha!!")

        if '13787226412780' == l_data_row[0]:
            print(l_data_row)
            print("Gotcha123")
        l_the_value = l_data_row[field_name_index]
        if l_the_value == None:
            continue
        if l_the_value == "":
            continue
        l_all_blanks = False
        # print(f"{l_column_name} is {l_data_row[field_name_index]}")

    if l_all_blanks:
        print(f"all values are blank for {l_column_name}")
    else:
        print(f"got at-least one non-blank value for {l_column_name}")


def process_a_job_diva_api(p_env, p_now, p_emr_cluster_id, p_reference_data_flag, p_job_diva_config):
    r_job_diva_credential_token = get_token_from_credentials(p_env)
    if not p_reference_data_flag:
        r_column_names, r_data_rows, r_to_date = source_job_diva_records(p_env, p_now, p_job_diva_config,
                                                                         r_job_diva_credential_token)
    else:
        r_column_names, r_data_rows = source_a_job_diva_reference_data(
            p_job_diva_config["job_diva_end_point"]
            , r_job_diva_credential_token)

    if len(r_data_rows) <= 0:
        return 0

    # print_debug_data(p_job_diva_config, r_column_names, r_data_rows)

    standardize_date_formats(r_column_names, r_data_rows, p_job_diva_config)
    r_bucket_name, r_s3_key, r_time_stamp = upload_to_s3(p_env, r_column_names, r_data_rows
                                                         , p_job_diva_config["job_diva_udf_list_name"])
                                                         
    l_temp_schema_name = "stg_jobdiva"
    l_consumption_schema_name = "cons_jobdiva"
    l_consumption_table_name = p_job_diva_config["job_diva_consumption_table_name"]
    l_primary_keys = p_job_diva_config["primary_keys"]
    
    l_column_names,l_transform_column_names,l_date_columns,l_timestamp_columns,l_numeric_columns,l_text_columns = get_columns_and_typecasts_from_table(p_schema_name,p_table_name,l_conn)
    process_bad_records(r_bucket_name, r_s3_key, l_consumption_table_name, l_primary_keys,  l_date_columns,l_timestamp_columns,l_numeric_columns,l_text_columns)
    
    #ingest_raw_file_in_emr(p_env, p_emr_cluster_id, r_bucket_name, r_s3_key, l_consumption_table_name)

    l_secrets = get_secret(p_env)
    l_wh_connection = get_warehouse_connection(l_secrets)

    # push_ingested_data_to_warehouse(l_wh_connection, r_s3_key, l_temp_schema_name, l_consumption_schema_name
    #                                 , l_consumption_table_name, l_primary_keys, r_column_names
    #                                 , l_secrets, r_bucket_name)

    if not p_reference_data_flag:
        update_last_source_ts(l_wh_connection, p_job_diva_config["job_diva_end_point"], r_to_date)

    l_wh_connection.commit()

    return len(r_data_rows)


def get_job_diva_configs():
    """l_job_diva_configs = [
        {"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedJobRecords'
            , "job_diva_udf_list_name": 'Jobs'
            , "job_diva_consumption_table_name": 'Jobs'
            , "primary_keys": ['JobID']
            , "excel_sheet_name": "NewUpdatedJobRecords"
            , "spreadsheet_start_row_number": 3
            , "isReferenceData": False}
        ,{"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedCompanyRecords'
            , "job_diva_udf_list_name": 'Companies'
            , "job_diva_consumption_table_name": 'Companies'
            , "primary_keys": ['CompanyId']
            , "excel_sheet_name": "NewUpdatedCompanyRecords"
            , "spreadsheet_start_row_number": 3
            , "isReferenceData": False}
        , {"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedBillingRecords'
            , "job_diva_udf_list_name": 'Billings'
            , "job_diva_consumption_table_name": 'Billings'
            , "primary_keys": ['ACTIVITYID', 'RECID']
            , "date_types": {"date_format": "%m/%d/%Y", "date_type_columns": ['START_DATE', 'END_DATE']}
            , "isReferenceData": False}
        , {"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedApprovedTimesheetRecords'
            , "job_diva_udf_list_name": 'ApprovedTimesheets'
            , "job_diva_consumption_table_name": 'Approved_Timesheets'
            , "primary_keys": ['TimesheetId']
            , "excel_sheet_name": "NewUpdatedApprovedTimesheetReco"
            , "spreadsheet_start_row_number": 2, "isReferenceData": False}
        , {"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedCandidateRecords'
            , "job_diva_udf_list_name": 'Candidates'
            , "job_diva_consumption_table_name": 'Candidates'
            , "primary_keys": ['CandidateId']
            , "excel_sheet_name": "NewUpdatedCandidateRecords"
            , "spreadsheet_start_row_number": 3
            , "isReferenceData": False}
        , {"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedMilestoneRecords'
            , "job_diva_consumption_table_name": "Milestones"
            , "job_diva_udf_list_name": 'Milestones'
            , "primary_keys": ['Milestone_Id']
            , "excel_sheet_name": "NewUpdatedMilestoneRecords"
            , "spreadsheet_start_row_number": 3
            , "isReferenceData": False}
        , {"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedTimesheetRecords'
            , "job_diva_udf_list_name": 'Timesheets'
            , "job_diva_consumption_table_name": 'Timesheets'
            , "primary_keys": ['TimesheetId'], "excel_sheet_name": "NewUpdatedTimesheetRecords"
            , "spreadsheet_start_row_number": 3, "isReferenceData": False}
        , {"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedApprovedExpenseRecords'
            , "job_diva_udf_list_name": 'ApprovedExpense'
            , "job_diva_consumption_table_name": 'Approved_Expenses'
            , "primary_keys": ['ExpenseId']
            , "excel_sheet_name": "NewUpdatedApprovedExpenseRecord"
            , "spreadsheet_start_row_number": 2
            , "isReferenceData": False}
        , {"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedExpenseRecords'
            , "job_diva_udf_list_name": 'Expense'
            , "job_diva_consumption_table_name": 'Expenses'
            , "primary_keys": ['ExpenseId']
            , "excel_sheet_name": "NewUpdatedExpenseRecords"
            , "spreadsheet_start_row_number": 3
            , "isReferenceData": False}
        , {"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedSalaryRecords'
            , "job_diva_udf_list_name": 'Salary'
            , "job_diva_consumption_table_name": 'Salary'
            , "primary_keys": ['EMPLOYEEID', 'RECID']
            , "date_types": {"date_format": "%Y-%m-%dT%H:%M:%S", "date_type_columns": ['EFFECTIVE_DATE', 'END_DATE']}
            , "excel_sheet_name": "NewUpdatedSalaryRecords"
            , "spreadsheet_start_row_number": 3, "isReferenceData": False}
        , {"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'SOWList'
            , "job_diva_udf_list_name": 'SOW'
            , "job_diva_consumption_table_name": 'SOW_List'
            , "primary_keys": ['Id']
            , "excel_sheet_name": "SOWList"
            , "spreadsheet_start_row_number": 2
            , "isReferenceData": True}
        , {"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedInvoices'
            , "job_diva_udf_list_name": 'Invoices'
            , "job_diva_consumption_table_name": 'Invoices'
            , "primary_keys": ['InvoiceId']
            , "excel_sheet_name": "NewUpdatedInvoices"
            , "spreadsheet_start_row_number": 2
            , "isReferenceData": False}
        ,{"job_diva_end_point_base_url": 'https://api.jobdiva.com/api/bi/'
            , "job_diva_end_point": 'NewUpdatedContactRecords'
            , "job_diva_udf_list_name": 'Contact'
            , "job_diva_consumption_table_name": 'Contact'
            , "primary_keys": ['ContactId']
            , "excel_sheet_name": "NewUpdatedContactRecords"
            , "spreadsheet_start_row_number": 2
            , "isReferenceData": False}
        , {"job_diva_end_point_base_url": 'https://api.jobdiva.com/api/bi/'
            , "job_diva_end_point": 'NewUpdatedCandidateQualificationRecords'
            , "job_diva_udf_list_name": 'CandidateQualification'
            , "job_diva_consumption_table_name": "Candidate_Qualification"
            , "primary_keys": ['QUALIFICATIONID', 'CANDIDATEID', 'QUALVALUE', 'DELETEFLAG']
            , "excel_sheet_name": "NewUpdatedCandidateQualificationRecords"
            , "spreadsheet_start_row_number": 2, "isReferenceData": False}
        , {"job_diva_end_point_base_url": 'https://api.jobdiva.com/api/bi/'
            , "job_diva_end_point": 'UsersList'
            , "job_diva_udf_list_name": 'UsersList'
            , "job_diva_consumption_table_name": "userslist"
            , "primary_keys": ['UserId']
            , "isReferenceData": True}
        ,{"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NonCompletedOnboardingPackages'
            , "job_diva_udf_list_name": 'NonCompletedOnboardingPackages'
            , "job_diva_consumption_table_name": 'Non_Completed_Onboarding_Packages'
            , "primary_keys": ['OnboardingId']
            , "excel_sheet_name": "NonCompletedOnboardingPackages"
            , "spreadsheet_start_row_number": 2
            , "isReferenceData": False}
        ,{"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'CompletedOnboardingPackages'
            , "job_diva_udf_list_name": 'CompletedOnboardingPackages'
            , "job_diva_consumption_table_name": 'Completed_Onboarding_Packages'
            , "primary_keys": ['OnboardingId']
            , "excel_sheet_name": "CompletedOnboardingPackages"
            , "spreadsheet_start_row_number": 2
            , "isReferenceData": False}
        ,{"job_diva_end_point_base_url": 'https://api.jobdiva.com/api/bi/'
            , "job_diva_end_point": 'NewUpdatedActivityRecords'
            , "job_diva_udf_list_name": 'Activities'
            , "job_diva_consumption_table_name": 'Activities'
            , "primary_keys": ['ActivityId']
            , "excel_sheet_name": "NewUpdatedActivityRecords"
            , "spreadsheet_start_row_number": 2
            , "isReferenceData": False}
        ,{"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedTaskRecords'
            , "job_diva_udf_list_name": 'Tasks'
            , "job_diva_consumption_table_name": 'Tasks'
            , "primary_keys": ['Id']
            , "excel_sheet_name": "NewUpdatedTaskRecords"
            , "spreadsheet_start_row_number": 2
            , "isReferenceData": False}
        ,{"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedTimesheetRecordsDailyBreakdown'
            , "job_diva_udf_list_name": 'TimesheetDailyBreakdown'
            , "job_diva_consumption_table_name": 'timesheets_daily_breakdown'
            , "primary_keys": ['EmployeeId','JobId','TimesheetId','TIMESHEET_INDEX']
            , "excel_sheet_name": "NewUpdatedTimesheetRecordsDailyBreakdown"
            , "spreadsheet_start_row_number": 3
            , "isReferenceData": False}
        ,{"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'CandidateOnboardingDocumentsByDate'
            , "job_diva_udf_list_name": 'CandidateOnboardingDocuments'
            , "job_diva_consumption_table_name": "Candidate_Onboarding_Tasks"
            , "primary_keys": ['CANDIDATEID', 'DOC_ID', 'PACKAGE_ID', 'GROUP_ID','STARTID']
            , "excel_sheet_name": "CandidateOnboardingDocumentsByDate"
            , "spreadsheet_start_row_number": 2, "isReferenceData": False}
    ]"""

    l_job_diva_configs = [
         {"job_diva_end_point_base_url": 'https://api.jobdiva.com/apiv2/bi/'
            , "job_diva_end_point": 'NewUpdatedBillingRecords'
            , "job_diva_udf_list_name": 'Billings'
            , "job_diva_consumption_table_name": 'Billings'
            , "primary_keys": ['ACTIVITYID', 'RECID']
            , "date_types": {"date_format": "%m/%d/%Y", "date_type_columns": ['START_DATE', 'END_DATE']}
            , "isReferenceData": False}
         ]
    # temp_job_diva_configs = []
    # for a_job_diva_config in l_job_diva_configs:
    #     if not a_job_diva_config['job_diva_end_point'] == "NewUpdatedInvoices":
    #         continue
    #     temp_job_diva_configs.append(a_job_diva_config)
    # return temp_job_diva_configs
    return l_job_diva_configs


def source_job_diva_data(p_env, p_now, p_emr_cluster_id, p_job_diva_config):
    r_job_diva_configs = get_job_diva_configs()

    if p_job_diva_config is not None:
        r_job_diva_configs = [p_job_diva_config]

    for l_job_diva_config in r_job_diva_configs:
        l_reference_data_flag = l_job_diva_config['isReferenceData']
        l_job_diva_end_point = l_job_diva_config['job_diva_end_point']

        l_num_retries = 1
        for x in range(l_num_retries):
            try:
                if x > 0:
                    print(f"retry number x={x}")
                r_count = 10000000
                while r_count > 0:
                    print(f"Starting processing for {l_job_diva_end_point}")
                    r_count = process_a_job_diva_api(p_env, p_now, p_emr_cluster_id, l_reference_data_flag, l_job_diva_config)
                    if r_count > 0:
                        print(f"\nProcessed {r_count} records for {l_job_diva_config['job_diva_end_point']}\n")
                    else:
                        print(f"\nNo more New/Updated records to process from {l_job_diva_end_point}\n")
                    if l_reference_data_flag:
                        break

                break
            except Exception:
                print(traceback.format_exc())
                raise Exception("JobDiva processing failed")

def print_full(x):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 2000)
    pd.set_option('display.float_format', '{:20,.2f}'.format)
    pd.set_option('display.max_colwidth', None)
    print(x)

def load_bad_data_to_warehouse_table(p_wh_connection, s3_key, p_error_log_schema_name, p_error_log_table_name
                                                , p_secrets, p_bucket_name):
  
    l_full_table_name = f'{p_error_log_schema_name}.{p_error_log_table_name}'
    sql_stmt = f"""SELECT aws_s3.table_import_from_s3(
    '{l_full_table_name}'
    , ''
    , '(format csv, header true)',
    '{p_bucket_name}',
    '{s3_key}',
    'us-east-1',
    '{p_secrets["aws_access_key_id"]}', '{p_secrets["aws_secret_access_key"]}'
    )"""

    cur = p_wh_connection.cursor()
    cur.execute(sql_stmt)
    records = cur.fetchall()
    for row in records:
        print("db = ", row)
    cur.close()
    p_wh_connection.commit()

def load_bad_records(p_list,df,p_table_name,p_primary_keys,p_wh_connection,p_secrets,p_raw_bucket_name,p_full_s3_key,p_status):
    bad_data_df = []
    for col in p_list:
        filtered_df = df.loc[(df[col].notnull() & df[col + "_1"].isnull())]
        bad_data_df.append(filtered_df)
        
    bad_data_df = pd.concat(bad_data_df)
    # print(p_status," - ",len(bad_data_df))
    if len(bad_data_df) > 0:
        bad_data_json = bad_data_df[set(p_primary_keys + p_list)].to_json(orient = 'records')
        bad_data_records = {'data_source': ['JobDiva'],
            'table_name': [p_table_name],
            'record_identifiers': [','.join(p_primary_keys)],
            'source_of_error':[','.join(p_list)],
            'status':[p_status],
            'bad_records':bad_data_json,
            'created_date': str(datetime.datetime.now())
        }
     
        bad_data_records_df = pd.DataFrame(bad_data_records)
        tz = timezone('America/New_York')
        now = str(datetime.datetime.now(tz)).replace(' ', '/')
        now = now.replace(':', '.')
        error_log_s3_key = f"RAW/JobDiva/ErrorLog/{now}/ErrorLog.csv"
        l_error_log_full_s3_key = f"s3://{p_raw_bucket_name}/{error_log_s3_key}"
        bad_data_records_df.to_csv(l_error_log_full_s3_key, index=False)
        load_bad_data_to_warehouse_table(p_wh_connection, error_log_s3_key, "cons_jobdiva", "error_log", p_secrets, p_raw_bucket_name)
    
        for col in p_list:
            df.rename(columns = {col:col+"_2"}, inplace = True)
            df.rename(columns = {col+"_1":col}, inplace = True)
        
        drop_cols = [col + "_2" for col in p_list]
        df.drop(columns=drop_cols, axis=1, inplace = True)
        df.to_csv(p_full_s3_key, date_format='%Y-%m-%d', index=False)
        
def check_duplicate_records(p_table_name,p_primary_keys,p_wh_connection,p_secrets,p_raw_bucket_name,p_full_s3_key):
    df = pd.read_csv(p_full_s3_key)
    duplicate_df = df[df[p_primary_keys].duplicated() == True]
    print("Duplicate records - ",len(duplicate_df))
    print(duplicate_df)
    if len(duplicate_df) > 0:
        duplicate_data_json = duplicate_df.to_json(orient = 'records')
        bad_data_records = {'data_source': ['JobDiva'],
            'table_name': [p_table_name],
            'record_identifiers': [','.join(p_primary_keys)],
            'source_of_error':[''],
            'status':['Duplicate Records'],
            'bad_records':duplicate_data_json,
            'created_date': str(datetime.datetime.now())
        }
     
        bad_data_records_df = pd.DataFrame(bad_data_records)
        tz = timezone('America/New_York')
        now = str(datetime.datetime.now(tz)).replace(' ', '/')
        now = now.replace(':', '.')
        error_log_s3_key = f"RAW/JobDiva/ErrorLog/{now}/ErrorLog.csv"
        l_error_log_full_s3_key = f"s3://{p_raw_bucket_name}/{error_log_s3_key}"
        bad_data_records_df.to_csv(l_error_log_full_s3_key, date_format='%Y-%m-%d', index=False)
        load_bad_data_to_warehouse_table(p_wh_connection, error_log_s3_key, "cons_jobdiva", "error_log", p_secrets, p_raw_bucket_name)
        non_duplicate_df = df[df[p_primary_keys].duplicated() == False]
        non_duplicate_df.to_csv(p_full_s3_key, date_format='%Y-%m-%d', index=False)
    
    
def process_bad_records(p_raw_bucket_name, p_s3_key, p_table_name, p_schema_name, p_primary_keys, p_date_columns,p_timestamp_columns,p_numeric_columns,p_text_columns, p_secrets,p_wh_connection):
    # print(p_date_columns)
    # print(p_timestamp_columns)
    # print(p_numeric_columns)
    # print(p_text_columns)
    l_full_s3_key = f"s3://{p_raw_bucket_name}/{p_s3_key}"
    # print(l_full_s3_key)
    check_duplicate_records(p_table_name,p_primary_keys,p_wh_connection,p_secrets,p_raw_bucket_name,l_full_s3_key)
    
    if p_timestamp_columns:
        df = pd.read_csv(l_full_s3_key)
        for col in p_timestamp_columns:
            df[col+"_1"] = pd.to_datetime(df[col],format='%Y-%m-%dT%H:%M:%S',errors='coerce')
            df[col+"_1"] = df[col+"_1"].dt.strftime('%Y-%m-%dT%H:%M:%S')
        # print(tabulate(df, headers='keys', tablefmt='psql'))  
        # print(df)
        load_bad_records(p_timestamp_columns,df,p_table_name,p_primary_keys,p_wh_connection,p_secrets,p_raw_bucket_name,l_full_s3_key,'Invalid Timestamp')
    
    if p_date_columns:
        df = pd.read_csv(l_full_s3_key)
        for col in p_date_columns:
            df[col+"_1"] = pd.to_datetime(df[col],format='%Y-%m-%d',errors='coerce')
            df[col+"_1"] = df[col+"_1"].dt.strftime('%Y-%m-%d')
        # print_full(df)
        load_bad_records(p_date_columns,df,p_table_name,p_primary_keys,p_wh_connection,p_secrets,p_raw_bucket_name,l_full_s3_key,'Invalid Date')   
    
    if p_numeric_columns:
        df = pd.read_csv(l_full_s3_key)
        for col in p_numeric_columns:
            df[col+"_1"] = pd.to_numeric(df[col],errors='coerce')
            p_schema_name = p_schema_name.lower()
            p_table_name = p_table_name.lower()
            col_lower = col.lower()
            sql_stmt = f"""SELECT numeric_precision, numeric_scale
                      FROM information_schema.columns
                      WHERE table_schema = '{p_schema_name}'
                      AND table_name   = '{p_table_name}'
                      and column_name = '{col_lower}'"""
            cur = p_wh_connection.cursor()
            cur.execute(sql_stmt)
            records = cur.fetchall()
            numeric_precesion, numeric_scale = records[0][0], records[0][1]
            temp = "9" * (numeric_precesion - numeric_scale)
            temp = int(temp)

            for i in range(len(df[col+"_1"])):
                if df[col+"_1"][i] is not math.nan:
                    if df[col][i] > temp:
                        df[col+"_1"][i] = math.nan
        load_bad_records(p_numeric_columns,df,p_table_name,p_primary_keys,p_wh_connection,p_secrets,p_raw_bucket_name,l_full_s3_key,'Invalid DataType Or Length')  
        
    if p_text_columns:
        df = pd.read_csv(l_full_s3_key)
        for col in p_text_columns:
            new_col_str = col+"_1";
            df[new_col_str] = np.nan
            # print(df[new_col_str])
            col_lower = col.lower()
            p_schema_name = p_schema_name.lower()
            p_table_name = p_table_name.lower()
            col_lower = col.lower()
            sql_stmt = f""" SELECT character_maximum_length
                            FROM information_schema.columns
                            WHERE table_schema = '{p_schema_name}'
                            AND table_name   = '{p_table_name}'
                            and column_name = '{col_lower}'"""
            cur = p_wh_connection.cursor()
            cur.execute(sql_stmt)
            records = cur.fetchall()
            character_maximum_length = records[0][0]
            
            for i in range(len(df[col])):
                print(f"outside + {i}")
                if isinstance(df[col][i], str):
                    print(f"inside + {i}")
                    if int(len(df[col][i])) > int(character_maximum_length):
                        df[new_col_str][i] = np.nan
                    else:
                        df[new_col_str][i] =  df[col][i]
                else:
                    df[col][i] = "temp"
                    df[new_col_str][i] = np.nan
        load_bad_records(p_text_columns,df,p_table_name,p_primary_keys,p_wh_connection,p_secrets,p_raw_bucket_name,l_full_s3_key,'Invalid Length')  

def get_columns_and_typecasts_from_table(p_temp_schema,p_table_name,p_connection):
    l_column_names = []
    l_transform_column_names = []
    selected_columns = """column_name, udt_name, character_maximum_length, numeric_precision, numeric_scale"""

    l_sql_stmt = f"""SELECT {selected_columns}
    FROM information_schema.columns
    WHERE table_schema = '{p_temp_schema}'
    AND table_name   = '{p_table_name}'
    """
    
    cur = p_connection.cursor()
    cur.execute(l_sql_stmt)
    records = cur.fetchall()
    l_date_columns = []
    l_timestamp_columns = []
    l_numeric_columns = []
    l_text_columns = []
    for row in records:
        if row[1].startswith('timestamp'):
            l_timestamp_columns.append(row[0].upper())
        elif row[1].startswith('varchar'):
            l_text_columns.append(row[0].upper())
        elif row[1].startswith('numeric'):
            l_numeric_columns.append(row[0].upper())
        elif row[1].startswith('date'):
            l_date_columns.append(row[0].upper())
    cur.close()
    p_connection.commit()
    #print("Get the columns of the delta table with type casting information.")
    return l_column_names,l_transform_column_names,l_date_columns,l_timestamp_columns,l_numeric_columns,l_text_columns
    
if __name__ == "__main__":
    
    # p_env='dev'
    l_env = 'dev'
    r_bucket_name='dsp-dev-datamesh-s3'
    r_s3_key = 'RAW/JobDiva/Billings/2023-01-10/06.46.10.096618-05.00/Billings_backup.csv'
    p_schema_name = 'cons_jobdiva'
    p_table_name = 'billings'
    l_consumption_table_name = 'Billings'
    l_primary_keys = ['ACTIVITYID', 'RECID']
    l_secrets = get_secret(l_env)
    l_conn = get_warehouse_connection(l_secrets)
    l_column_names,l_transform_column_names,l_date_columns,l_timestamp_columns,l_numeric_columns,l_text_columns = get_columns_and_typecasts_from_table(p_schema_name,p_table_name,l_conn)
    process_bad_records(r_bucket_name, r_s3_key, l_consumption_table_name, p_schema_name, l_primary_keys,  l_date_columns,l_timestamp_columns,l_numeric_columns,l_text_columns, l_secrets,l_conn)
    
    l_env = 'dev'
    # glue_client = boto3.client("glue")
    # args = getResolvedOptions(sys.argv, ["WORKFLOW_NAME", "WORKFLOW_RUN_ID"])
    # workflow_name = args["WORKFLOW_NAME"]
    # workflow_run_id = args["WORKFLOW_RUN_ID"]
    
    # workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)[
    #     "RunProperties"]
    # r_cluster_id = workflow_params["JOBDIVA_EMR_CLUSTER_ID"]
    r_cluster_id = ""
    #print(f"Got cluster id = {r_cluster_id} from workflow!!")

    # while True:
    #     # l_cluster_name = f"dsp-{l_env}-datamesh-emr-cluster_spark_3.0.1-jobdiva"
    #     # r_cluster_id, cluster_name, cluster_state, master_private_ip = \
    #     #     lec.get_me_an_emr_cluster_to_use(l_env, l_cluster_name)

    #     # r_cluster_id = "j-3CN4B34MTVGWS"
    #     tz = timezone('America/New_York')

    #     l_now = datetime.datetime.now(tz).replace(microsecond=0)
    #     print(f'now before value {l_now}')
    #     l_now = l_now - datetime.timedelta(seconds=10)
    #     print(f'now after value {l_now}')

    #     source_job_diva_data(l_env, l_now, r_cluster_id, None)
    #     break

    #     print("Done a JobDiva cycle!!!... Sleeping for 300 seconds...")
    #     time.sleep(300)
