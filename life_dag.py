# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Any

import airflow
from airflow import models
from airflow.operators import python_operator
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator;


# config variables
#dataset for examples
dm_ex=models.Variable.get('gcp_schema_bigquery');
#dataset storage
stg=models.Variable.get('stg_dataset');
#dataset warehouse
dw=models.Variable.get('dw_dataset');
#dataset mart for exploit
dm=models.Variable.get('dm_dataset');

default_dag_args  = {
    'start_date': airflow.utils.dates.days_ago(2)
}

with models.DAG(
    dag_id='load_data_from_files_v2', default_args=default_dag_args,
    schedule_interval=None
)as dag:

# [START howto_operator_gcs_to_bq]
load_table_a = GoogleCloudStorageToBigQueryOperator(
    task_id='load_table_a',
    bucket='fs-storage-cb-sh',
    source_objects=['LPMXfile_table_a_YYMMDD.txt'],
    field_delimiter='|',
    destination_project_dataset_table='gcp_schema_bigquery.gcp_table_a',
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

# [END howto_operator_gcs_to_bq]
copy_data_to_b = bigquery_operator.BigQueryOperator(
    task_id='copy_data_to_b',
    sql="""
        insert into {0}.TABLE_B select * from {1}.gcp_table_a
        """.format(dm_ex,dm_ex),
    big_query_conn_id='bigquery_default',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    dag=dag)

load_table_a >> copy_data_to_b