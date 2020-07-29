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
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator;

# config variables

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = models.DAG(
    dag_id='load_data_from_files', default_args=args,
    schedule_interval=None
)
DST_BUCKET_UTF8 = ('pubsite_prod_rev_ingestion/reviews_utf8')
# [START howto_operator_gcs_to_bq]
load_table_a = GoogleCloudStorageToBigQueryOperator(
    task_id='load_table_a',
    bucket='fs-storage-cb-sh',
    bucket=DST_BUCKET_UTF8,
    source_objects=['/LPMXfile_table_a_YYMMDD.txt'],
    field_delimiter='|',
    destination_project_dataset_table='gcp_schema_bigquery.gcp_table_a',
    schema_fields=[
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'age', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag)
# [END howto_operator_gcs_to_bq]
copy_data_to_b = BigQueryOperator(
    bucket='fs-storage-cb-sh',
    task_id='copy_data_to_b',
    source_objects=['/LPMXfile_table_a_YYMMDD.txt'],
    field_delimiter='|',
    destination_project_dataset_table='gcp_schema_bigquery.gcp_table_a',
    schema_fields=[
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'age', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

load_csv