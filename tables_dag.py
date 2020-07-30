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
#data storage
stg=models.Variable.get('stg_dataset');
#data warehouse
dw=models.Variable.get('dw_dataset');
#data mart for exploit
dm=models.Variable.get('dm_dataset');

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = models.DAG(
    dag_id='load_data_from_files', default_args=args,
    schedule_interval=None
)

create_tables_from_sql_bucket = BigQueryOperator(
    task_id='creating_tables',
    sql='fs-storage-cb-sh/tables/*.sql',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id ='bigquery_default',
    allow_large_results=True,
    use_legacy_sql=False,
    dag=dag)


create_tables_from_sql_bucket