#!/bin/bash

awslocal s3 mb s3://dagster-dbt-raw
awslocal s3 cp ./sample_data/sample_calls_25.csv s3://dagster-dbt-raw