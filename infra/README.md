Why use sceptre for managing Cloudformation?
==============================================

[Github](https://github.com/cloudreach/sceptre) repo page outlines features + advantages

Launch AWS Neptune env
==========================

> sceptre launch-env dev


Launch AWS Neptune each stack individually
============================================

> sceptre launch-stack dev vpc

> sceptre launch-stack dev neptune

> sceptre launch-stack dev etl


Teardown AWS Neptune environment
============================================

> sceptre delete-env dev


Stack Breakdown
============================

- vpc:
    - VPC
    - VPC S3 Endpoint
    - Neptune Security Group
    - InternetGateway
    - 3 Subnets

- neptune:
    - Neptune DB Cluster
    - Neptune DB Instance
    - Neptune DB SubnetGroup (3 Subnets from VPC stack)
    - IAM Role for loading data from S3 to Neptune

- etl:
    - S3 bucket: for storing ETL scripts, raw data, transformed data
    - Glue Database
    - Glue Crawler
    - Glue Job
    - Glue IAM Role