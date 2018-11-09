Overview
====================

- This project explores the use case of anti-money laundering / fraud detection using Graph Databases
- Data set has been obtained from Kaggle: https://www.kaggle.com/ntnu-testimon/paysim1
- Aims to create an end to end solution from cloud architecture to application / api

Project Structure
========================

- app: Application / API source code. Runtime python2.7 on AWS Lambda + AWS API Gateway
- data: data source, data collection
- etl: ETL code running on AWS Glue
- infra: Infrastructure as Code to spin up all resources in AWS using Cloudformation
