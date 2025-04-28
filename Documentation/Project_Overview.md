# Fab Retailers Project

## Project Overview:
This project demonstrates the creation of a Snowflake data pipeline for a retail company with multiple departments like Sales & Marketing, HR, Finance, and IT. It includes real-time data ingestion, data masking for sensitive information, and sharing data with subsidiary companies alongwith visualisation using Snowsight dashboard.

## Technologies Used:
- Snowflake
- Python (for data generation scripts)
- SQL
- AWS EC2 (for running data creation scripts)
- AWS S3 (for storing the data)

## Folder Structure:
- **01_Setup**: Contains scripts for database schema setup, roles, and stages.
- **02_Departments**: Includes SQL for department-specific data.
- **03_RealTime_Ingestion**: Real-time ingestion scripts using Snowflake streams and tasks.
- **04_Security**: Scripts for security features like dynamic masking and secure views.
- **05_Subsidiaries**: Data sharing scripts for subsidiaries.
- **06_Dashboard**: Snowsight queries and dashboard notes.
- **Pre-requisites**: Ec2 data generation scripts and S3 bucket structure demonstration
- **Documenation**: Step by step guide of project workflow.

## Instructions:
1. Set up Snowflake account and configure trial account for the project.
2. Further, please refer to the structure_of_project and workflow diagrams for step by step instructions 

## Limitations:
- This project uses a Snowflake trial account with limited data and compute resources.
- Also due to the complexity, the project doesn't include a front end for now and only includes a dynamic Snowsight dashboard (Tableau connector isnt available Tableau free account). 
