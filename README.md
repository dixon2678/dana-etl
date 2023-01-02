# dana-etl
# Steps to test locally
- Run docker-compose up airflow-init
- Run docker-compose up
- Airflow UI will be running at localhost:5887 (User: airflow, Password: airflow)
- Set the required environment variables (access_token, creds, project_id)
- Unpause and Trigger the Data-Pipeline DAG

This project really reflects alot about my values in Data Engineering :)
For more details on it, or Airflow setup please check out this repository too! https://github.com/dixon2678/el-snowflake
