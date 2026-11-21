import time
import os
from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import pendulum 
from airflow.sdk import Variable

DATABRICKS_HOST = Variable.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = Variable.get("DATABRICKS_TOKEN")
JOB_ID = Variable.get("JOB_ID")



@dag(
        dag_id = 'orchestrate',
        schedule = '0 11 * * *',
        start_date=pendulum.datetime(2026, 11, 17, tz="Asia/Kolkata"),
        catchup = False
) 
def orchestrate():
    @task
    def ingest_cdc():
        ws = WorkspaceClient(
            host= DATABRICKS_HOST,
            token=DATABRICKS_TOKEN
        )

        job_trigger = ws.jobs.run_now(job_id=JOB_ID)

        while True:

            job_run = ws.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break 
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
                    
            time.sleep(5)  # Wait for 5 seconds before checking the job status again
        
        return "CDC Ingestion Completed"

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target"
    
    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id = 'silver_technical',
        bash_command ='cd /opt/airflow/walmart_project && dbt run --select silver_t'
    )

    silver_technical_tests = BashOperator(
        task_id = 'silver_technical_tests',
        bash_command ='cd /opt/airflow/walmart_project && dbt test --select silver_t'
    )

    gold_ephemeral = BashOperator(
        task_id = 'gold_ephemeral',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select gold/ephemeral'
    )

    
    gold_dimensions = BashOperator(
        task_id = 'gold_dimensions',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt snapshot'
    )

    gold_fact = BashOperator(
        task_id = 'gold_facts',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select gold/fact'
    )

    silver_business = BashOperator(
        task_id = 'silver_business',
        bash_command ='cd /opt/airflow/walmart_project && dbt run --select silver_b'
    )

    silver_business_tests = BashOperator(
        task_id = 'silver_business_tests',
        bash_command ='cd /opt/airflow/walmart_project && dbt test --select silver_b'
    )

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_fact

orchestrate_dag = orchestrate()