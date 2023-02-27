from time import sleep
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import datetime
import logging

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'Svitlana Galianova',
    'email_on_failure': False,
    'retries': 0
}


@dag(dag_id='controller_dag',
     default_args=DEFAULT_ARGS,
     schedule_interval=None,
     start_date=days_ago(7),
     catchup=False)
def controller():
    '''
    Example usage of the TriggerDagRunOperator. This example holds 2 DAGs:
    1. 1st DAG (controller_dag) holds a TriggerDagRunOperator, which will trigger the 2nd DAG
    2. 2nd DAG (target_dag) which will be triggered by the TriggerDagRunOperator in the 1st DAG

    Based on: https://github.com/apache/airflow/blob/main/airflow/example_dags/example_trigger_target_dag.py
    '''
    logging.basicConfig(level=logging.INFO)

    @task
    def log_message():
        try:
            logging.info('This dag will trigger target_dag')
            # the next dag is not triggered until this dag is completed successfully
            # uncomment the next line for testing
            # raise ValueError('Testing on fail')
        except Exception as ex:
            raise ex

    trigger_target_dag = TriggerDagRunOperator(
        task_id='trigger_target_dag',
        trigger_dag_id='target_dag',
        conf={'text': 'Hello world',
              'number': 1234,
              'time_now': datetime.datetime.now().isoformat(sep=' ', timespec='milliseconds')},
    )

    log_message() >> trigger_target_dag


dag = controller()

if __name__ == '__main__':
    from airflow.utils.state import State
    dag.clear(dag_run_state=State.NONE)
    dag.run()
