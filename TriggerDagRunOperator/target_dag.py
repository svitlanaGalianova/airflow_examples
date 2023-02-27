from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import logging

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'Svitlana Galianova',
    'email_on_failure': False,
    'retries': 0
}

SCHEDULE = None


@dag(dag_id='target_dag',
     default_args=DEFAULT_ARGS,
     schedule_interval=None,
     start_date=days_ago(7),
     catchup=False)
def target():
    '''This dag will be triggered by controller_dag'''
    logging.basicConfig(level=logging.INFO)

    @task
    def log_message(dag_run=None):
        try:
            logging.info(f'''These are the arguments passed by the controller_dag:
                text={dag_run.conf.get("text")},
                number={dag_run.conf.get("number")},
                time_now={dag_run.conf.get("time_now")}''')
        except Exception as ex:
            raise ex

    log_message()


dag = target()

if __name__ == '__main__':
    from airflow.utils.state import State
    dag.clear(dag_run_state=State.NONE)
    dag.run()
