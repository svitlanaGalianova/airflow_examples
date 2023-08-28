from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

import logging

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'Svitlana Galianova',
    'email_on_failure': False,
    'retries': 0
}

SCHEDULE = None


@dag(dag_id='dynamic_task_dag',
     default_args=DEFAULT_ARGS,
     schedule_interval=None,
     start_date=days_ago(7),
     catchup=False)
def dynamic_tasks():
    '''This dag will be triggered by controller_dag'''
    logging.basicConfig(level=logging.INFO)

    @task
    def combine_data(results):
        log.info(','.join(results))

    with TaskGroup('dynamic_tasks_group', tooltip='dynamic_tasks_group') as dynamic_tasks_group:
        items = ['One', 'Two', 'Three']

        results = []

        for i in items:
            @task(task_id=f'log_message_{i}')
            def log_message(item):
                try:
                    logging.info(f'Hello {item}')
                    return f'Hello {item}'
                except Exception as ex:
                    raise ex

            results.append(log_message(i))

    dynamic_tasks_group >> combine_data(results)


dag = dynamic_tasks()

if __name__ == '__main__':
    from airflow.utils.state import State
    dag.clear(dag_run_state=State.NONE)
    dag.run()
