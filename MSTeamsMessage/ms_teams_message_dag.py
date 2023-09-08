from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import logging
import pandas as pd
import requests


log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'Svitlana Galianova',
    'email_on_failure': False,
    'retries': 0
}

SCHEDULE = None


@dag(dag_id='ms_teams_message_dag',
     default_args=DEFAULT_ARGS,
     schedule_interval=None,
     start_date=days_ago(7),
     catchup=False)
def ms_teams_message():
    '''This dag will send a teams message
       Based on: https://medium.com/@d.s.m/apache-airflow-send-messages-to-microsoft-teams-bcb2521d1ca4'''
    logging.basicConfig(level=logging.INFO)

    @task
    def send_message():
        # how to get webhook url: https://medium.com/@d.s.m/apache-airflow-send-messages-to-microsoft-teams-bcb2521d1ca4
        webhook_url = ''
        # format of the message: Title (activityTitle), Paragraph (activitySubtitle), table (second element in sections), 2 buttons with URLs (potentialAction)
        json = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "49baba",
            "summary": "Summary",
            "sections": [{
                "activityTitle": "Activity Title",
                "activitySubtitle": "Activity Subtitle",
                "markdown": True
            }, {
                "startGroup": True,
                "text": ""
            }],
            "potentialAction": [{
                "@type": "OpenUri",
                "name": "Google",
                "targets": [{
                    "os": "default",
                    "uri": "https://google.com/"
                }]
            }, {
                "@type": "OpenUri",
                "name": "GitHub",
                "targets": [{
                    "os": "default",
                    "uri": "https://github.com/"
                }]
            }]
        }
        # populate content of the table
        df = pd.DataFrame({'item_id': [1, 2, 3],
                           'category_id': ['01', '02', '03'],
                           'time': '2023-01-31',
                           'location': ['Canada', 'Asia', 'US']})

        html = df.to_html(index=False)

        # apply basic CSS
        styles = {
            '<th>': '<th style="background-color: rgba(73, 186, 186, 0.6);padding: 3px;">',
            '<td>': '<td style="padding: 3px;">'
        }
        for key, value in styles.items():
            html = html.replace(key, value)
        log.info(html)

        json['sections'][1]['text'] = html

        # send the actual message
        headers = {'content-type': 'application/json'}
        requests.post(webhook_url, json=json, headers=headers)

    send_message()


dag = ms_teams_message()

if __name__ == '__main__':
    from airflow.utils.state import State
    dag.clear(dag_run_state=State.NONE)
    dag.run()
