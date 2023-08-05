from dotenv import load_dotenv
load_dotenv()

import requests
import os
import threading
import time
import vanna as vn

from flask_cors import CORS
from flask import Flask, request

vn.set_api_key(os.environ['VANNA_API_KEY'])
vn.set_model(os.environ['VANNA_ORG'])

app = Flask(__name__)
CORS(app)

def post_message(sink, text):
  body = {
    'channel': sink,
    'text': text,
  }
  
  try:
    response = requests.post(
      'https://slack.com/api/chat.postMessage',
      json=body,
      headers={
        'Authorization': 'Bearer {}'.format(os.environ['BOT_USER_OAUTH_TOKEN'])
      }
    )

    if not response or response.status_code != 200 or not response.json().get('ok'):
      raise Exception('Failed to post chat, request body: {}, response status: {}, response data: {}'.format(
        body, response.status_code, response.json()
      ))

    return response.json()
  except Exception as e:
    app.logger.error('Error posting message to {}'.format(sink))
    app.logger.error(str(e))

  return None

def reply_message(sink, text, ts, broadcast):
  body = {
    'channel': sink,
    'text': text,
    'thread_ts': ts,
    'reply_broadcast': broadcast
  }
  
  try:
    response = requests.post(
      'https://slack.com/api/chat.postMessage',
      json=body,
      headers={
        'Authorization': 'Bearer {}'.format(os.environ['BOT_USER_OAUTH_TOKEN'])
      }
    )

    if not response or response.status_code != 200 or not response.json().get('ok'):
      raise Exception('Failed to post chat, request body: {}, response status: {}, response data: {}'.format(
        body, response.status_code, response.json()
      ))

    return response.json()
  except Exception as e:
    app.logger.error('Error posting message to {}'.format(sink))
    app.logger.error(str(e))

  return None

def upload_file(sink, file_content, filename, title, initial_comment, ts):
    body = {
        'channels': sink,
        'file': file_content,
        'filename': filename,
        'title': title,
        'initial_comment': initial_comment
        # 'thread_ts': ts,
        # 'reply_broadcast': True
    }

    try:
        response = requests.post(
            'https://slack.com/api/files.upload',
            data=body,
            headers={
                'Authorization': 'Bearer {}'.format(os.environ['BOT_USER_OAUTH_TOKEN'])
            },
            files={'file': file_content}
        )

        if not response or response.status_code != 200 or not response.json().get('ok'):
            raise Exception('Failed to upload file, request body: {}, response status: {}, response data: {}'.format(
                body, response.status_code, response.json()
            ))

        return response.json()
    except Exception as e:
        app.logger.error('Error uploading file to {}'.format(sink))
        app.logger.error(str(e))

    return None


def reply_message_with_delay(delay, sink, text, ts, broadcast):
  time.sleep(delay)
  reply_message(sink, text, ts, broadcast)

from google.cloud.sql.connector import Connector
connector = Connector()

def getconn():
    conn = connector.connect(
          os.environ['POSTGRES_INSTANCE_CONNECTION_STRING'],
          "pg8000",
          user=os.environ['POSTGRES_USER'],
          password=os.environ['POSTGRES_PASSWORD'],
          db=os.environ['POSTGRES_DB'],
      )

    return conn

import pandas as pd
vn.run_sql = lambda sql: pd.read_sql_query(sql, getconn())

def sql_reply(question, sink, ts):
  sql = vn.generate_sql(question)

  slack_sql = "```\n" + sql + "\n```"

  reply_message(sink, slack_sql, ts, broadcast=False)

  df = vn.run_sql(sql)

  slack_table = "```\n" + df.head(10).to_markdown(index=False) + "\n...```"

  reply_message(sink, slack_table, ts, broadcast=False)

  plotly_code = vn.generate_plotly_code(question=question, sql=sql, df=df)
  fig = vn.get_plotly_figure(plotly_code=plotly_code, df=df)

  img = fig.to_image(format="png", width=800, height=600, scale=2)

  upload_file(sink, img, "plot.png", "Plot", question, ts)


@app.route('/')
def index():
  return 'Vanna.AI Slack backend is up'


@app.route('/event', methods=['POST'])
def handle_events():
  data = request.get_json()
  # Verify the event route.
  if data['type'] == 'url_verification':
    return data['challenge']
  
  # Fallback.
  return ''


@app.route('/slash', methods=['POST'])
def handle_slash():
  data = request.form

  # Post the command + text that was entered by the user.
  # post_message_resp = post_message(data['channel_id'], '{} {}'.format(data['command'], data['text']))
  post_message_resp = post_message(data['channel_id'], 'I was asked "{}"'.format(data['text']))
  
  # Post the first reply.
  # reply_message(data['channel_id'], sql, post_message_resp['ts'])
  x = threading.Thread(target=sql_reply, args=(data['text'], data['channel_id'], post_message_resp['ts']))
  x.start()

  # Post the second reply after a delay of 5s.
  # x = threading.Thread(target=reply_message_with_delay, args=(5, data['channel_id'], 'My Message 2', post_message_resp['ts']))
  # x.start()

  return ('', 200)

# main driver function
if __name__ == "__main__":
    app.run(port=8081)