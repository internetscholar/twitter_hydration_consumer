import tweepy
import boto3
import json
import psycopg2
import configparser
import os
import logging
import requests
import traceback
from psycopg2 import extras


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def main():
    # todo create a new image on AWS for this module.
    if 'DISPLAY' not in os.environ:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['db_name'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)
    logging.info("Connected to database %s", config['database']['db_name'])

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=aws_credential['default_region']
    )
    sqs = aws_session.resource('sqs')
    logging.info("Connected to AWS in %s", aws_credential['default_region'])

    received_message = None
    twitter_credentials = None
    tweets_queue = sqs.get_queue_by_name(QueueName='twitter_hydration')
    twitter_credentials_queue = sqs.get_queue_by_name(QueueName='twitter_credentials')
    ip = None
    incomplete_transaction = False

    try:
        ip = requests.get('http://checkip.amazonaws.com').text.rstrip()

        message = twitter_credentials_queue.receive_messages()
        twitter_credentials = json.loads(message[0].body)
        message[0].delete()
        logging.info("Obtained Twitter credential for account %s - app %s",
                     twitter_credentials['account'],
                     twitter_credentials['app_name'])

        auth = tweepy.OAuthHandler(twitter_credentials['consumer_key'], twitter_credentials['consumer_secret'])
        auth.set_access_token(twitter_credentials['access_token'], twitter_credentials['access_token_secret'])
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        logging.info('Authenticated on Twitter')

        tweets_queue_empty = False
        logging.info('Connected to queue twitter_hydration')
        while not tweets_queue_empty:
            message = tweets_queue.receive_messages()
            if len(message) == 0:
                tweets_queue_empty = True
                logging.info('No more messages in queue twitter_hydration')
            else:
                received_message = json.loads(message[0].body)
                message[0].delete()
                incomplete_transaction = True
                logging.info('Received message from project %s with %d tweets',
                             received_message['project_name'],
                             len(received_message['tweet_ids']))
                twitter_chunks = chunks(received_message['tweet_ids'], 100)
                for twitter_chunk in twitter_chunks:
                    tweets = api.statuses_lookup(twitter_chunk, include_entities=True, trim_user=False)
                    logging.info('Requested tweets from Twitter')
                    cur.execute("insert into twitter_hydration_request (ip) "
                                "values (%s) returning request_id", (ip,))
                    request_id = cur.fetchone()['request_id']
                    if len(tweets) > 0:
                        # noinspection PyProtectedMember
                        data_text = ','.join(cur.mogrify("(%s,%s,%s,%s)",
                                                         (received_message['project_name'],
                                                          tweet.id,
                                                          json.dumps(tweet._json).
                                                          replace("\\u0000", " ").replace("\u0000", " "),
                                                          request_id,)).decode('utf-8') for tweet in tweets)
                        cur.execute('insert into twitter_hydrated_tweet '
                                    '(project_name, tweet_id, response, request_id) values {} '
                                    'on conflict do nothing'.format(data_text))
                        conn.commit()
                        logging.info('Saved tweets to database')
                    else:
                        logging.info('No more tweets')
                incomplete_transaction = False
    except Exception:
        conn.rollback()
        # add record indicating error.
        cur.execute("insert into error (current_record, error, module, ip) VALUES (%s, %s, %s, %s)",
                    (json.dumps({'twitter_credentials': twitter_credentials,
                                 'received_message': received_message}),
                     traceback.format_exc(),
                     'twitter_hydration_consumer',
                     ip), )
        logging.info('Saved record with error information')
        conn.commit()
        if incomplete_transaction:
            tweets_queue.send_message(MessageBody=json.dumps(received_message))
            logging.info('Enqueued tweet ids')
        if twitter_credentials is not None:
            twitter_credentials_queue.send_message(MessageBody=json.dumps(twitter_credentials))
            logging.info('Enqueued Twitter credentials')
        raise
    finally:
        conn.close()
        logging.info('Disconnected from database')


if __name__ == '__main__':
    main()
