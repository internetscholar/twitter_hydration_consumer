import tweepy
import boto3
import json
import psycopg2
import configparser
import os


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


if __name__ == '__main__':
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['dbname'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor()

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential[0],
        aws_secret_access_key=aws_credential[1],
        region_name=aws_credential[2]
    )
    sqs = aws_session.resource('sqs')
    twitter_credentials_queue = sqs.get_queue_by_name(QueueName='twitter-credentials')
    message = twitter_credentials_queue.receive_messages()
    twitter_credentials = json.loads(message[0].body)
    message[0].delete()

    auth = tweepy.OAuthHandler(twitter_credentials['consumer_key'], twitter_credentials['consumer_secret'])
    auth.set_access_token(twitter_credentials['access_token'], twitter_credentials['access_token_secret'])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    tweets_queue_empty = False
    tweets_queue = sqs.get_queue_by_name(QueueName='tweet-ids')
    while not tweets_queue_empty:
        message = tweets_queue.receive_messages()
        if len(message) == 0:
            tweets_queue_empty = True
        else:
            received_message = json.loads(message[0].body)
            message[0].delete()
            twitter_chunks = chunks(received_message['tweet_ids'], 100)
            for twitter_chunk in twitter_chunks:
                tweets = api.statuses_lookup(twitter_chunk, include_entities=True, trim_user=False)
                if len(tweets) > 0:
                    dataText = ','.join(cur.mogrify('(%s,%s,%s)',
                                                    (received_message['query_alias'],
                                                     tweet.id,
                                                     json.dumps(tweet._json))).decode('utf-8') for tweet in tweets)
                    cur.execute('insert into tweet (query_alias, tweet_id, response) values ' + dataText)
                    conn.commit()

    conn.close()