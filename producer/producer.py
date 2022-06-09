#!/usr/bin/env python

#from TwitterAPI import TwitterAPI
import tweepy
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import time
import random
# import os

CONSUMER_KEY = "xx"
CONSUMER_SECRET = "xx"
ACCESS_TOKEN_KEY = "xx"
ACCESS_TOKEN_SECRET = "xx"


if __name__ == "__main__":
    
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    print(args)
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    # ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print(
                "Produced record to topic {} partition [{}] @ offset {}".format(
                    msg.topic(), msg.partition(), msg.offset()
                )
            )

 
    auth = tweepy.AppAuthHandler(consumer_key = CONSUMER_KEY, consumer_secret=CONSUMER_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)



    for tweet in tweepy.Cursor(api.search_tweets, q="Starbucks", lang="en").items():
        string_encode = tweet._json["text"].encode("ascii", "ignore")
        string_decode = string_encode.decode()
        r = {
            "id" :tweet._json["id"], 
            "text": string_decode
            }
        
        
        record_key = str(r["id"])
        record_value = json.dumps(r['text'])
        print("Producing record: {}\t{}".format(record_key, record_value))
            # partition = random.randint(1, 1)
        producer.produce(
                topic=topic,
                key=record_key, 
                value=record_value
                #on_delivery=acked,
        )


    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
