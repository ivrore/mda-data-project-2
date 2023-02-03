#Import libraries
import json
import time
import random
import logging
import argparse
import google.auth
from google.cloud import pubsub_v1

credentials, project = google.auth.default()

rand = random.random()

#Input arguments
parser = argparse.ArgumentParser(description=('Temperature Dataflow pipeline.'))
parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
parser.add_argument(
                '--topic_name',
                required=True,
                help='PubSub topic name.')

args, opts = parser.parse_known_args()

class PubSubMessages:
    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id, topic_name):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        publish_future = self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("Nueva temperatura registrada. Temperatura: %s", message['Temperatura'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Iot closed.")


#Código a generar           
def temperaturaRandom():
    probabilidad= random.random()
    if probabilidad <= 0.05:
        return random.uniform(0,1) or random.uniform(5,6)
    else:
        return random.uniform(2,4)


def product1():
    rfid_id = "5Fh8U"
    product_name = "Pollo"
    Temperatura_min = int(1)
    Temperatura_max = int(5)
    Temperatura = round(temperaturaRandom(),2)
            
    return {
        "Name": product_name,
        "Rfid_id": rfid_id,
        "Temperatura_minima": Temperatura_min,
        "Temperatura_maxima": Temperatura_max,
        "Temperatura": Temperatura
    }

 
def run_generator(project_id,topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)
    #Publish message into the queue every 5 seconds
    try:
        while True:
            message: dict = product1()
            pubsub_class.publishMessages(message)
            #it will be generated a transaction each 2 seconds
            time.sleep(5)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_generator(args.project_id, args.topic_name)


