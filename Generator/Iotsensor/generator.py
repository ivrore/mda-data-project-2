
#Import libraries
import json
import time
import random
import logging
import argparse
import google.auth
from datetime import datetime
from google.cloud import pubsub_v1

#Input arguments
parser = argparse.ArgumentParser(description=('Arguments for Dataflow pipeline.'))
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
        logging.info("New data has been registered for rfid: %s", message['Rfid_id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")

# Read products JSON file

with open("./products.json") as file:
        prod = json.load(file)

# Simulates a temperature with 2% possibilities to be anormal

def temperaturaRandom():
    probability = random.random()
    if probability <= 0.02:
        return random.uniform(0,1) or random.uniform(5,6)
    else:
        return random.uniform(2,4)

# Generate diferent products from JSON

def product():

    product_id = prod[random.randint(0,4)]['Product_id']
    rfid_id = prod[int(product_id)-1]['Rfid_id']
    product_name = prod[int(product_id)-1]['Product_name']
    measurement_time = str(datetime.now())
    temp_now = round(temperaturaRandom(),2)
    latitude = ""
    longitude = ""
    
    # Return values in a dict
    return {
        "Rfid_id" : rfid_id,
        "Product_id": product_id,
        "Name": product_name,
        "Measurement_time": measurement_time,
        "Temp_now": temp_now,
        "Latitude": latitude,
        "Longitude": longitude
        }

# Generate rfid data

def run_generator(project_id, topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)
    # Publish message to topic every 5 seconds
    try:
        while True:
            message: dict = product()
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

