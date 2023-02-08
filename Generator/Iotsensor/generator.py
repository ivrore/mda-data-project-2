#Import libraries
import json
import time
import random
import logging
import argparse
import requests
import google.auth
<<<<<<<< HEAD:Generator/Client/generator.py
from google.cloud import pubsub_v1

credentials, project = google.auth.default()

rand = random.random()

========
from datetime import datetime
from google.cloud import pubsub_v1

>>>>>>>> main:Generator/Iotsensor/generator.py
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
<<<<<<<< HEAD:Generator/Client/generator.py
        logging.info("Nueva temperatura registrada. Temperatura: %s", message['Temperatura'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Iot closed.")


#Código a generar           
def temperaturaRandom():
    probabilidad= random.random()
    if probabilidad <= 0.05:
========
        logging.info("New data has been registered for rfid: %s", message['Rfid_id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")

rfid = ['pF8z9GBG', 'XsEOhUOT', '89x5FhyA', 'S3yG1alL', '5pz386iG']
products = ['cerdo','conejo','ternera','cordero','pollo']

# Simulates a temperature with 3% possibilities to be anormal

def temperaturaRandom():
    probability = random.random()
    if probability <= 0.03:
>>>>>>>> main:Generator/Iotsensor/generator.py
        return random.uniform(0,1) or random.uniform(5,6)
    else:
        return random.uniform(2,4)

<<<<<<<< HEAD:Generator/Client/generator.py

def product1():
    rfid_id = "5Fh8U"
    product_name = "cerdo"
    id_producto = "1"
    Temperatura_min = int(1)
    Temperatura_max = int(3)
    Temperatura = round(temperaturaRandom(),2)
            
========
# Generate rfid data

def product():

    rfid_id = random.choice(rfid)
    product_id = int(rfid.index(rfid_id)+1)
    product_name = products[product_id-1]
    measurement_time = str(datetime.now())
    temp_now = round(temperaturaRandom(),2)
    
    # Return values in a dict
>>>>>>>> main:Generator/Iotsensor/generator.py
    return {
        "Rfid_id" : rfid_id,
        "Product_id": product_id,
        "Name": product_name,
<<<<<<<< HEAD:Generator/Client/generator.py
        "Rfid_id": rfid_id,
        "Temperatura_minima": Temperatura_min,
        "Temperatura_maxima": Temperatura_max,
        "Temperatura": Temperatura
    }
========
        "Measurement_time": measurement_time,
        "Temp_now": temp_now
        }
>>>>>>>> main:Generator/Iotsensor/generator.py

 
def run_generator(project_id,topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)
    # Publish message to topic every 5 seconds
    try:
        while True:
<<<<<<<< HEAD:Generator/Client/generator.py
            message: dict = product1()
========
            message: dict = product()
>>>>>>>> main:Generator/Iotsensor/generator.py
            pubsub_class.publishMessages(message)
            #it will be generated a transaction each 2 seconds
            time.sleep(5)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    #run_generator(args.project_id, args.topic_name)
    run_generator(args.project_id, args.topic_name)

<<<<<<<< HEAD:Generator/Client/generator.py

========
>>>>>>>> main:Generator/Iotsensor/generator.py
