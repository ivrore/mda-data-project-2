
#Import libraries
import json
import time
import uuid
import random
import logging
import argparse
import google.auth
from datetime import datetime
from google.cloud import pubsub_v1
'''
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
        self.publisher = pubsub_v1.PublisherIot()
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        publish_future = self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("Nueva temperatura registrada. Id: %s", message['Temperatura'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Iot closed.")
'''

#Código a generar
#Creamos una clase para representar la etiqueta IoT



def product1():
    rfid_id = random.choice(['pF8z9GBG', 'XsEOhUOT', '89x5FhyA', 'S3yG1alL', '5pz386iG'])
    product_name = "Pollo"
    Temperatura_min = float(1)
    Temperatura_max = float(5)
    Temperatura_actual = receive_data_from_iot
    
    return {
        "Name": product_name,
        "Rfid_id": rfid_id,
        "Temperatura mínima": Temperatura_min,
        "Temperatura máxima": Temperatura_max,
        "Temperatura actual": Temperatura_actual,
    }

class IoTDevice:
    def __init__(product1):

        #Guardamos la temperatura actual del producto
        product1.temperature = 0
    #Creamos una nueva función que simule la actualización de la temperatura
    def update_temperature(self):
        self.temperature = random.uniform(1, 5)
#Creamos una nueva función que simule la generación y variación de la temperatura según la información que registra el dispositivo IoT
def receive_data_from_iot(iot_device):
    while True:
        iot_device.update_temperature()
        print("Temperature: {}".format(iot_device.temperature))
        time.sleep(5)

#Muestra la temperatura actualizada
iot = IoTDevice()
receive_data_from_iot(iot)

