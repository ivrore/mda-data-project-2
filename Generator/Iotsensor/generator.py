
#Import libraries
import json
import time
import random
import logging
import argparse
import time
from datetime import datetime, timedelta
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

# Simulates a temperature with 3% possibilities to be anormal

def temperaturaRandom():
    probability = random.random()
    if probability <= 0.03:
        return random.uniform(0,1) or random.uniform(5,6)
    else:
        return random.uniform(2,4)

# Simulates a location by a set of coordinates with 3 seconds difference from each 

def route_time(start_time):
    
    with open ("./gps/route.json") as file:
        coordinates = json.load(file)
    # New list adding trip timestamp
    coordinates_list = []

    for item in coordinates:
        # Set the starting time as actual time for the first coordinate
        if coordinates_list == []:
            dict = {"coordinate":item,"timestamp":str(start_time)}
            coordinates_list.append(dict)
        else:
        # Add +3 seconds for each coordinate as simulated location movement
            up_time = timedelta(seconds=3)
            start_time += up_time
            dict = {"coordinate":item,"timestamp":str(start_time)}
            coordinates_list.append(dict)
    # Save list in a JSON with trip timestamp
    jsonString = json.dumps(coordinates_list)
    jsonFile = open("./gps/location.json", "w")
    jsonFile.write(jsonString)
    jsonFile.close()
    return coordinates_list

# Function to simulate current location based on location timestamp

def current_location(message,coordinates_list):
   
    coordinates_list = route_time(start_time)
    current_timestamp = str(datetime.now())
    # Iterates over coordinates list looking for the coordinate according to the next timestamp
    for element in coordinates_list:
        
        if str(element["timestamp"]) > current_timestamp: #message['Measurement_time']:
            message['Location'] = element["coordinate"]
            return message
    
# Generate diferent products from JSON

def product():

    product_id = prod[random.randint(0,4)]['Product_id']
    rfid_id = prod[int(product_id)-1]['Rfid_id']
    product_name = prod[int(product_id)-1]['Product_name']
    measurement_time = str(datetime.now())
    temp_now = round(temperaturaRandom(),2)
    trip_time = str(start_time)
    location = ""
    
    # Return values in a dict
    return {
        "Rfid_id" : rfid_id,
        "Product_id": product_id,
        "Name": product_name,
        "Measurement_time": measurement_time,
        "Temp_now": temp_now,
        "Trip_time": trip_time,
        "Location" : location
        }

# Generate rfid data

def run_generator(project_id, topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)
    coordinates_list = route_time(start_time)
    # Publish message to topic every 5 seconds
    try:
        while True:
            message: dict = product()
            current_location(message,coordinates_list)
            pubsub_class.publishMessages(message)
            print (message)
            #it will be generated a transaction each 2 seconds
            time.sleep(5)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    start_time = datetime.now()
    route_time(start_time)
    run_generator(args.project_id, args.topic_name)

