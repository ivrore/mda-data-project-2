import base64
import os
from google.cloud import pubsub_v1
import json

def writeToAlertTopic(event, context):
    
    # Decode message from Pubsub
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    msg = json.loads(pubsub_message)

    # Setting environment variables to load from cloud functions
    project_id = os.environ['PROJECT_ID']
    topic_name = os.environ['ALERT_TOPIC_OUTPUT']

    # Load publisher class for Pubsub
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    
    # Filter 'warning temperature' messages
    if msg['status'] == 'Warning':
      print(f"Warning: Rfid {msg['Rfid_id']} temperature is out of range. Value: {msg['Temp_now']} degrees.")
      # Encode JSON to send in Pubsub
      output_json = json.dumps(msg).encode('utf-8')
      # Send message to alert topic
      publisher.publish(topic_path,output_json)
      return
     
   


