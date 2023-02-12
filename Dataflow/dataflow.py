#Import beam libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp import bigquery_tools

#Import Common Libraries
from datetime import datetime
import argparse
import json
import logging
import requests
import numpy as np
    
# Decode PubSub message from topic 


def ParsePubSubMessage(message):
    #Decode PubSub message
    pubsubmessage = message.data.decode('utf-8')
    #Convert string message to JSON format
    row = json.loads(pubsubmessage)
    #Logging
    logging.info("Receiving message from PubSub:%s", pubsubmessage)
    #Return function
    return row

class AddTimestampDoFn(beam.DoFn):
    
    def process(self, element):
        #Add processing time to message
        element['processing_time'] = str(datetime.now())
        #return function
        yield element

# Dofn beam function to check if product temperature is optimal

class CheckTemperatureStatusDoFn(beam.DoFn):
    ''' Simulate a call to supplier product database to get temperature values'''
    #Initialize the class by setting the host and endpoint to call
    def __init__(self, hostname):
        self.hostname = hostname
        self.endpoint = '/wGBM56/p_db'
    #Add process function
    def process(self, element):

        try:
            api_request = requests.get(self.hostname + self.endpoint)
            #Show the status response in the logs
            logging.info("Request was finished with the following status: %s", api_request.status_code)
            r = api_request.json()
            # Set index of product_id in a varible for look into API
            p_id = int(element['Product_id'])-1
            # Store in two variables max/min temp from supplier database for each product
            p_max = r[p_id]['max_temp']
            p_min = r[p_id]['min_temp']
            # Check if temperature is between max/min temp indicated in supplier database
            if float(p_min) <= element['Temp_now'] <= float(p_max) : 
                # If temperature not in range add 'warning'status
                element['status'] = "Warning"
                logging.info("Warning: Rfid %s temperature is out of range [%s-%s]. Value: %s degrees at %s",element['Rfid_id'],p_min,p_max,element['Temp_now'],datetime.now())
                yield element
            else:
                element['status'] = 'Ok'
                logging.info(element)
                yield element
       #Error handle
        except Exception as err:
                logging.error("Error while trying to call to the API: %s", err)

# Dofn beam function to format data for output

class OutputFormatDoFn(beam.DoFn):
    """ Set a specific format for the output data."""
    #Add process function
    def process(self, element):
        #Convert the json to the proper pubsub format
        output_json = json.dumps(element)
        yield output_json.encode('utf-8')

""" Dataflow Process """
def run_dataflow():

    """ Input Arguments"""
    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                    '--project_id',
                    required=True,
                    help='GCP cloud project name')
    parser.add_argument(
                    '--hostname',
                    required=True,
                    help='API Hostname provided during the session.')
    parser.add_argument(
                    '--input_subscription',
                    required=True,
                    help='PubSub Subscription which will be the source of data.')
    parser.add_argument(
                    '--output_topic',
                    required=True,
                    help='PubSub Topic which will have all data.')
    parser.add_argument(
                    '--output_bigquery',
                    required=True,
                    help='Table where data will be stored in BigQuery. Format: <dataset>.<table>.')
    parser.add_argument(
                    '--bigquery_schema_path',
                    required=False,
                    default='./bq_schema/schema.json',
                    help='BigQuery Schema Path within the repository.')

                    
    args, pipeline_opts = parser.parse_known_args()

    """ BigQuery Table Schema """

    #Load schema from /schema folder
    with open(args.bigquery_schema_path) as file:
        input_schema = json.load(file)

    schema = bigquery_tools.parse_table_schema_from_json(json.dumps(input_schema))

    """ Apache Beam Pipeline """
    #Pipeline Options
    options = PipelineOptions(pipeline_opts, save_main_session=True, streaming=True, project=args.project_id)

    #Pipeline
    with beam.Pipeline(argv=pipeline_opts,options=options) as p:
        
        """ Part 01: Add processing time and add temperature status """
        data = (
            p 
                | "Read From PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/{args.project_id}/subscriptions/{args.input_subscription}", with_attributes=True)
                # Parse JSON messages with Map Function
                | "Parse JSON messages" >> beam.Map(ParsePubSubMessage)
                # Adding Processing timestamp
                | "Add Processing Time" >> beam.ParDo(AddTimestampDoFn())
                # Add temperature status
                | "Check temperature" >> beam.ParDo(CheckTemperatureStatusDoFn(args.hostname))
        )
        
        """ Part 02: Write before data to BigQuery """
        (
            data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = f"{args.project_id}:{args.output_bigquery}",
                schema = schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        
        """ Part 03: Set output and send data to admin topic"""

        (
            data 
                # Define output format
                | "OutputFormat" >> beam.ParDo(OutputFormatDoFn())
                # Write notification to admin PubSub Topic
                | "Send Push Notification admin topic" >> beam.io.WriteToPubSub(topic=f"projects/{args.project_id}/topics/{args.output_topic}", with_attributes=False)
                # Decode message again
        )       


if __name__ == '__main__':
    #Add Logs
    logging.getLogger().setLevel(logging.INFO)
    #Run process
    run_dataflow()