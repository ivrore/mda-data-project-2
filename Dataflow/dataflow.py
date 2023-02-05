#Import beam libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import MeanCombineFn
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.transforms.core import CombineGlobally
import apache_beam.transforms.window as window

from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp import bigquery_tools

#Import Common Libraries
from datetime import datetime
import argparse
import json
import logging
import requests
    



# Decode pub/sub message from topic

def ParsePubSubMessage(message):
    #Decode PubSub message in order to deal with
    pubsubmessage = message.data.decode('utf-8')
    #Convert string decoded in json format(element by element)
    row = json.loads(pubsubmessage)
    #Logging
    logging.info("Receiving message from PubSub:%s", pubsubmessage)
    #Return function
    return row

class AddTimestampDoFn(beam.DoFn):
    
    def process(self, element):
        #Add ProcessingTime field
        element['processing_time'] = str(datetime.now())
        #return function
        yield element

# Function to check if product temperature is optimal

class CheckTemperatureStatusDoFn(beam.DoFn):
    """ Call provider API to extract data from its own DB """
    #Initialize the class by setting the host and endpoint to call
    def __init__(self, hostname):
        self.hostname = hostname
        self.endpoint = '/ngg9I9/p_db'
    #Add process function
    def process(self, element):
        #Dealing with sensitive fields
        '''if element['Product'] != None:
            logging.info("Masking an Email field...")
            #Make API Request'''
        try:
            api_request = requests.get(self.hostname + self.endpoint)
            #Show the status response in the logs
            logging.info("Request was finished with the following status: %s", api_request.status_code)
            r = api_request.json()
            p_id = int(element['Product_id'])-1
            p_max = r[p_id]['max_temp']
            p_min = r[p_id]['min_temp']
            if element['Temp_now'] not in range(p_min,p_max):
                element['status'] = "Warning"
                print (element['status'])
                logging.info(element)
                yield element
            else:
                element['status'] = 'Ok'
                logging.info(element)
                yield element
       #Error handle
        except Exception as err:
                logging.error("Error while trying to call to the API: %s", err)

# DoFN: Filter
             
class FilterFn(beam.DoFn):
    def process(self, element):
        if (element['status'] == 'warning'):
            logging.info("Warning: Rfid %s temperature is out of range. Value: %s degrees at %s",element['Rfid_id'],element['Temp_now'],datetime.now())
            yield element

# DoFn 05 : Output data formatting
class OutputFormatDoFn(beam.DoFn):
    """ Set a specific format for the output data."""
    #Add process function
    def process(self, element):
        #Send a notification with the best-selling product_id in each window
        output_msg = {"ProcessingTime": str(datetime.now()), "message": f"was the best-selling product."}
        #Convert the json to the proper pubsub format
        output_json = json.dumps(output_msg)
        yield output_json.encode('utf-8')

""" Dataflow Process """
def run():

    """ Input Arguments"""
    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                    '--project_id',
                    required=True,
                    help='GCP cloud project name')
    parser.add_argument(
                    '--hostname',
                    required=False,
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
                    '--alert_output_topic',
                    required=True,
                    help='PubSub Topic which will have only alert notification data.')
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
        
        """ Part 01: Format data by masking the sensitive fields and checking if the transaction is fraudulent."""
        data = (
            p 
                | "Read From PubSub" >> beam.io.ReadFromPubSub(topic=args.input_subscription)
                # Parse JSON messages with Map Function
                | "Parse JSON messages" >> beam.Map(ParsePubSubMessage)
                # Adding Processing timestamp
                | "Add Processing Time" >> beam.ParDo(AddTimestampDoFn())
        )
        
        """ Part 02: Writing data to BigQuery"""
        (
            data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = f"{args.project_id}:{args.output_bigquery}",
                schema = schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        
        """ Part 03: Send information to topics"""

        (
            data 
                # Add temperature status
                | "Check temperature" >> beam.ParDo(CheckTemperatureStatusDoFn())
                # Define output format
                | "OutputFormat" >> beam.ParDo(OutputFormatDoFn())
                # Write notification to admin PubSub Topic
                | "Send Push Notification admin topic" >> beam.io.WriteToPubSub(topic=f"projects/{args.project_id}/topics/{args.output_topic}", with_attributes=False)
                # Filter to send only if temp alert
                | 'Filter messages' >> beam.ParDo(FilterFn())
                # Write notification to alert PubSub Topic
                | "Send Push Notification alert topic" >> beam.io.WriteToPubSub(topic=f"projects/{args.project_id}/topics/{args.alert_output_topic}", with_attributes=False)
        )

if __name__ == '__main__':
    #Add Logs
    logging.getLogger().setLevel(logging.INFO)
    #Run process
    run()