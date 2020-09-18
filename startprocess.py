import xlrd
import os
from bigqueryutil import create_dataset
from config_util import readconfig
from pubsubutil import checktopicavailable, checksubscriptionavailable, publishmessage
import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window

# Defines the BigQuery schema for the output table.
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'pubsubtest.json'
from storage_util import check_bucket_available

SCHEMA = ','.join([
    'url:STRING',
    'num_reviews:INTEGER',
    'score:FLOAT64',
    'first_date:TIMESTAMP',
    'last_date:TIMESTAMP',
    ])


def parse_json_message(message):
    import time
    """Parse the input json message and add 'score' & 'processing_time' keys."""
    row = json.loads(message)
    return {
        'url': row['url'],
        'score': 1.0 if row['review'] == 'positive' else 0.0,
        'processing_time': int(time.time()),
        }


def get_statistics(url_messages):
    """Get statistics from the input URL messages."""
    url, messages = url_messages
    return {
        'url': url,
        'num_reviews': len(messages),
        'score': sum(msg['score'] for msg in messages) / len(messages),
        'first_date': min(msg['processing_time'] for msg in messages),
        'last_date': max(msg['processing_time'] for msg in messages),
        }


def run(args, project_id, job_id, region, input_subscription, output_table, temp_folder, window_interval):
    """to deploy in GCP dataflow"""

    # Create and set your PipelineOptions.
    # For Cloud execution, specify DataflowRunner and set the Cloud Platform
    # project, job name, temporary files location, and region.
    # For more information about regions, check:
    # https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
    options = PipelineOptions(
        flags=args,
        runner='DataflowRunner',
        project=project_id,
        job_name=job_id,
        temp_location=temp_folder,
        region=region,
        save_main_session=True,
        streaming=True)

    # Create the Pipeline with the specified options.
    # with beam.Pipeline(options=options) as pipeline:
    #   pass  # build your pipeline here.

    """Build and run the pipeline."""
    # options = PipelineOptions(args, save_main_session=True, streaming=True)
    pipeline = beam.Pipeline(options=options)
    # with beam.Pipeline(options=options) as pipeline:
    # Read the messages from PubSub and process them.
    messages = (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription=
                                                            input_subscription).with_output_types(bytes)
            | 'UTF-8 bytes to string' >> beam.Map(lambda msg: msg.decode('utf-8'))
            | 'Parse JSON messages' >> beam.Map(parse_json_message)
            | 'Fixed-size windows' >> beam.WindowInto(window.FixedWindows(int(window_interval), 0))
            | 'Add URL keys' >> beam.Map(lambda msg: (msg['url'], msg))
            | 'Group by URLs' >> beam.GroupByKey()
            | 'Get statistics' >> beam.Map(get_statistics))

    # Output the results into BigQuery table.
    output = messages | 'Write to Big Query' >> beam.io.WriteToBigQuery(
        output_table, schema=SCHEMA)

    result = pipeline.run()


def startdataflow(l):
    print('Dataflow starts .... :')
    # parser = argparse.ArgumentParser()
    # pipeline_args,known_args = parser.parse_known_args(l)
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project_id',
        help='Project_ID '
             'eg: peaceful-branch-279707')
    parser.add_argument(
        '--job_id',
        help='Unique Job ID')
    parser.add_argument(
        '--output_table',
        help='Output BigQuery table for results specified as: '
             'PROJECT:DATASET.TABLE or DATASET.TABLE.')
    parser.add_argument(
        '--input_subscription',
        help='Input PubSub subscription of the form '
             '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."')
    parser.add_argument(
        '--temp_folder',
        help='Temporary folder for data processing '
             '"gs://<BUCKET_ID>/<TEMP_FOLDER_ID>."')
    parser.add_argument(
        '--region',
        default='us-central1',
        help='region.')
    parser.add_argument(
        '--window_interval',
        default=60,
        help='Window interval in seconds for grouping incoming messages.')
    # known_args, pipeline_args = parser.parse_known_args()
    known_args, pipeline_args = parser.parse_known_args(l)
    print(known_args, '\n', pipeline_args)
    run(pipeline_args, known_args.project_id, known_args.job_id, known_args.region, known_args.input_subscription,
        known_args.output_table,
        known_args.temp_folder,
        known_args.window_interval)
    exit()


#
# def readconfig(sheetname):
#     newdict = dict()
#     loc = "PubSub_Bigquery_input.xlsx"
#     wb = xlrd.open_workbook(loc)
#     sheet = wb.sheet_by_name(sheetname)
#     for i in range(sheet.nrows):
#         newdict[sheet.cell_value(i, 0)] = sheet.cell_value(i, 1)
#     print(newdict)
#     return newdict
#

def readmessage(messagepath):
    with open(messagepath, 'r+') as fobj:
        message = fobj.read()

    print(message)
    return message


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--action',
        help='pubsub' ' : Create Pub/SUb '
             'publish' ' : Publish message '
             'dataflow' ' :create job with pipeline '
             'bigquery' ' :create bigquery table '
             'storage'  ' :create storage bucket for temp folder'
        )

    known_args, pipeline_args = parser.parse_known_args()
    print(known_args, '\n', pipeline_args)

    if known_args.action == 'pubsub':
        config = readconfig('PubSub')
        checktopicavailable(config)
        checksubscriptionavailable(config)
    elif known_args.action == 'publish':
        config = readconfig('Publish')
        message = readmessage(config['MessagePath'])
        publishmessage(config,message)

    elif known_args.action == 'bigquery':
        config = readconfig('BigQuery')
        create_dataset(config)

    elif known_args.action == 'storage':
        config = readconfig('StorageBucket')
        check_bucket_available(config)

    elif known_args.action == 'dataflow':
        # from dfPipe import startdataflow
        config = readconfig('Dataflow')
        subscription = 'projects/' + config['ProjectId'] + '/subscriptions/' + config['SubscritonName']
        temp_folder = 'gs://' + config['Bucket_name'] + '/' + config['temp_folder']
        out_bigquery_table = config['ProjectId'] + ':' + config['Dataset_Name'] + '.' + config['Table_Name']

        l = ['--project_id', config['ProjectId'],
             '--job_id', config['Dataflow_Job_id'],
             '--region', config['region'],
             '--output_table', out_bigquery_table,
             '--input_subscription', subscription,
             '--temp_folder', temp_folder,
             '--window_interval', str(int(config['window_interval']))
             ]
        print(l)

        checktopicavailable(config)
        checksubscriptionavailable(config)
        check_bucket_available(config)
        #create_dataset(config)

        startdataflow(l)
