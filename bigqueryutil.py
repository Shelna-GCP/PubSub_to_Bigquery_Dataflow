# pip install --upgrade google-cloud-bigquery
import google
import xlrd
from google.cloud import bigquery
from google.cloud.bigquery import ExternalConfig, TimePartitioningType

from config_util import readconfig, get_credential


def readschema(sheetname):
    loc = "PubSub_Bigquery_input.xlsx"
    wb = xlrd.open_workbook(loc)
    sheet = wb.sheet_by_name(sheetname)
    newlist = list()
    for i in range(1, sheet.nrows):
        mode = sheet.row_values(i)[2] if sheet.row_values(i)[2] is not '' else 'NULLABLE'
        newlist.append(bigquery.SchemaField(sheet.row_values(i)[0], sheet.row_values(i)[1], mode=mode))
    return newlist


def get_bigquery_client():
    config = readconfig('BigQuery')
    credentials = get_credential(config)
    storage_client = bigquery.Client(credentials=credentials, project=config['ProjectId'])
    return storage_client


def create_table(config):
    # Construct a BigQuery client object.
    # client = bigquery.Client().from_service_account_json('pubsubtest.json')
    client = get_bigquery_client()

    table_id = "{}.{}.{}".format(config['ProjectId'], config['Dataset_Name'], config['Table_Name'])
    try:

        schema = readschema(config['schema_sheet_name'])

        table = bigquery.Table(table_id, schema=schema)
        if config['sourceFormat'] != 'None' and config['ExternalsourceUris'] != '':
            extConf = ExternalConfig(config['sourceFormat'])
            extConf.source_uris = eval('['+config['ExternalsourceUris']+']')
            table.external_data_configuration(extConf)

        if config['TimePartitioning_type'] != 'No Partition':
            type_ = TimePartitioningType.DAY
            field = config['TimePartitioning_field']
            expiration_ms = config['TimePartitioning_expiration_ms']
            table.time_partitioning(type_=type_, field=field, expiration_ms=expiration_ms)

            table.require_partition_filter = True if config['Require_partition_filter'] == 'Yes' else False

        table.clustering_fields = eval('['+config['Clustering_order_fields']+']') if config['Clustering_order_fields'] !='' else None
        table.encryption_configuration = config['Table_Encryption_Key'] if config['Table_Encryption_Key'] != '' else None

        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )
    except google.api_core.exceptions.Conflict as er:
        print('Table {} already present'.format(table_id))


def create_dataset(config):
    # Construct a BigQuery client object.
    # client = bigquery.Client().from_service_account_json('pubsubtest.json')
    client = get_bigquery_client()
    dataset_id = "{}.{}".format(config['ProjectId'], config['Dataset_Name'])
    try:
        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = config['Location']
        if 'Table_expiration_duration' in config.keys():
            dataset.default_table_expiration_ms = int(config['Table_expiration_duration']) if config['Table_expiration_duration'] != '' else None
          # Make an API request.
        if 'Encryption' in config.keys():
            dataset.default_encryption_configuration = config['Encryption_Key'] if config['Encryption'] == 'Customer-managed key' else None

        dataset = client.create_dataset(dataset)
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    except google.api_core.exceptions.Conflict as er:
        print('Dataset already present')
    create_table(config)
