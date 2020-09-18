import google
from google.cloud import storage
from config_util import readconfig, get_credential


def get_storage_client():
    config = readconfig('StorageBucket')
    credentials = get_credential(config)
    storage_client = storage.Client(credentials=credentials, project=config['ProjectId'])
    return storage_client


def create_bucket(config):
    """Creates a new bucket."""
    storage_client = get_storage_client()
    try:
        bucket = storage.Bucket(storage_client)
        bucket.name = config['Bucket_name']
        bucket.location = config['Location']
        if all(['Storage_class']) in config.keys():
            bucket.storage_class = config['Storage_class']
        if all(['Set Retention policy','Duration']) in config.keys():
            if config['Set Retention policy'] == 'Yes':
                bucket.retention_period = config['Duration']

        bucket = storage_client.create_bucket(bucket)

        if all(['Access_control']) in config.keys():
            if config['Access_control'] == 'Uniform':
                enable_uniform_bucket_level_access(config['Bucket_name'])
        if all(['Labels']) in config.keys():
            if len(eval(config['Labels'])) > 0:
                add_bucket_label(config['Bucket_name'], config['Labels'])

        print("Bucket {} created".format(bucket.name))
    except google.api_core.exceptions.Conflict as er:
        print('Please try with different storage name')


def check_bucket_available(config):
    """Lists all buckets."""
    notfound = True
    storage_client = get_storage_client()
    buckets = storage_client.list_buckets()
    bucket_name = config['Bucket_name']
    for bucket in buckets:
        if bucket.name == bucket_name:
            notfound = False

    if notfound:
        create_bucket(config)

    else:
        print('Bucket already exist')


def enable_uniform_bucket_level_access(bucket_name):
    """Enable uniform bucket-level access for a bucket"""

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    bucket.patch()

    print(
        "Uniform bucket-level access was enabled for {}.".format(bucket.name)
        )


def enable_default_kms_key(bucket_name, kms_key_name):
    """Sets a bucket's default KMS key."""

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bucket.default_kms_key_name = kms_key_name
    bucket.patch()

    print(
        "Set default KMS key for bucket {} to {}.".format(
            bucket.name, bucket.default_kms_key_name
            )
        )


def add_bucket_label(bucket_name, labelsadd):
    """Add a label to a bucket."""

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    labels = bucket.labels
    labels.update(eval(labelsadd))
    bucket.labels = labels
    bucket.patch()

    print("Updated labels on {}.".format(bucket.name))
