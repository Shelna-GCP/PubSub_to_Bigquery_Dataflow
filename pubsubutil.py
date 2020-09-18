"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
import time
import os
import google
from google.api_core.retry import Retry
from google.cloud import pubsub_v1

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'pubsubtest.json'
from google.pubsub_v1 import Topic, Subscription

from accessGCPSecretKey import access_secret_version


def get_publisher_client(config):
    credential_path = access_secret_version(config['ProjectId'], config['ProjectId'], version_id=1)
    client = pubsub_v1.PublisherClient().from_service_account_json(credential_path)
    return client


def get_subscriber_client(config):
    credential_path = access_secret_version(config['ProjectId'], config['ProjectId'], version_id=1)
    client = pubsub_v1.SubscriberClient().from_service_account_json(credential_path)
    return client


def createpub(config):
    from google.cloud import pubsub_v1

    publisher = get_publisher_client(config)
    topic_path = publisher.topic_path(config['ProjectId'], config['TopicName'])
    kms_key_name = None
    if 'Encryption_Key' in config.keys():
        kms_key_name = None if config['Encryption_Key'] == '' else config['Encryption_Key']
    try:
        topic = Topic(name = topic_path , kms_key_name=kms_key_name)
        topic = publisher.create_topic(topic)

        print("Topic created: {}".format(topic))
    except BaseException as e:
        print(e)

def createsubscription(config):
    push_config = None
    expiration_policy = None
    ack_deadline_seconds = None
    message_retention_duration = None
    retain_acked_messages = None
    enable_message_ordering = None
    dead_letter_policy = None
    retry = None

    subscriber = get_subscriber_client(config)
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=config['ProjectId'],
        topic=config['TopicName'],  # Set this to something appropriate.
        )
    subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
        project_id=config['ProjectId'],
        sub=config['SubscritonName'],  # Set this to something appropriate.
        )
    if all(['Delivery_type', 'push_endpoint']) in config.keys():
        push_config = None if config['Delivery_type'] == 'Pull' else {'push_endpoint': config['push_endpoint'],
                                                                      'attributes': config['attributes']}
    if all(['Subscription_expiration_duration']) in config.keys():
        expiration_policy = None if config['Subscription_expiration_duration'] == '' else {
            'ttl': config['Subscription_expiration_duration']}
    if all(['Acknowledgement_deadline']) in config.keys():
        ack_deadline_seconds = None if config['Acknowledgement_deadline'] == '' else config['Acknowledgement_deadline']
    if all(['Retain_acknowledged_messages', 'Message_retention_duration']) in config.keys():
        retain_acked_messages = True if config['Retain_acknowledged_messages'] == 'Yes' else False
        message_retention_duration = config['Message_retention_duration'] if retain_acked_messages == True else None
    if all(['Message_ordering']) in config.keys():
        enable_message_ordering = True if config['Message_ordering'] == 'Yes' else False
    if all(['Dead_letter_policy_enable', 'dead_letter_topic', 'max_delivery_attempts']) in config.keys():
        dead_letter_policy = None if config['Dead_letter_policy_enable'] == 'No' else {
            'dead_letter_topic': config['dead_letter_topic'], 'max_delivery_attempts': config['max_delivery_attempts']}
    if all(['minimum_backoff', 'maximum_backoff', 'Retry_policy_enable']) in config.keys():
        retry = Retry(initial=config['minimum_backoff'], maximum=config['maximum_backoff']) if config['Retry_policy_enable'] == 'Yes' else None
    try:
        subscription = Subscription(
            name=subscription_name, topic=topic_name, push_config=push_config, expiration_policy=expiration_policy,
            ack_deadline_seconds=ack_deadline_seconds, retain_acked_messages=retain_acked_messages,
            message_retention_duration=message_retention_duration, enable_message_ordering=enable_message_ordering,
            dead_letter_policy=dead_letter_policy, retry_policy=retry
            )
        subscriber.create_subscription(subscription)
        print("Subscription created: {}".format(subscription_name))
    except BaseException as e:
        print(e)

def checktopicavailable(config):
    print('Verifying the topic')

    try:
        client = get_publisher_client(config)
        topic = client.topic_path(config['ProjectId'], config['TopicName'])
        response = client.get_topic(topic=topic)
        return response
    except google.api_core.exceptions.NotFound as er:
        print('Topic doesnot exist. Creating New Topic :{}'.format(config['TopicName']))
        createpub(config)


def checksubscriptionavailable(config):
    print('Verifying the subsciption ')
    client = get_subscriber_client(config)
    try:
        subscription = client.subscription_path(config['ProjectId'], config['SubscritonName'])
        response = client.get_subscription(subscription=subscription)
        return response
    except google.api_core.exceptions.NotFound as er:
        print('Subscription doesnot exist. Creating New Subscription :{}'.format(subscription))
        createsubscription(config)


def publishmessage(config, message):
    checktopicavailable(config)

    batch_settings = ()
    if config['Publish_type'] == 'Recurring':
        max_messages = int(config['Number_of_messages'])  # default 100
        max_latency = int(config['Message_interval'])  # default 10 ms

        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages=max_messages,
            max_bytes=1024,  # default 1 MB
            max_latency=max_latency,
            )
    publisher_options = ()
    attr = dict()
    if config['Message_ordering'] == 'Yes':
        enable_message_ordering = True
        attr['ordering_key'] = config['ordering_key ']
        publisher_options = pubsub_v1.types.PublisherOptions(
            enable_message_ordering=True
            )
    if config['Message_attributes'] != '':
        attrdict = eval(config['Message_attributes'])
        attr.update(attrdict)

    credential_path = access_secret_version(config['ProjectId'], config['ProjectId'], version_id=1)
    publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings, publisher_options=publisher_options).from_service_account_json(credential_path)
    #publisher = get_publisher_client(config)

    topic_path = publisher.topic_path(config['ProjectId'], config['TopicName'])

    futures = dict()

    def get_callback(f, data):
        def callback(f):
            try:
                print(f.result())
                futures.pop(data)
            except:  # noqa
                print("Please handle {} for {}.".format(f.exception(), data))

        return callback

    jsondata = message
    # print(jsondata)
    futures.update({jsondata: None})
    # When you publish a message, the client returns a future.
    future = publisher.publish(
        topic_path, data=jsondata.encode("utf-8"),**attr  # data must be a bytestring.
        )
    futures[jsondata] = future
    # Publish failures shall be handled in the callback function.
    future.add_done_callback(get_callback(future, jsondata))

    # Wait for all the publish futures to resolve before exiting.
    while futures:
        time.sleep(5)

    print("Published message with error handler.")
