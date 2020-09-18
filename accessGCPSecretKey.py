from google.api_core.exceptions import NotFound
from google.cloud import secretmanager
import  os
import hashlib

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'E:\\GCPProjects\\json\\kms_sa.json'
def access_secret_version(PROJECT_ID,secret_id, version_id="latest"):

    # secret manager client object
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    try:
        name = client.secret_version_path(PROJECT_ID, secret_id, version_id)

        # Access the secret version.
        response = client.access_secret_version(name)

        # Return the decoded payload.
        return response.payload.data.decode('UTF-8')

    except NotFound as ex:
        print(ex.message)


def secret_hash(secret_value):
    # return the sha224 hash of the secret value
    return hashlib.sha224(bytes(secret_value, "utf-8")).hexdigest()


