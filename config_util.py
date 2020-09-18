import os
import time
from googleapiclient import discovery
import xlrd
from google.oauth2 import service_account
from datetime import datetime

from accessGCPSecretKey import access_secret_version


def readconfig(sheetname):
    newdict = dict()
    loc = "PubSub_Bigquery_input.xlsx"
    wb = xlrd.open_workbook(loc)
    sheet = wb.sheet_by_name(sheetname)
    for i in range(sheet.nrows):
        newdict[sheet.cell_value(i, 0)] = sheet.cell_value(i, 1)
    print('Processing {}:'.format(sheetname), newdict)
    print('****************************************************************************\n ')
    return newdict


def get_credential(config):
    credential = access_secret_version(PROJECT_ID=config['ProjectId'], secret_id=config['ProjectId'], version_id=1)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential
    credentials = service_account.Credentials.from_service_account_file(credential)
    return credentials


def get_current_time():
    now = datetime.now()
    return now


def wait_for_operation(service, project, operation):
    print('Waiting for operation to finish...')
    try:
        while True:
            result = service.operations().get(
                project=project,
                operation=operation).execute()

            if result['status'] == 'DONE':
                print("Done.")
                if 'error' in result:
                    raise Exception(result['error'])
                return result
            time.sleep(1)

    except ConnectionResetError as e:
        print('May be some connectivity issue occurred')


def modify_policy_add_member(config, policy, role, member):
    """Adds a new member to a role binding."""

    binding = next(b for b in policy["bindings"] if b["role"] == role)
    binding["members"].append(member)
    return policy


def getresourceservice(config):
    credentials = get_credential(config)
    service = discovery.build("cloudresourcemanager", "v1", credentials=credentials)
    return service


def set_policy(config,project_id, policy):
    """Sets IAM policy for a project."""

    service = getresourceservice(config)

    policy = (
        service.projects().setIamPolicy(
            resource=project_id, body={"policy": policy}).execute())
    print('Added \'roles/storage.admin\' to the sql service account')
    return policy


def get_policy(config, version=1):
    """Gets IAM policy for a project."""

    service = getresourceservice(config)
    policy = (
        service.projects().getIamPolicy(
            resource=config['ProjectId'],
            body={"options": {"requestedPolicyVersion": version}},
            ).execute())
    return policy


def add_service_account_role(config, role, member):
    policy = get_policy(config, 1)
    policynew = modify_policy_add_member(config=config, policy=policy, role=role, member=member)
    set_policy(config=config, project_id=config['ProjectId'], policy=policynew)
