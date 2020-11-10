import json
import urllib.parse
import boto3
import urllib3
import uuid

print('Loading function')

s3 = boto3.client('s3')
grafanaurl = "https://<grafanaurl from console>/gateway/authtkn/knoxtoken/api/v1/token"
jobsurl = "https://<jobs url from console>/dex/api/v1/jobs/jobname/run"
arg1 = "<any argument>"
arg2 = "<any argument>"
cde_uuid = str(uuid.uuid1())

def get_secret():
    """Retrieve the CDE credentials from AWS Secrets Manager."""
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(
            SecretId='your-service-id'
        )
    except ClientError as e:
        print('Failed to retrieve secret from Secrets Manager.')
        print(e)
        exit()
    else:
        secret = json.loads(response['SecretString'])
        return '{0}:{1}'.format(secret['username'], secret['password'])

def getToken(grafanaurl):
    creds = get_secret()
    http = urllib3.PoolManager()
    headers = urllib3.make_headers(basic_auth=creds)
    req_token = http.request('GET', grafanaurl, headers=headers)

    token = json.loads(req_token.data.decode('utf-8'))
    return token['access_token']

def submitjob(cde_token, jobsurl, jobpayload):
    http = urllib3.PoolManager()
    headers = {"Content-Type": "application/json",
               "Authorization": "Bearer " + cde_token}
    encodedpayload = json.dumps(jobpayload).encode('utf-8')
    r_jobsubmit = http.request('POST', jobsurl, headers=headers, body=encodedpayload)
    print(r_jobsubmit.status)
    print(dir(r_jobsubmit))


def lambda_handler(event, context):

    print("Function Star")

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    payload_dict = {
    "overrides": {
        "spark": {"args": ['s3a://{}/{}'.format(bucket,key),
                           arg1,
                           arg2]
                  }
        },
    "variables": {"uuid": cde_uuid }
    }

    cde_token = getToken(grafanaurl)

    submitjob(cde_token, jobsurl, payload_dict)