import json
import urllib.parse
import boto3
import os
import json

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):



    user="YOUR-CDP-WORKLOAD-USER"
    passwd="YOUR-CDP-WORLOAD-PASS"
    userpasswd=user+":"+passwd

    grafanaurl="<Your Grafana URL>"
    jobsurl="\"<Your jobs url>\" "
    #grafanaurl="https://service.cde-2sp6d49p.sunman1.a465-9q4k.cloudera.site/grafana/d/0Oq0WmQWk/instance-metrics?orgId=1&refresh=5s&var-virtual_cluster_name=etl"
    #jobsurl="\"https://9b9wwrgp.cde-2sp6d49p.sunman1.a465-9q4k.cloudera.site/dex/api/v1/jobs/testjob/run\" "

    #example
    #jobpayload="\"{\\\"overrides\\\":{\\\"spark\\\":{\\\"args\\\":[\\\"hello\\\"]}},\\\"user\\\":\\\""+user+"\\\"}\""
    jobpayloadpre="\"{\\\"overrides\\\":{\\\"spark\\\":{\\\"args\\\":[\\\""
    jobpayloadpost="\\\"]}},\\\"user\\\":\\\""+user+"\\\"}\""
    getToken="curl -u \""+userpasswd+"\" $(echo \'"+grafanaurl+"\' | cut -d\'/\' -f1-3 | awk \'{print $1\"/gateway/authtkn/knoxtoken/api/v1/token\"}\')"

    print("curling to get cde token")

    stream= os.popen(getToken)
    json_object_string = stream.read()
    json_object = json.loads(json_object_string)
    cde_token=json_object["access_token"]
    #print("Access Token: "+cde_token)



    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        print("bucket: "+bucket)
        print("key: "+key)
        jobpayload=jobpayloadpre+bucket+"/"+key+jobpayloadpost
        print("jobpayload: "+jobpayload)

        #build job url
        jobsubmit="curl -b hadoop-jwt="+cde_token+" -X POST "+jobsurl+" -H \"Content-Type: application/json\" -d "+jobpayload

        print("call cde for spark submit")
        jobstream = os.popen(jobsubmit)
        joboutput = jobstream.read()
        print("CDE JobID: "+joboutput)

        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e