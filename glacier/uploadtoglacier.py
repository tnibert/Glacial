#! /usr/bin/env python3
from sys import argv
import boto3
import pprint
from datetime import date

client = boto3.client('glacier')
vaultname = "familyphotos"
to_upload = argv[1]
print("Uploading {}...".format(to_upload))

# upload file
with open(to_upload, 'rb') as f:
    response = client.upload_archive(vaultName=vaultname,
                                     archiveDescription="{}-{}".format(to_upload, date.today()),
                                     body=f)

pprint.pprint(response)
print("Done")

# you can get all archive IDs by running a vault inventory job
def refresh_inventory():
    """
    Untested
    """
    job_req = client.initiate_job(vaultName='myvault',
                                  jobParameters={'Type': 'inventory-retrieval'})

    while True:
        status = client.describe_job(vaultName='myvault',
                                     jobId=job_req['jobId'])
        if status['Completed']:
            break
        time.sleep(300)

    job_resp = client.get_job_output(vaultName='myvault',
                                     jobId=job_req['jobId'])

    # first download the output and then parse the JSON
    output = job_resp['body'].read()

    archive_list = json.loads(output)['ArchiveList']
    # persist archive_list

    return archive_list
