import sys.argv
import boto3
from datetime import date

client = boto3.client('glacier')
vaultname = "familyphotos"
to_upload = sys.argv[1]
print(to_upload)

# upload file
with open(to_upload, 'rb') as f:
    response = client.upload_archive(vaultName=vaultname,
                                     archiveDescription="{}-{}".format(to_upload, date.today()),
                                     body=f)

print(response)

def refresh_inventory():
    job req = client.initiate_job(vaultName='myvault',
                                  jobParameters={'Type': 'inventory-retrieval'})

    while True:
        status = client.describe_job(vaultName='myvault',
                                     jobId=job_req['jobId']
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
