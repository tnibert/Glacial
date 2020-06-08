#! /usr/bin/env python3
from sys import argv
import boto3
import pprint
from datetime import date
import argparse
import time


# you can get all archive IDs by running a vault inventory job                                                                                                                               
def refresh_inventory(vault, jobid=None):
    """                                                                                                                                                                                       
    Untested                                                                                                                                                                                  
    """
    if not jobid:
        job_req = client.initiate_job(vaultName=vault,
                                      jobParameters={'Type': 'inventory-retrieval'})
        pprint.pprint(job_req)
        jobid = job_req['jobId']

    while True:
        status = client.describe_job(vaultName=vault,
                                     jobId=jobid)
        pprint.pprint(status)
        if status['Completed']:
            print("Job complete")
            break
        sleeptime = 300
        print("Sleeping {}...".format(sleeptime))
        time.sleep(sleeptime)

    job_resp = client.get_job_output(vaultName=vault,
                                     jobId=jobid)

    # first download the output and then parse the JSON                                                                                                                                       
    output = job_resp['body'].read()
    pprint.pprint(output)

    archive_list = json.loads(output)['ArchiveList']
    # persist archive_list                                                                                                                                                                    

    return archive_list


# read arguments
parser = argparse.ArgumentParser()

parser.add_argument("-u")
parser.add_argument("-i", action='store_true')
parser.add_argument("-j", default=None)

args = parser.parse_args()

client = boto3.client('glacier')
vaultname = "familyphotos"
to_upload = args.u
if to_upload:
    print("Uploading {}...".format(to_upload))

    # upload file
    with open(to_upload, 'rb') as f:
        response = client.upload_archive(vaultName=vaultname,
                                         archiveDescription="{}-{}".format(to_upload, date.today()),
                                         body=f)
    pprint.pprint(response)

if args.i is True:
    print(refresh_inventory(vaultname, args.j))

print("Done")
