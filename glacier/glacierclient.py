#! /usr/bin/env python3
from multipart import upload_large_file, CHUNK_SIZE
from datetime import date
from os.path import getsize
import boto3
import pprint
import argparse
import time
import json


LOG = "glacier.log"


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
    parsedoutput = json.loads(output)
    pprint.pprint(parsedoutput)

    archive_list = parsedoutput['ArchiveList']
    # persist archive_list
    return archive_list


def write_log(data):
    """
    Record glacier object upload
    :param data: information to write out
    :return:
    """
    with open(LOG, "a") as fi:
        fi.write(str(data))
        fi.write("\n")

# read arguments
parser = argparse.ArgumentParser()

# todo: add help text
parser.add_argument("-u")
parser.add_argument("-i", action='store_true')
parser.add_argument("-j", default=None)
parser.add_argument("-l", action='store_true')
parser.add_argument("-v", required=True)        # vault name

args = parser.parse_args()

client = boto3.client('glacier')
vaultname = args.v
to_upload = args.u      # the file name to upload

# if the file size is less than the chunk size
# upload in one part, otherwise upload multi part
if getsize(to_upload) <= CHUNK_SIZE:
    print("Uploading {}...".format(to_upload))

    # upload file
    with open(to_upload, 'rb') as f:
        response = client.upload_archive(vaultName=vaultname,
                                         archiveDescription="{}-{}".format(to_upload, date.today()),
                                         body=f)
    pprint.pprint(response)
    write_log(response)

else:
    print("Uploading {}...".format(to_upload))

    response = upload_large_file(vaultname, to_upload, "{}-{}".format(to_upload, date.today()))
    write_log(response)

if args.i is True:
    pprint.pprint(refresh_inventory(vaultname, args.j))

if args.l is True:
    response = client.list_jobs(vaultName=vaultname)
    pprint.pprint(response)

print("Done")
