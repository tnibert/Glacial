#Python Examples of using AWS S3

import boto3

s3 = boto3.resource(‘s3’)
for bucket in s3.buckets.all():
    print(bucket.name)

# Upload a new file
data = open('test.jpg', 'rb')
s3.Bucket('my-bucket').put_object(Key='test.jpg', Body=data)


#Python Examples of using AWS Glacier

#import boto3
import json
import pprint

awsAccess = “AccessKey”
awsSecret =  “SecretKey”
vName = “Vaultnamehere”
fileName = “filenamehereincludingthepath”
fileDesc = “AmazonEMRZipFile”

glacier = boto3.client(‘glacier’, region_name=’ap-south-1′)

#List Vaults
response = glacier.list_vaults()
print(json.dumps(response,indent=1))


#Create a Vault and Upload data to an archive

#myvault = glacier.create_vault(vaultName=vName)
#archive = glacier.upload_archive(vaultName=vName, archiveDescription=fileDesc, body=fileName)

# List the number of jobs

response = glacier.list_jobs(vaultName=vName)
print(json.dumps(response,indent=1))


# Please do keep track of the archive id

archiveid = glacier.upload_archive(vaultName=vName, archiveDescription=fileDesc, body=fileName)
print(json.dumps(archiveid,indent=1))
getvault = glacier.describe_vault(vaultName=vName)
print(json.dumps(getvault,indent=1))

# If you ever want, you can delete the archive on the vault
# with the archive ID.
# v.delete_archive(archiveID)

# Describe a JOB
response = glacier.describe_job(vaultName=vName, jobId=”JOBIDHERE”)
print(json.dumps(response,indent=1))

#Describe a Vault
response = glacier.describe_vault(vaultName=vName)
pprint.pprint(response)
