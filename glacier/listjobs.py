import boto3
import pprint

client = boto3.client('glacier')
vaultname = "familyphotos"
response = client.list_jobs(vaultName=vaultname)
pprint.pprint(response)
