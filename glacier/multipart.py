from botocore.utils import calculate_tree_hash
import boto3
import pprint

# based on http://blog.ragsagar.in/2015/10/03/large-file-upload-using-amazon-glacier-boto3.html

# I am uploading as chunks of 32mb
CHUNK_SIZE = 1048576 * 32
#AWS_ACCESS_KEY_ID = "Your access key id"
#AWS_SECRET_ACCESS_KEY = "Your secret access key"


def upload_large_file(vault_name, filepath, description):
    glacier = boto3.resource("glacier")
    vault = glacier.Vault(account_id="-", name=vault_name)

    multipart_upload = vault.initiate_multipart_upload(accountId="-", archiveDescription=description, partSize=str(CHUNK_SIZE))
    upload_id = multipart_upload.id
    print("Upload id: {}".format(upload_id))

    with open(filepath, 'rb') as f:
        start_range = 0
        for chunk in read_in_chunks(f, CHUNK_SIZE):
            range_data = "bytes %s-%s/*" % (start_range, f.tell()-1)
            print("Uploading range %s" % range_data)
            multipart_upload.upload_part(range=range_data, body=chunk)
            start_range = f.tell()

        print("Finalizing...")
        s256t_hash = calculate_tree_hash(f)
        f.seek(0)
        response = multipart_upload.complete(archiveSize=str(start_range),
                                             checksum=s256t_hash)
        print("Hash: {}".format(s256t_hash))

    pprint.pprint(response)
    return response
    #archive_id = response.get('archiveId')
    #return archive_id


def read_in_chunks(file_obj, chunk_size):
    """ Reads the given file as chunks, instead of loading everything into memory."""
    while True:
        data = file_obj.read(chunk_size)
        if not data:
            break
        yield data
