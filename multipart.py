from botocore.utils import calculate_tree_hash
from botocore.exceptions import BotoCoreError
import boto3
import pprint

# uploading as chunks of 2mb
CHUNK_SIZE = 1048576 * 2
#AWS_ACCESS_KEY_ID = "Your access key id"
#AWS_SECRET_ACCESS_KEY = "Your secret access key"


# todo: investigate making async for more throughput
def upload_large_file(vault_name, filepath, description):
    """
    Do a multi part upload to glacier
    :param vault_name:
    :param filepath:
    :param description:
    :return:
    """
    glacier = boto3.resource("glacier")
    vault = glacier.Vault(account_id="-", name=vault_name)

    multipart_upload = vault.initiate_multipart_upload(accountId="-", archiveDescription=description, partSize=str(CHUNK_SIZE))
    upload_id = multipart_upload.id
    print("Upload id: {}".format(upload_id))

    with open(filepath, 'rb') as f:
        retrylist = upload_segments(multipart_upload, read_in_chunks(f, CHUNK_SIZE))

        f.seek(0, 2)
        fsize = f.tell()

        while len(retrylist) > 0:
            print("Retrying failed parts")
            # syntax turns list into a generator
            retrylist = upload_segments(multipart_upload, (i for i in retrylist))

        print("Finalizing upload {} ...".format(upload_id))
        f.seek(0)
        s256t_hash = calculate_tree_hash(f)
        response = multipart_upload.complete(archiveSize=str(fsize),
                                             checksum=s256t_hash)
        print("Hash: {}".format(s256t_hash))

    pprint.pprint(response)
    return response
    #archive_id = response.get('archiveId')
    #return archive_id


def read_in_chunks(file_obj, chunk_size):
    """
    Reads the given file as chunks, instead of loading everything into memory.
    Returns generator
    """
    while True:
        start_range = file_obj.tell()
        data = file_obj.read(chunk_size)
        if not data:
            break
        range_data = "bytes %s-%s/*" % (start_range, file_obj.tell() - 1)
        yield {'range': range_data, 'body': data}


def upload_segments(mp_upload, chunks_generator):
    """
    Upload an individual chunk of file to multipart upload, with error handling
    :param mp_upload: multipart upload object
    :param chunks_generator: generator yielding file data in dicts of
            {'range': range_data, 'body': data}
    :return: list of chunks that need upload retried
    """
    newretrylist = []
    for chunk in chunks_generator:
        print("Uploading range %s" % chunk['range'])
        try:
            mp_upload.upload_part(**chunk)
        except BotoCoreError as e:
            print(e)
            newretrylist.append(chunk)
    return newretrylist
