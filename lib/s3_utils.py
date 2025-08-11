''' Functions for connecting to S3 and uploading/downloading files from it '''

import os
import boto3
from boto3.s3.transfer import TransferConfig
import botocore

# SET UP LOGGING
import logging
from pythonjsonlogger import jsonlogger


formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(message)s')
logHandler = logging.StreamHandler()
logHandler.setFormatter(formatter)
logger = logging.getLogger('s3_logger')
logger.propagate = False
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)
logger.info('Starting job')


def _create_session(region_name='us-west-2',
                    environment='aws',
                    profile_name='invcts-federated'):
    ''' Creates boto3 session

    Inputs
    ======
    region_name : string
        name of region
    environment : string
        whether the script is running locally or on aws
    profile_name : string
        profile name for credential purposes when running locally,
        typically 'invcts-federated'

    Example use
    ===========
    N/A: this function is not intended to be called directly by user
    '''

    if environment == 'aws':
        session = boto3.session.Session(region_name=region_name)
    else:
        session = boto3.session.Session(region_name=region_name,
                                        profile_name=profile_name)
    return session


def get_temp_creds(region_name='us-west-2',
                   environment='aws',
                   profile_name='invcts-federated'):
    ''' Gets temporary credentials for COPY and LOAD Redshift commands

    Inputs
    ======
    region_name : string
        name of region
    environment : string
        whether the script is running locally or on aws
    profile_name : string
        profile name for credential purposes when running locally,
        typically 'invcts-federated'

    Example use
    ===========
    get_temp_creds(region_name='us-west-2',
                   environment='local',
                   profile='invcts-federated')
    '''

    session = _create_session(region_name, environment, profile_name)
    ak = session.get_credentials().access_key
    sk = session.get_credentials().secret_key
    tkn = session.get_credentials().token
    cred_str = 'aws_access_key_id={0};aws_secret_access_key={1};token={2}'.format(ak, sk, tkn)
    return cred_str


def get_bucket(bucket,
               region_name='us-west-2',
               environment='aws',
               profile_name='invcts-federated'):
    ''' Retreives S3 bucket

    If running locally you should have the AWS credential updater
    running at the same time you run this code.

    Inputs
    ======
    bucket : string
        S3 bucket name
    region_name : string
        name of region
    environment : string
        whether the script is running locally or on aws
    profile_name : string
        profile name for credential purposes when running locally,
        typically 'invcts-federated'

    Example use
    ===========
    get_bucket(bucket='persis-datalab-team',
               region_name='us-west-2',
               environment='local', profile='invcts-federated')
    '''

    session = _create_session(region_name, environment, profile_name)
    s3 = session.resource('s3')
    mybucket = s3.Bucket(bucket)
    try:
        s3.meta.client.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404
        # if it was a 404 error, then the bucket does not exist
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            raise NameError('404 bucket does not exist')
    return mybucket


def rename_s3_file(bucket,
                   old_file,
                   new_file,
                   region_name='us-west-2',
                   environment='aws',
                   profile_name='invcts-federated'):
    ''' Renames a file in an S3 bucket

    If running locally you should have the AWS credential updater
    running at the same time you run this code.

    Inputs
    ======
    bucket : string
        S3 bucket name
    old_file : string
        s3 path and old file name
    new_file : string
        s3 path and new file name
    region_name : string
        name of region
    environment : string
        whether the script is running locally or on aws
    profile_name : string
        profile name for credential purposes when running locally,
        typically 'invcts-federated'

    Example use
    ===========
    rename_s3_file(bucket='persis-datalab-team',
                   old_file='temp/old_file.csv',
                   new_file='temp/new_file.csv')
    '''

    session = _create_session(environment=environment)
    s3 = session.resource('s3')
    s3.Object(bucket, new_file).copy_from(CopySource=bucket + '/' + old_file)
    s3.Object(bucket, old_file).delete()
    logger.info('S3: {} renamed to {}'.format(old_file, new_file))


def download_file_from_s3(bucket,
                          s3_path,
                          filename,
                          filepath='',
                          region_name='us-west-2',
                          environment='aws',
                          profile_name='invcts-federated',
                          multipart_threshold=8388608,
                          multipart_chunksize=8388608):
    ''' Downloads file(s) from an S3 bucket

    If running locally you should have the AWS credential updater
    running at the same time you run this code.

    Inputs
    ======
    bucket : string
        S3 bucket name
    s3_path : string
        path within the bucket to the file you would like to download
    filename: string
        name of the file you would like to download
    filepath : string
        path to the local directory in which you would like to save the file
    region_name : string
        name of region
    environment : string
        whether the script is running locally or on aws
    profile_name : string
        profile name for credential purposes when running locally,
        typically 'invcts-federated'
    multipart_threshold : int
        minimum filesize to initiate multipart download
    multipart_chunksize : int
        chunksize for multipart download

    Example use
    ===========
    download_file_from_s3(bucket='persis-datalab-team',
                          s3_path='tmp/',
                          filename='myfile.csv',
                          filepath='data/',
                          environment='local')
    '''

    mybucket = get_bucket(bucket, region_name, environment, profile_name)
    # multipart_threshold and multipart_chunksize defaults = Amazon defaults
    config = TransferConfig(multipart_threshold=multipart_threshold,
                            multipart_chunksize=multipart_chunksize)
    logger.info('S3_path + filename = {0}'.format(s3_path + filename))
    logger.info('Local filepath + filename = {0}'.format(os.path.join(filepath, filename)))
    mybucket.download_file(s3_path + filename,
                           os.path.join(filepath, filename),
                           Config=config)


def upload_file_to_s3(bucket,
                      s3_path,
                      filename,
                      filepath='',
                      region_name='us-west-2',
                      environment='aws',
                      profile_name='invcts-federated',
                      multipart_threshold=8388608,
                      multipart_chunksize=8388608):
    ''' Uploads a file to an S3 bucket

    If running locally you should have the AWS credential updater
    running at the same time you run this code.

    Inputs
    ======
    bucket : string
        S3 bucket name
    s3_path : string
        path within the bucket to the file you would like to download
    filename: string
        name of the file you would like to download
    filepath : string
        path to the local directory in which you would like to save the file
    region_name : string
        name of region
    environment : string
        whether the script is running locally or on aws
    profile_name : string
        profile name for credential purposes when running locally,
        typically 'invcts-federated'
    multipart_threshold : int
        minimum filesize to initiate multipart upload
    multipart_chunksize : int
        chunksize for multipart upload

    Example use
    ===========
    upload_file_to_s3(bucket='persis-datalab-team',
                      s3_path='tmp/',
                      filename='myfile.csv',
                      filepath='data/',
                      environment='local')
    '''

    mybucket = get_bucket(bucket, region_name, environment, profile_name)
    # multipart_threshold and multipart_chunksize defaults = Amazon defaults
    config = TransferConfig(multipart_threshold=multipart_threshold,
                            multipart_chunksize=multipart_chunksize)
    mybucket.upload_file(os.path.join(filepath, filename),
                         s3_path + filename,
                         Config=config)
