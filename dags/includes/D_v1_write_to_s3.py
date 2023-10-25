"""
#Summary:
#This connects to the S3 bucket created in 'v1-first-setup.py' and reads the saved parquet files from C_v1-etl-pyspark. These parquet files will then 
#be written to the S3 bucket
"""

#Step 1: Connect to AWS S3 bucket using AWS credentials
import boto3
import os 
import shutil 


key_id = ''
secret_key = ''
BUCKET_NAME = 'final-etl-v1'


def write_to_s3():
    s3= boto3.resource("s3", aws_access_key_id = key_id, aws_secret_access_key = secret_key)

    #establish connection to s3 using AWS credentials
    s3_client = boto3.client("s3", aws_access_key_id = key_id, aws_secret_access_key = secret_key)

    #Step 4: Access bucket:
    bucket = s3.Bucket(BUCKET_NAME)

    local_dir = os.path.join(os.getcwd(),'data','parquets')
    local_filepath = ''
    s3_path = ''

    for dirpath,directories,files in os.walk(local_dir):
        #dirpath represents the directory, i.e. data\parquets\heart_data0
        #files is a list of filenames inside of that subdirectory 
        #Iterate through each file and upload to s3 bucket

        #Don't upload any files located in 'processed' folder.

        if 'processed' in directories:
            directories.remove('processed')

        for file in files:
            if file.endswith('parquet'):
                local_filepath = os.path.join(dirpath,file)
                s3_path = os.path.join('s3-prefix',file)

                s3_client.upload_file(local_filepath,BUCKET_NAME, s3_path)
                print('Successfully uploaded ', file, ' to ', BUCKET_NAME)
                #move the folder containing the uploaded parquet to processed folder so it doesn't get reprocessed
                shutil.move(dirpath, os.path.join(local_dir,'processed'))


#write_to_s3()