
#Step 1: Create AWS S3 bucket using AWS credentials
import boto3 #AWS SDK 
import os
import tempfile 


key_id = ''
secret_key = ''
BUCKET_NAME = 'final-etl-v1'

def create_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)
        print('Directory created:', directory_path)
    else:
        print('Directory already exists:', directory_path)


def setup():
#0. Create directories for future scripts

    # Create the main input csv data directory PART B AND C
    data_directory = os.path.join(os.getcwd(), 'data')
    create_directory(data_directory)

    # Create the processed csv data directory PART C
    split_csv_dir = os.path.join(data_directory, 'batches')
    create_directory(split_csv_dir)

    # Create the processed csv data directory PART C
    processed_csv_dir = os.path.join(split_csv_dir, 'processed')
    create_directory(processed_csv_dir)

    # Create the parquets directory: PART C
    parquet_directory = os.path.join(data_directory, 'parquets')
    create_directory(parquet_directory)

    # Create the processed parquets directory PART D
    parquet_processed_dir = os.path.join(parquet_directory, 'processed')
    create_directory(parquet_processed_dir)



#1. S3 bucket creation and testing

    #A. establish connection to s3 using AWS credentials
    s3_client = boto3.client("s3", aws_access_key_id = key_id, aws_secret_access_key = secret_key)
    #B. create bucket 
    try:
        s3_client.create_bucket(Bucket = BUCKET_NAME)
    except Exception as e:
        print('Error: ',e)

    else:
    #Step 2:If bucket is created, then we must test writing to bucket and reading from bucket

        #2A: Create temp files to test writing to S3
        #create a temporary subdirectory to save all temporary files to
    
        temp_dir = tempfile.mkdtemp() 
        temp_file_name = os.path.join(temp_dir, 'test1.txt')
        with open(temp_file_name,'w') as file:
            file.write("hello world!")

        #Step 2B: Upload temp files to S3 bucket
            data = open(temp_file_name,'rb')
            KEY = 'test1.txt'
            s3_client.put_object(Body = data, Bucket = BUCKET_NAME, Key = KEY)
        #STEP 2C: Test reading files from bucket

        s3= boto3.resource("s3", aws_access_key_id = key_id, aws_secret_access_key = secret_key)
        bucket = s3.Bucket(BUCKET_NAME)


        for obj in bucket.objects.all():
            key = obj.key
            body = obj.get()['Body'].read()
            print("S3 Bucket Contents: ,",key,body)

        #Step 3: Clear all test data from bucket
        for obj in bucket.objects.all():
            key = obj.key
            s3_client.delete_object(Bucket= BUCKET_NAME, Key=key)   
        print('S3 Bucket successfully created and is empty.')

#setup()