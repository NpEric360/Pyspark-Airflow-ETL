"""
#This script will read all 'contaminated' .csv files generated in B_v1_data_scrambler, cleans the data, and converts each into seperate parquets

#1. Creates a spark session
#2. Specify the schema of the input data to avoid issues with Pyspark's schema inferences
#3. Clean the data:
    #a. Outliers
    #b. Correct ranges
    #C. Duplicate rows
    #D. missing values (other methods should be tested)
#4. Creates all necessary directories to store parquets
#5. Convert transformed data to parquets

"""
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col
import datetime

#Define directories

parquet_directory = os.path.join(os.getcwd(),'data','parquets')
batches_directory = os.path.join(os.getcwd(),'data','batches')
processed_csv_dir = os.path.join(batches_directory,'processed')





#Define the schema to avoid incorrect schema inferences when spark.read.csv
custom_schema = StructType([
    StructField("Age", IntegerType(), True),
    StructField("Sex", StringType(), True),
    StructField("ChestPainType", StringType(), True),
    StructField("RestingBP", IntegerType(), True),
    StructField("Cholesterol", IntegerType(), True),
    StructField("FastingBS", StringType(), True),
    StructField("RestingECG", StringType(), True),
    StructField("MaxHR", IntegerType(), True),
    StructField("ExerciseAngina", StringType(), True),
    StructField("Oldpeak", DoubleType(), True),
    StructField("ST_Slope", StringType(), True),
    StructField("HeartDisease", StringType(), True)
])

#Step 1: Fetch one of the .csv batch files if it exists, and return a pyspark dataframe
def fetch_csv(spark):    
    files = os.listdir(batches_directory)
    
    for filename in files:
        if filename.endswith('.csv'):
            current_filepath = os.path.join(batches_directory,filename)
            pyspark_dataframe = spark.read.csv(current_filepath, header=True, schema = custom_schema)

            if not pyspark_dataframe.isEmpty():
                #Return the filepath so we can move it to a 'processed' folder once it is written to a parquet
                #This cannot be done now because pyspark needs the csv file to remain in its current directory
                #csv_path = os.path.join(processed_dir,filename)
                return pyspark_dataframe, filename
    #If no .csv files found, return None,None
    return None, None


#Step 2. Remove incorrect datatypes, and outliers

def clean_data(spark,spark_dataframe):#csv_file):
    dirty_data = spark_dataframe

    #Step 1. Remove duplicate, and missing values 
    no_duplicates = dirty_data.dropDuplicates()
    transformed_data = no_duplicates.na.drop()

    #Step 2. Remove outliers, incorrect data types/ranges
    heart_data_cleaned = transformed_data.filter(
    (col('Age').cast("int").isNotNull()) &
    (col('Age').between(18,130)) &
    (col('Sex').isin('M','F')) &
    (col('ChestPainType').isin('TA', 'ATA', 'NAP', 'ASY')) &
    (col('RestingBP').between(80,250)) &
    (col('Cholesterol').between(100,1000)) &
    (col('FastingBS').between(0,1)) &
    (col('RestingECG').isin('Normal', 'ST', 'LVH')) & 
    (col('MaxHR').between(60,202)) & 
    (col('ExerciseAngina').isin('Y','N')) &
    (col('Oldpeak').between(-2.6,6.2)) &
    (col('ST_Slope').isin('Up','Flat','Down')) &
    (col('HeartDisease').between(0,1))
    )
    return heart_data_cleaned


def etl():
    spark = SparkSession.builder \
    .appName("heart_etl") \
    .getOrCreate()
    
#Step 1: Fetch a batch csv file, clean the csv file and write as parquet
    #Fetch_csv() iterates through data directory and returns a panda dataframe of first .csv file
    pyspark_dataframe, filename = fetch_csv(spark)

    if pyspark_dataframe: #fetch_csv returns non null value
        final_data = clean_data(spark, pyspark_dataframe)

    #Format current date and time for naming the files
        current_datetime = datetime.datetime.now()
        formatted_datetime = current_datetime.strftime("%Y_%m_%d_%H%M%S")
    #Write cleaned csv file as .parquet to data\parquets
        final_data.write.parquet(os.path.join(parquet_directory,'heart_data{}'.format(formatted_datetime)))
    #Move the current batch csv file to the batches\processed directory so it doesn't get processed in the future
        old_dir = os.path.join(batches_directory,filename)
        new_dir = os.path.join(processed_csv_dir,filename)
        os.rename(old_dir,new_dir)
    else:
        print('ERROR: No data files available')
    spark.stop()

#etl()
def test():
    print('Hello')