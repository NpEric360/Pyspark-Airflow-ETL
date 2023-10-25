"""
This is also a set-up script to introduce the following contaminants in the dataset:
1. Outliers
2. Duplicate Rows
3. Incorrect data types
This script also splits up the original dataset into 10 batches purely for future testing of Airflow DAG
"""

import pandas as pd
import random
import math
import numpy as np
import os

#Operation 1: delete values randomly
def missing_values(dataframe):
    num_rows,num_cols = dataframe.shape
    num_delete = math.floor(0.001*num_rows)

    for col in dataframe.columns:
        for _ in range(num_delete):
            rand_index = random.randint(0,num_rows)
            dataframe[col][rand_index] = None
    return dataframe

#Operation 2: Introduce Outliers and an incorrect data type

def outlier_incorrect_datatype(dataframe):
    num_rows,num_cols = dataframe.shape
    num_outliers = math.floor(0.01*num_rows/num_cols)
    for col in dataframe.columns:
        #if this column is a float or int, create outliers by multilying by 100
        if isinstance(dataframe[col][0],np.float64) or isinstance(dataframe[col][0],np.int64): 
            rand_index = random.randint(1,num_rows-1)
            for _ in range(num_outliers):
                rand_index = random.randint(1,num_rows-1)
                dataframe[col][rand_index] *= 100       
            dataframe[col][rand_index-1] = 'test'
    return dataframe

#Operation 3: add duplicate rows to current batch

def add_duplicate_rows(dataframe):
    num_duplicates = math.floor(0.01*len(dataframe))
    duplicate_rows = dataframe.sample(num_duplicates)
    dirty_data = pd.concat([dataframe,duplicate_rows], ignore_index=True)
    return dirty_data


####

def scramble_data():

    data_directory = os.path.join(os.getcwd(),data)
    batches_directory = os.path.join(data_directory,'batches')
    input_data_path = os.path.join(data_directory,'heart_data.csv')

    clean_data = pd.read_csv(input_data_path)
    print(type(clean_data))
    print(clean_data.empty)
    if not clean_data.empty:
        #A. perform Operations 1,2 on the entire dataset first.
        transform_1 = missing_values(clean_data)
        transform_2 = outlier_incorrect_datatype(transform_1)
        
        #B. split up the dataset into 10 batches (for testing DAG only.)
        #testing  # of rows in each batch

        start_idx, end_idx = 0, 0
        num_files = 10
        batch_size = math.floor(len(transform_2)/num_files)

        #Iterate through number of files and assign a slice of the original dataframe to each 
        for i in range(num_files-1):
            start_idx = i * batch_size 
            if i < num_files - 1:
                end_idx = (i+1) * batch_size
            else:
                end_idx = len(transform_2)
            current_data = transform_2[start_idx:end_idx]
            
            #add duplicate rows to this current batch
            current_data_split = add_duplicate_rows(current_data)

            current_data_split.to_csv(os.path.join(batches_directory,'heart_dirty_dataset_{}.csv'.format(i)), index = False)
            
        #If there are remaining rows add them to the last file
        remaining_data = transform_2[end_idx:]
        remaining_data_split = add_duplicate_rows(remaining_data)
        remaining_data_split.to_csv(os.path.join(batches_directory,'heart_dirty_dataset_{}.csv'.format(i)), index = False)
    else:
        print('Input data source is empty.')



#scramble_data()