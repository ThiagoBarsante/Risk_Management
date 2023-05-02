"""
This module uses Spark to automate the process of reading raw data and generating a fraction
of it (suppose 1%) in the format of a Parquet file with loan contract_id info.

The purpose of this program is to provide real data to simulate the entire execution of the
Credit risk modeling data pipeline.

Usage:
- First, configure the SparkSession to connect to your Spark cluster
- Then, provide the input data source path, the fraction of the data to sample,
    and the output path for the Parquet file.
- Finally, run the program to generate this new parquet file

Example:
    from pyspark.sql import SparkSession
    ...

Users can also run this program by passing parameters as defined in the fnc_validate_parameters()
function, which validates and extracts the necessary input parameters from the command
line arguments.

"""
import sys
import datetime
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


### Initial setup and Spark functions
pd.options.display.max_columns = None

## True for EMR Serverless executin or False for EMR Cluster or Local Spark execution
EMR_SERVERLESS_EXECUTION = False
S3_BUCKET_NAME = '../../emr_serverless_demo'


def fnc_validate_parameters(aws_emr_serverless_execution=EMR_SERVERLESS_EXECUTION):
    """
    This function validate the parameters to be used with EMR Serverless

    Parameters
    ----------
    aws_emr_serverless_execution : TYPE: boolean, optional  - EMR Cluster or localhost DEBUG
        DESCRIPTION. The default is EMR_SERVERLESS_EXECUTION = True.

    Returns
    -------
    Input, Output path passed as parameters or stop the process with an error

    """
    if (len(sys.argv) != 3) and aws_emr_serverless_execution:
        print("Usage: spark-etl ['input folder'] ['output folder'] ['rpt_folder']")
        sys.exit(-1)

    if not aws_emr_serverless_execution: ## EMR Cluster or local execution
        input_location_aux = S3_BUCKET_NAME + '/s3_data/input'
        output_location_aux = S3_BUCKET_NAME + '/s3_data/output/'
    else: # EMR Serverless parameters sample
        # ['../s3_data/input/', '../s3_data/output/', '../s3_data/rpt/']
        input_location_aux = sys.argv[1]
        output_location_aux = sys.argv[2]

    # ## Debug INFO
    print('Input location: ', input_location_aux)
    print('Output location: ', output_location_aux)

    return input_location_aux, output_location_aux

def fnc_print_datetime(msg='Default - msg'):
    """
    Function to get current date and time and print a msg

    Parameters
    ----------
    msg : TYPE, string
        DESCRIPTION. The default is 'Default - msg'.

    Returns
    -------
    None.

    """
    dt_format = datetime.datetime.now()
    formatted_date = dt_format.strftime('%Y-%m-%d %H:%M:%S')
    print(formatted_date, msg)

def fnc_filename_timestap(filename_str):
    """
    This function will generate a new filename and append a timestamp at the end

    Parameters
    ----------
    filename : string
        Original filename to be read.

    Returns
    -------
    str - filename with timestamp.

    """
	# Get the current date and time
    now = datetime.datetime.now()

	# Format the date and time as a string
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")

    return f"{filename_str}_WITH_CONTRACT_ID_{timestamp}.parquet"

def fshape(dataframe1):
    """
    Wrapper function to print the shape of a Spark dataframe
    The number of rows and columns

    Parameters
    ----------
    dataframe1 : Spark dataframe

    """
    print('Shape : ', dataframe1.count(), len(dataframe1.columns))

def fhead(dataframe1, num_records=3):
    """
    The fhead function will print first header rows of a Spark Dataframe

    Parameters
    ----------
    dataframe1 : Spark dataframe
    num_records : int, optional
        DESCRIPTION. The default is 3.

    Returns
    -------
    Pandas dataframe for print option

    """
    return dataframe1.limit(num_records).toPandas()

def fsummary(dataframe1):
    """
    This function will generate Summary information about the spark dataframe
    as a pandas dataframe

    Parameters
    ----------
    dataframe1 : Spark dataframe

    Returns
    -------
    Pandas dataframe


    """
    return dataframe1.summary().toPandas()

def fnc_contract_id_generation(sdf_filename, filetype='parquet', fraction_sample=0.15):
    """
    The wrapper function fnc_contract_id_generation will read a parquet filename
    using spark and is going to generate a unique identifier for each record
    with column name contract_id.
        It simulates the index example of a pandas dataframe

    A fraction of the original parquet file will be exported at the end of
    execution

    Parameters
    ----------
    parquet_filename : str
        DESCRIPTION. Parquet file name to be processed
    fraction_sample : TYPE, optional
        DESCRIPTION. The default is 0.15. - 15% of records from original filename
    file_type : str
        csv to read csv file format or
        parquet to read parquet file format

    Returns
    -------
    sdf : Spark Dataframe
        Return the new spark dataframe with unique identifier (contract_id).

    """

    if filetype == 'csv':
        sdf = spark.read.csv(sdf_filename, header=True, inferSchema=True)
    elif filetype == 'parquet':
        sdf = spark.read.parquet(sdf_filename)
    else:
        print(' Error in file type format : ', filetype)
        sys.exit(-1)

    sdf = sdf.sample(fraction=fraction_sample, seed=42) ## default franction of 0.01

    cols = sorted(set(sdf.columns))
    cols.insert(0, 'contract_id')

    # add a unique index column - contract id
    sdf = sdf.withColumn("contract_id", monotonically_increasing_id())
    sdf = sdf[cols]

    fshape(sdf)

    return sdf

## --------------   Main Program

fnc_print_datetime(msg=' - Program started  ... ')

(input_location, output_location) = fnc_validate_parameters()

## Spark cluster - local environment - parquet file sample
# input_location = '/tmp/Credit_Risk_Modeling/data_s3/loan_data_2015_2018.parquet'

parquet_file = input_location

print ('Parquet file generated with Spark - ' , fnc_filename_timestap(parquet_file))

## default Spark appName - Spark3-app local execution
spark = SparkSession.builder.appName('Spark3-ML-quick-app').master('local[*]').getOrCreate()
sc = spark.sparkContext
# spark

# Read raw data and generate new Spark dataframe with contract id for 1% of the original records
sdf_with_contract_id = fnc_contract_id_generation(parquet_file, fraction_sample=0.01)

export_filename = fnc_filename_timestap(parquet_file)

## Export as one Parquet file
sdf_with_contract_id.coalesce(1).write.mode("overwrite").parquet(export_filename)

print('**** ')

fnc_print_datetime(msg=' Done! ')
