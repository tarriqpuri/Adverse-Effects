'''
MAIN.PY: Read 800 JSON files from S3 bucket, clean data, transform into
Patients, Drugs, and Reactions tables, write to PostgreSQL
'''

from main_functions import *

import os
os.environ["PYSPARK_PYTHON"] = "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"

if __name__ == "__main__":

        #INITIALIZE SPARK SESSION
        spark = SparkSession\
                .builder\
                .appName("s3 to spark to postgres")\
                .getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel("ERROR")

        #POSTGRES CREDENTIALS
        POSTGRESQL_URL = "jdbc:postgresql://ec2-34-229-140-20.compute-1.amazonaws.com:5431/test"
        POSTGRESQL_TABLE = "substance"
        POSTGRESQL_USER = ""
        POSTGRESQL_PASSWORD = ""


        #SET PARAMETERS FOR SPARK JOB
        num = 60
        spark.conf.set("spark.sql.shuffle.partitions", num)
        spark.conf.set("spark.sql.files.maxPartitionBytes", 16777216)

        for x in range(4,21):
                if x<10:
                        year = '200' + str(x)
                else:
                        year = '20' + str(x)

                #S3 BUCKET
                s3url="s3a://tarriq-spark-test/adverse-effects/*"
                
                #READ DATAFRAME
                df = spark.read.option("multiLine","true").json(s3url)


                #CREATE TABLES
                patient_df = create_patient_df(df)
                bad_df, drug_df = create_drug_df(df)
                reaction_df = create_reaction_df(df)
                
                #WRITE TO POSTGRES DATABASE
                if year == '2004':                        
                    postgres_write(patient_df, drug_df, reaction_df, bad_df, \
                        POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PASSWORD)
                else:
                     postgres_append(patient_df, drug_df, reaction_df, bad_df, \
                         POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PASSWORD)
        spark.stop()







