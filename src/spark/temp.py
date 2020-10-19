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
                



                #DRUG DF
                drug_df=df.withColumn("results",sf.explode(sf.col("results"))).select("results.safetyreportid", \
                "results.patient.drug")
                drug_df=drug_df.withColumn("drug",sf.explode(sf.col("drug")))
                drug_df=drug_df.select(flatten(drug_df.schema))
                #DETERMINE BAD CASES FROM MANUFACTUERER, ROUTE AND PRODUCT TYPE COLUMNS
                #values for array sizes determine by sensitivity analysis
                bad_df = drug_df.where( (sf.size(sf.col("product_type")) >= 5 ) | (sf.size(sf.col("manufacturer_name")) >= 5) | (sf.size(sf.col("route")) >= 2) ).select("safetyreportid")

                #AVOID ARRAYS, CONVERT TO STRING
                drug_df=drug_df.withColumn("generic_name",drug_df["generic_name"].getItem(0))\
                .withColumn("application_number",drug_df["application_number"].getItem(0))\
                .withColumn("brand_name",drug_df["brand_name"].getItem(0))\
                .withColumn("manufacturer_name",drug_df["manufacturer_name"].getItem(0))\
                .withColumn("nui",drug_df["nui"].getItem(0))\
                .withColumn("package_ndc",drug_df["package_ndc"].getItem(0))\
                .withColumn("pharm_class_cs",drug_df["pharm_class_cs"].getItem(0))\
                .withColumn("pharm_class_epc",drug_df["pharm_class_epc"].getItem(0))\
                .withColumn("pharm_class_moa",drug_df["pharm_class_moa"].getItem(0))\
                .withColumn("product_ndc",drug_df["product_ndc"].getItem(0))\
                .withColumn("product_type",drug_df["product_type"].getItem(0))\
                .withColumn("route",drug_df["route"].getItem(0))\
                .withColumn("rxcui",drug_df["rxcui"].getItem(0))\
                .withColumn("spl_id",drug_df["spl_id"].getItem(0))\
                .withColumn("spl_set_id",drug_df["spl_set_id"].getItem(0))\
                .withColumn("substance_name",drug_df["substance_name"].getItem(0))\
                .withColumn("unii",drug_df["unii"].getItem(0))
                window = Window.partitionBy(drug_df['safetyreportid']).orderBy(drug_df["brand_name"])
                drug_df=drug_df.select('*', rank().over(window).alias('index'))
                drug_df=drug_df.withColumn("drug-key",sf.concat(sf.col('safetyreportid'),sf.lit('-'),sf.col('index')))
                if 'drugrecurrence' in drug_df.columns:
                        drug_df = drug_df.drop('drugrecurrence')
                drug_df = drug_df.withColumn("drugindication", sf.when(drug_df.drugindication.contains('UNK'),None).otherwise(sf.col('drugindication')))




                #Reaction DF
                reaction_df=df.withColumn("results",sf.explode(sf.col("results"))).select("results.safetyreportid", \
                "results.patient.reaction")
                reaction_df=reaction_df.withColumn("reaction",sf.explode(sf.col("reaction")))
                reaction_df=reaction_df.select(flatten(reaction_df.schema))
                window = Window.partitionBy(reaction_df['safetyreportid']).orderBy(reaction_df["reactionmeddrapt"])
                reaction_df=reaction_df.select('*', rank().over(window).alias('index'))
                reaction_df=reaction_df.withColumn("reaction-key",sf.concat(sf.col('safetyreportid'),sf.lit('-'),sf.col('index')))
                

                if year == '2004':

                        #WRITE TO POSTGRES DATABASE
                        patient_df.write \
                                .format("jdbc") \
                                .option("url", POSTGRESQL_URL) \
                                .option("dbtable", "patients") \
                                .option("user", POSTGRESQL_USER) \
                                .option("password", POSTGRESQL_PASSWORD) \
                                .option("driver","org.postgresql.Driver")\
                                .mode("overwrite") \
                                .save()

                        drug_df.write \
                                .format("jdbc") \
                                .option("url", POSTGRESQL_URL) \
                                .option("dbtable", "drugs") \
                                .option("user", POSTGRESQL_USER) \
                                .option("password", POSTGRESQL_PASSWORD) \
                                .option("driver","org.postgresql.Driver")\
                                .mode("overwrite") \
                                .save()

                        reaction_df.write \
                                .format("jdbc") \
                                .option("url", POSTGRESQL_URL) \
                                .option("dbtable", "reactions") \
                                .option("user", POSTGRESQL_USER) \
                                .option("password", POSTGRESQL_PASSWORD) \
                                .option("driver","org.postgresql.Driver")\
                                .mode("overwrite") \
                                .save()

                        bad_df.write \
                                .format("jdbc") \
                                .option("url", POSTGRESQL_URL) \
                                .option("dbtable", "badCases") \
                                .option("user", POSTGRESQL_USER) \
                                .option("password", POSTGRESQL_PASSWORD) \
                                .option("driver","org.postgresql.Driver")\
                                .mode("overwrite") \
                                .save()


                else:
                        patient_df.write \
                                .format("jdbc") \
                                .option("url", POSTGRESQL_URL) \
                                .option("dbtable", "patients") \
                                .option("user", POSTGRESQL_USER) \
                                .option("password", POSTGRESQL_PASSWORD) \
                                .option("driver","org.postgresql.Driver")\
                                .mode("append") \
                                .save()
                        print("first save")
                        drug_df.write \
                                .format("jdbc") \
                                .option("url", POSTGRESQL_URL) \
                                .option("dbtable", "drugs") \
                                .option("user", POSTGRESQL_USER) \
                                .option("password", POSTGRESQL_PASSWORD) \
                                .option("driver","org.postgresql.Driver")\
                                .mode("append") \
                                .save()
                        print("snd save")
                        reaction_df.write \
                                .format("jdbc") \
                                .option("url", POSTGRESQL_URL) \
                                .option("dbtable", "reactions") \
                                .option("user", POSTGRESQL_USER) \
                                .option("password", POSTGRESQL_PASSWORD) \
                                .option("driver","org.postgresql.Driver")\
                                .mode("append") \
                                .save()
                        print("fnl save")
                        bad_df.write \
                                .format("jdbc") \
                                .option("url", POSTGRESQL_URL) \
                                .option("dbtable", "badCases") \
                                .option("user", POSTGRESQL_USER) \
                                .option("password", POSTGRESQL_PASSWORD) \
                                .option("driver","org.postgresql.Driver")\
                                .mode("append") \
                                .save()

        spark.stop()







