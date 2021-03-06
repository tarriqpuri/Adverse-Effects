'''
SPARK-FUNCTIONS.PY: Contains functions for cleaning and transforming
spark dataframe in main code
'''

import sys
import os
from operator import add

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank




def dynamic_date(col, frmts=("yyyyMMdd", "yyyyMM")):
    return sf.coalesce(*[sf.to_date(col, i) for i in frmts])


def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType

        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)

    return fields




def create_patient_df(df):
    #grab all relevant columns 
    patient_df = df.withColumn("results",sf.explode(sf.col("results"))).select("results.*","results.patient.*")
    #drop patient column as it already exists as results.patient.*
    patient_df = patient_df.drop("patient")
    #drop drug and reaction columns as they are not included in patient table
    patient_df = patient_df.drop("drug")
    patient_df = patient_df.drop("reaction")
    #flatten all structs in schema into seperate columns
    patient_df = patient_df.select(flatten(patient_df.schema))
    #format various date columns (yyyymmdd OR yyyymm AS yyyy/mm/dd)
    patient_df = patient_df.withColumn('receiptdate',dynamic_date(patient_df['receiptdate']))
    patient_df = patient_df.withColumn('receivedate',dynamic_date(patient_df['receivedate']))
    patient_df = patient_df.withColumn('transmissiondate',dynamic_date(patient_df['transmissiondate']))
    return patient_df


def create_drug_df(df):
    #grab relevant columns (safetyreportid and drug related information)
    drug_df=df.withColumn("results",sf.explode(sf.col("results"))).select("results.safetyreportid", \
    "results.patient.drug")
    #explode and flatten nested array
    drug_df=drug_df.withColumn("drug",sf.explode(sf.col("drug")))
    drug_df=drug_df.select(flatten(drug_df.schema))
    #determine bad cases from manufacturer, route and product type columns
    #values for array sizes determine by sensitivity analysis
    bad_df = drug_df.where( (sf.size(sf.col("product_type")) >= 5 ) | (sf.size(sf.col("manufacturer_name")) >= 5) | (sf.size(sf.col("route")) >= 2) ).select("safetyreportid")

    #avoid arrays, convert to string
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
    #create a drug-key (safetyreportid-index) where index represents the nth drug for a case
    window = Window.partitionBy(drug_df['safetyreportid']).orderBy(drug_df["brand_name"])
    drug_df=drug_df.select('*', rank().over(window).alias('index'))
    drug_df=drug_df.withColumn("drug-key",sf.concat(sf.col('safetyreportid'),sf.lit('-'),sf.col('index')))
    if 'drugrecurrence' in drug_df.columns:
            drug_df = drug_df.drop('drugrecurrence')
    #make nulls and unknowns equivalent
    drug_df = drug_df.withColumn("drugindication", sf.when(drug_df.drugindication.contains('UNK'),None).otherwise(sf.col('drugindication')))

    return (bad_df, drug_df)



def create_reaction_df (df):
    #grab relevant columns (safetyreportid and reaction related information)
    reaction_df=df.withColumn("results",sf.explode(sf.col("results"))).select("results.safetyreportid", \
    "results.patient.reaction")
    #explode reaction column
    reaction_df=reaction_df.withColumn("reaction",sf.explode(sf.col("reaction")))
    #flatten nested struct of reaction columns
    reaction_df=reaction_df.select(flatten(reaction_df.schema))
    #create key for reaction table (as in drug table, the structure is safetyreportid-index)
    window = Window.partitionBy(reaction_df['safetyreportid']).orderBy(reaction_df["reactionmeddrapt"])
    reaction_df=reaction_df.select('*', rank().over(window).alias('index'))
    reaction_df=reaction_df.withColumn("reaction-key",sf.concat(sf.col('safetyreportid'),sf.lit('-'),sf.col('index')))
    return reaction_df

def postgres_write(patient_df, drug_df, reaction_df, bad_df, POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PASSWORD):
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

def postgres_append(patient_df, drug_df, reaction_df, bad_df, POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PASSWORD):
    patient_df.write \
            .format("jdbc") \
            .option("url", POSTGRESQL_URL) \
            .option("dbtable", "patients") \
            .option("user", POSTGRESQL_USER) \
            .option("password", POSTGRESQL_PASSWORD) \
            .option("driver","org.postgresql.Driver")\
            .mode("append") \
            .save()

    drug_df.write \
            .format("jdbc") \
            .option("url", POSTGRESQL_URL) \
            .option("dbtable", "drugs") \
            .option("user", POSTGRESQL_USER) \
            .option("password", POSTGRESQL_PASSWORD) \
            .option("driver","org.postgresql.Driver")\
            .mode("append") \
            .save()

    reaction_df.write \
            .format("jdbc") \
            .option("url", POSTGRESQL_URL) \
            .option("dbtable", "reactions") \
            .option("user", POSTGRESQL_USER) \
            .option("password", POSTGRESQL_PASSWORD) \
            .option("driver","org.postgresql.Driver")\
            .mode("append") \
            .save()

    bad_df.write \
            .format("jdbc") \
            .option("url", POSTGRESQL_URL) \
            .option("dbtable", "badCases") \
            .option("user", POSTGRESQL_USER) \
            .option("password", POSTGRESQL_PASSWORD) \
            .option("driver","org.postgresql.Driver")\
            .mode("append") \
            .save()
