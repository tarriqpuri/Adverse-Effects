# Adverse Effects
### Improving Patient Outcomes for Prescribed Medication

The presentation slides can be found [here](https://docs.google.com/presentation/d/1s17D2VxaNppbjIhvesi7fQ0S1EsHI8kzfqqyxD12h7w/edit#slide=id.g97d56aba72_0_119).

<hr/>

## Table of Contents
- [Introduction](#introduction)
- [Architecture](#architecture)
- [Dataset](#dataset)


## Introduction

In the United States, it is estimated that 100 000 people pass away annually due to adverse drug reactions. The goal of the project is to enable a team of data scientists to develop machine learning algorithms for predicting death in a patient prior to the administration of a drug with the aim of saving lives. To this end, the data can be difficult to obtain and understand. This project provides (1) a data pipeline that gives the data scientist access to the data and (2) a data exploration tool that enables feature engineering. 

## Architecture

The data pipeline includes S3, Spark, PostgreSQL, Airflow, and Dash. S3 stores each JSON file which is obtained through the OpenFDA API. Spark is used to make sense of the nested JSON data. This includes column explodes, date formatting, quality control, and id generation. Inspired by the schema of the JSON file, the data is split into 4 tables: Patients, Drugs, Reactions and Bad Cases. These four databases are written into PostgreSQL for ease of query. Group By queries are run to organize the data for visualization by Dash. 

<p align="center">
<img src = "./Images/tech-stack.jpg" width="800" class="center">
</p>

## Dataset

The data was taken from an open source API called OpenFDA. This contains 800 files with roughly 100 GB of data. Each file contains information about the patient (weight, age, etc.), the drugs they were given (brand name, dosage, route, etc.), and the reactions they experienced (headache, nausea, etc.). The data is stored as a complex nested JSON file, where there are multiple structures in arrays in structures. This can be seen in the image below. 

<p align="center">
<img src = "./Images/JSON.jpg" width="800" class="center">
</p>

## Engineering challenges

## Trade-offs

<hr/>

## How to install and get it up and running