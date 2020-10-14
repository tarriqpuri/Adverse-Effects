# Adverse Effects
### Improving Patient Outcomes for Prescribed Medication

The presentation slides can be found [here](https://docs.google.com/presentation/d/1s17D2VxaNppbjIhvesi7fQ0S1EsHI8kzfqqyxD12h7w/edit#slide=id.g97d56aba72_0_119).

<hr/>

## Introduction

In the United States, it is estimated that 100 000 people pass away annually due to adverse drug reactions. The goal of the project is to enable a team of data scientists to develop machine learning algorithms for predicting death in a patient prior to the administration of a drug with the aim of saving lives. To this end, the data can be difficult to obtain and understand. This project provides (1) a data pipeline that gives the data scientist access to the data and (2) a data exploration tool that enables feature engineering. 

## Architecture

## Dataset

The data was taken from an open source API called OpenFDA. This contains 800 files with roughly 100 GB of data. Each file contains information about the patient (weight, age, etc.), the drugs they were given (brand name, dosage, route, etc.), and the reactions they experienced (headache, nausea, etc.). The data is stored as a complex nested JSON file, where there are multiple structures in arrays in structures. This can be seen in the image below. 

<p align="center">
<img src = "images/JSON.jpg" width="800" class="center">
</p>

## Engineering challenges

## Trade-offs

<hr/>

## How to install and get it up and running