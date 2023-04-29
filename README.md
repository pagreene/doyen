# Doyen

A tool for finding experts using publicly available data sources such as pubmed. This
project uses ElasticSearch to store article data from [pubmed](https://pubmed.ncbi.nlm.nih.gov/), and
uses it to rapidly find and rank experts with specific expertise.

## Setting up ElasticSearch

First, you will want to ingest the data from the [PubMed FTP server](https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/).
This involves setting up an ElasticSearch instance, and then running the ingestion pipeline. You can find
the instructions for setting up ElasticSearch [here](./ingestion_pipeline/README.md).

## Next you will want to set up the REST API 

TBD

## Finally, you will want to set up the Web App

The front end client is implemented in another repository, which you can
find [here](https://github.com/DoyenTeam/doyenclient).
