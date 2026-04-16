# Distributed Sentiment Analysis Pipeline (Hadoop + Spark)

**University of Stavanger — Data Intensive Systems (DAT500)**

## Overview

Built a 4-node Hadoop/Spark distributed pipeline for unsupervised sentiment clustering of COVID-19 tweets using Word2Vec embeddings and K-Means clustering. Optimized distributed execution via data partitioning, cutting Word2Vec stage runtime from **20 minutes to 3.1 minutes** on a 2GB dataset.

## Architecture

- **Ingestion**: Multi-CSV loading across distributed nodes
- **Preprocessing**: Tokenization, stop word removal, stemming (NLTK + SparkNLP)
- **Embedding**: Word2Vec via PySpark MLlib
- **Clustering**: K-Means unsupervised sentiment grouping
- **Storage**: Delta Lake for versioned data management

## Performance

| Stage | Before Optimization | After Optimization |
|-------|--------------------|--------------------|
| Word2Vec Training | 20 minutes | 3.1 minutes |
| Dataset Size | 2GB (6 CSV files) | Same |

## Tech Stack

- Apache Spark (PySpark)
- Apache Hadoop (4-node cluster)
- Delta Lake
- SparkNLP
- NLTK
- Word2Vec + K-Means

## Dataset

COVID-19 tweets dataset (2GB) — available on [Kaggle: Corona Virus Tweets](https://www.kaggle.com/datasets/smid80/coronavirus-covid19-tweets).

> Dataset not included in this repo due to size. Download and place CSV files in a `dataset/` folder.

## Files

- `sentimentAnalysis.py` — main PySpark pipeline (Word2Vec + K-Means clustering)
- `MapReduce.py` — MapReduce implementation for word frequency
- `MapReduceCsv.py` — CSV-optimized MapReduce processing
- `convert.py` — data format conversion utilities
- `removeNewLine.py` — preprocessing utility
- `Final Report_Group03_Sentiment Analysis of Covid-19 Tweets.pdf` — full project report

## How to Run

```bash
pip install pyspark delta-spark sparknlp nltk

# Start Spark session and run pipeline
spark-submit sentimentAnalysis.py
```

> Requires a configured Hadoop/Spark cluster or local Spark session.
