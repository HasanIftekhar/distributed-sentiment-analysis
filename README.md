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

- Apache Spark 3.x (PySpark)
- Apache Hadoop 3.x (4-node cluster)
- Delta Lake
- SparkNLP (requires Java 8 or 11)
- NLTK
- Word2Vec + K-Means

## Cluster Environment

This pipeline was developed and tested on a **4-node Hadoop/Spark cluster** with the following configuration:

- Spark 3.x with Delta Lake extension
- Hadoop 3.x in pseudo-distributed / fully-distributed mode
- SparkNLP requires **Java 8 or Java 11** — ensure `JAVA_HOME` is set
- Master URL passed via `createSparkSession(master_url)` — update to your cluster or use `local[*]` for single-machine testing

## Dataset

COVID-19 tweets dataset (2GB) — available on [Kaggle: Corona Virus Tweets](https://www.kaggle.com/datasets/smid80/coronavirus-covid19-tweets).

> Dataset not included due to size. Download and place CSV files in a `dataset/` folder.

## Files

- `sentimentAnalysis.py` — main PySpark pipeline (Word2Vec + K-Means clustering)
- `MapReduce.py` — MapReduce implementation for tweet parsing and word frequency
- `MapReduceCsv.py` — CSV-optimized MapReduce processing
- `convert.py` — data format conversion utilities
- `removeNewLine.py` — preprocessing utility
- `Final Report_Group03_Sentiment Analysis of Covid-19 Tweets.pdf` — full project report

## How to Run

```bash
pip install -r requirements.txt

# Submit to Spark cluster
spark-submit --packages io.delta:delta-core_2.12:2.1.0 sentimentAnalysis.py

# Or run locally (single machine)
# Update master_url to 'local[*]' in sentimentAnalysis.py first
python sentimentAnalysis.py
```

> SparkNLP requires Java 8 or 11. Set `JAVA_HOME` before running.
