# Social Media Analysis

## Project Description

This project focuses on analyzing social media data, particularly tweets, to help users understand trends. It uses Apache Airflow for data orchestration and PostgreSQL for data storage and analysis.

## How It Works

- **Data Collection**: Tweets are collected and stored in a PostgreSQL datalake as raw JSON data.
- **Data Processing**: The raw data is processed to extract key information such as the tweet's author, sentiment, topics, and hashtags.
- **Data Storage**: Processed data is stored in structured tables in a PostgreSQL data warehouse for analysis.

## Analysis Features

- Count tweets by hashtags.
- Analyze topics mentioned in tweets.
- Track the number of tweets per user.

## Installation

To run this project, you will need:
```bash
- Apache Airflow
- PostgreSQL
- Python (with psycopg2)
