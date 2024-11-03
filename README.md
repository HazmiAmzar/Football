# Football Statistics ETL Pipeline
This repo is an ETL (Extract, Transform, Load) pipeline that retrieves football statistics for English Premier League teams using the Football API. The project extracts data from the API, transforms it into a structured format, saves it to a JSON file, and loads it into a MongoDB database.

## Project Overview
The purpose of this project is to showcase an ETL process using Python to handle data from an external API. The project includes error handling for database connections, environment configuration for sensitive data, and data transformation for easy analysis.

## Features
* Data Extraction: Retrieves English Premier League team statistics from the Football API using a specified list of team IDs.
* Data Transformation: Processes raw API data into a structured format using Pandas.
* Data Storage: Saves the structured data to both a JSON file (epl.json) and a MongoDB collection.
