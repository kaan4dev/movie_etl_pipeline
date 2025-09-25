# 🎬 Movie ETL Pipeline with BigQuery & Airflow

## 📌 Project Overview
This project demonstrates an **end-to-end data engineering pipeline** built with:
- **Python & Requests** → Extract movie data from TMDB API
- **PySpark** → Transform and clean raw data
- **BigQuery** → Store and analyze processed data
- **Airflow** → Orchestrate the pipeline
- **Pandas & Matplotlib** → Data analysis and visualization

The pipeline automates the journey of raw movie data into meaningful insights.

---

## ⚙️ Pipeline Architecture
```text
         TMDB API
            |
        (Extract.py)
            ↓
       Raw CSV Files
            ↓
   (Transform_spark.py with PySpark)
            ↓
  Processed JSON (cleaned dataset)
            ↓
   Load into BigQuery (bq load)
            ↓
  SQL Analysis + Visualization
            ↓
   Airflow DAG orchestrates entire flow

         TMDB API (3rd-party data source)
            |
     [Extract Step: extract_api.py]
       → Fetches movie metadata (title, release_date, genre_ids, popularity, vote_average, etc.)
       → Saves the raw response into CSV format
            ↓
       Raw CSV Files (data/raw/movies_tmdb.csv)
            ↓
     [Transform Step: transform_spark.py]
       → Cleans malformed values (e.g., empty strings, invalid numerics)
       → Extracts release_year from release_date
       → Explodes genre_ids array into individual rows
       → Maps original_language → full name
       → Creates derived columns:
            • rating_percent (vote_average * 10)
            • popularity_bucket (Low, Medium, High)
       → Outputs cleaned JSON dataset
            ↓
  Processed JSON (data/processed/movies_cleaned_json)
            ↓
     [Load Step: BigQuery]
       → Load data into BigQuery table `movies.cleaned_movies`
       → Schema auto-detected (rating_percent, language_full, release_year, etc.)
       → Data ready for SQL analysis
            ↓
  SQL Analysis + Visualization (Jupyter Notebook)
       → Run exploratory SQL queries (e.g., avg rating by decade, top languages)
       → Visualize results with Pandas & Matplotlib
            ↓
     [Orchestration: Airflow DAG]
       → Automates Extract → Transform → Load sequence
       → Schedules daily runs
       → Monitors success/failure of each task
