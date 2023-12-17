# Polish Housing Price Modeling
This project focuses on creating a machine learning model to predict housing prices in Poland using data scraped from www.otodom.pl. 

It contains two main components:

### download_data.py

Responsible for scraping data from the website.
Features asynchronous methods to collect URLs and details of housing offers.
Segregates data into files based on administrative divisions.
### polish-housing-price-modeling.ipynb

A Jupyter Notebook that performs a comprehensive analysis and modeling of the housing data.
Divided into several sections:
- **Data Collection**: Downloads and merges data into one file.
- **Data Cleaning & Preparation**: Handles missing values and prepares data for analysis.
- **Exploratory Data Analysis (EDA)**: Examines distributions, outliers, and correlations.
- **Feature Engineering**: Creates new features, scales data, encodes categorical variables, and selects relevant features.
- **Model Building & Evaluation**: Implements Linear Regression and XGBoost Regressor models for price prediction.
