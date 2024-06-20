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
- **Data Preprocessing**: Creates new features, scales data, encodes categorical variables, and selects relevant features.
- **Model Building & Evaluation**: Implements Linear Regression and XGBoost Regressor models for price prediction.

### polish-housing-price-modeling-workshop.ipynb (in progress)

A Jupyter Notebook aiming to automate essential aspects of **Data Preprocessing** and **Model Building & Evaluation** steps, currently a work in progress.

#### Additional Files: 

The following files are available in the **data.tar.gz** archive:
- **offers_data.txt**: Contains detailed information about individual housing offers.
- **cities_data.txt**: Provides data regarding various cities and their attributes.
