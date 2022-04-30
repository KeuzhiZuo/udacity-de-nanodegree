# Data Engineering Capstone Project
#### Project Summary
This project builds etl pipelines that extract, transform and load data from different data sources, clean and organize as a database for data analysis purpose and future usage.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Scope 
Combine the knowledge from this data engineer course, and apply them into this project. This project includes i94 immigration data, world temperature data, and US demographic data. I will setup a data warehouse with fact and dimension tables that can store all the data.

#### Data Sets
| Data Set | Format | Description |
|---|---|---|
|[I94 Immigration Data](https://travel.trade.gov/research/reports/i94/historical/2016.html)| SAS | This data set is from US gov, and reccontains international visitor arrival information.|
|[World Temperature Data](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)| CSV | This is a Kaggle data set that contains monthly average temperature data at different country in the world wide.|
|[U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)| CSV | This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.|
|[Airport Code Table](https://datahub.io/core/airport-codes#data)| Not Used Here | This dataset contains information about the airport of all US cities.|

### Step 2: Explore and Assess the Data
#### 2.1 Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc. Please refer to [capstone_project_test.ipynb] for more details.

#### 2.2 Cleaning Steps
1. Format SAS time to pandas datetime
2. Parse description and generate the dimension table
3. Tranform city, state to upper case to match city _code and state _code table

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
I use star schema to map out the conceptual data model. 
| f_immigration  | 
|----------------|
| immigration_id |
| cic_id         |
| year           |
| month          |
| city_code      |
| state_code     |
| mode           |
| visa           |
| arrive_date    |
| departure_date |
| country        |

| dim_imm_personal | 
|------------------|
| imm_person_id    |
| cic_id           |
| citizen_country  |
| residence_country|
| birth_year       |
| gender           |
| ins_num          |

| dim_imm_airline  | 
|------------------|
| imm_airline_id   |
| cic_id           |
| airline          |
| admin_num        |
| flight_nubmer    |
| visa_type        |

| dim_temperature     | 
|---------------------|
| dt                  |
| city_code           |
| country             |
| avg_temp            |
| avg_temp_uncertainty|
| year                |
| month               |

| dim_demo_pop   | 
|----------------|
| demo_pop_id    |
| city_code      |
| state_code     |
| male_population|
| female_population|
| num_veterans    |
| foregin_born |
| race        |

| dim_demo_stats   | 
|----------------|
| demo_stat_id    |
| city_code      |
| state_code     |
| median_age|
| avg_household_size|

#### 3.2 Mapping Out Data Pipelines
1. Assume all data sets are stored in S3 buckets as below: (The etl.py is based on this, but the tests in [capstone_project_test.ipynb] used the sample data in the folder)
- [S3_Bucket]/immigration/18-83510-I94-Data-2016/*.sas7bdat
- [S3_Bucket]/I94_SAS_Labels_Descriptions.SAS
- [S3_Bucket]/temperature/GlobalLandTemperaturesByCity.csv
- [S3_Bucket]/demography/us-cities-demographics.csv
2. Cleaning step to clean up data sets, deduplicate, deal with missing values
3. Transform immigration data to fact and dimension tables, temperature data to dimension table, and parse description to tables, and split demography data to dimension tables
4. Store these tables back to target S3 bucket

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Please refer to sample code in  [capstone_project_test.ipynb] Step 1,2.
#### 4.2 Data Quality Check
Please refer to sample code in  [capstone_project_test.ipynb] Step 4.
#### 4.3 Data Dictionary 
fact_immigraions:
|-- cicid: id from sas file
|-- entry_year: 4 digit year
|-- entry_month: numeric month
|-- origin_country_code: i94 country code as per SAS Labels Descriptions file
|-- port_code: i94port code as per SAS Labels Descriptions file
|-- arrival_date: date of arrival in U.S.
|-- travel_mode_code: code for travel mode of arrival as per SAS Labels Descriptions file
|-- us_state_code: two letter U.S. state code
|-- departure_date: departure date from U.S.
|-- age: age of the immigrant
|-- visa_category_code: visa category code as per SAS Labels Descriptions file
|-- occupation: occupation of immigrant
|-- gender: gender of immigrant
|-- birth_year: birth year of immigrant
|-- entry_date: Date to which admitted to U.S. (allowed to stay until)
|-- airline: airline code used to arrive in U.S.
|-- admission_number: admission number
|-- flight_number: flight number
|-- visa_type: visa type

dim_city_demographics:
|-- port_code: i94port code
|-- city: U.S. city name
|-- state_code: two letter U.S. sate code
|-- male_population: total male population
|-- female_population: total female population
|-- total_population: total population
|-- number_of_veterans: number of veterans
|-- num_foreign_born: number of foreign born

### Step 5: Complete Project Write Up(README)
#### Tools
1. AWS S3 bucket: data storage
2. AWS Redshift: data warehouse and data analysis
3. Python:
    * Pandas - exploratory data analysis on small data set
    * PySpark - data processing on large data set
    * execute the etl pipelines

#### Data Update Frequency
1. Tables generated from immigration and temperature data set update frequency should be monthly since the raw data set is monthly.
2. Government usually collect demographic data annually or every two or three years because it's the most cost efficient frequency. Therefore tables generated from demography data set should be updated annually.

#### User Scenario 
1. Users: 
    - a. US government who is in charge of controling the immigration 
    - b. People from other countries that wants to live in the US, can refer to this data to see if it's a good choice 
2. Questions these data models can answer 
    - a. General descriptive questions like, which city has the most , female and male immigrants ratio
    - b. Research/Deep dive questions like, among all the immigrants, how many people got the citzenship and how many are still residents, so that it can provide some strategies for US government to control the immigration process and policies. 

#### Future Design Considerations
**If the data was increased by 100x.** 
- AWS EMR can processe large data sets on cloud.

**If the data populates a dashboard that must be updated on a daily basis by 7am every day.**
- We can use airflow to schedule the daily 7am run. 

**If the database needed to be accessed by 100+ people.**
- AWS Redshift can handle up to 500 connections. We can use Redshift with confidence to handle this request. 

#### Note
The capstone_project_test.ipynb used the sample data in this folder to show the whole EDA, etl process, and data quality check. The etl.py could not run if the AWS cluster and S3 config is not set up, but it used the same concept as the capston_project_test.ipynb. 