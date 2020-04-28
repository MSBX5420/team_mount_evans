# MSBX 5420-003: Unstructured and Distributed Data Modeling and Analysis
## COVID-19 Twitter Data Ingest and Analysis
## Team Mount Evans

Team Mount Evan’s objective for the project was to analyze large sets of Twitter data produced in the midst of the COVID-19 pandemic. The data analyzed stretched from the beginning of March 2020 to the beginning of April 2020. While the data came from all across the world, the team decided to analyze tweets that are in English only. The data was sourced from Kaggle.com. The team used GitHub as a centralized repository for shared data files and working code. The team also used AWS EMR for storage, code construction, and as a local cluster to test iterations of code. The methodology is described below:

## Data Distribution Outline

Step 1: Distributing Raw Data

Step 2: AWS EMR

Step 3: Final Deployment

* More information can be found in the [Design Document.](https://github.com/MSBX5420/team_mount_evans/blob/master/Design%20Document.txt) This will include a more detailed outline of the steps to load, ingest, deploy, and analyze the data as well as a code diagram of operations.

## Getting Started

### Dependencies

Utilized Python as the base language, including the following packages:
* matplotlib 
* pyspark.sql
* seaborn
* io 
* datetime
* sys
* pickle 
* statsmodels
* pandas
* numpy
* wordcloud
* textblob

Github repository used for version control & code management

## Code Links

Final Code Used for EMR Deployment
* [finalCode.py](https://github.com/MSBX5420/team_mount_evans/blob/master/code_prod/finalCode.py)
* [Final Code.ipynb](https://github.com/MSBX5420/team_mount_evans/blob/master/Final%20Code.ipynb)

The .py is a script meant to be submitted to an EMR environment and produce results on a cluster as text files and pngs, while the .ipynb still prints visuals out to console and is interactive.

Sample Data
* [20200312_Coronavirus_Tweets_Subset.CSV](https://github.com/MSBX5420/team_mount_evans/blob/master/20200312_Coronavirus_Tweets_Subset.CSV)

Testing Code

The following code links are segments developed locally prior to the final deloyment of the entire dataset. The below code  was tested on a small subset of the data and applied to the master file upon deployment.

* Sentiment analysis using TextBlob of Tweets in English [spark_data_ingest.py](https://github.com/MSBX5420/team_mount_evans/blob/master/spark_data_ingest.py)
* Added New Cleaning Code [Data Ingest and Cleaning.py](https://github.com/MSBX5420/team_mount_evans/blob/master/Data%20Ingest%20and%20Cleaning.py)
* Create Wordcloud Code [Wordcloud Code](https://github.com/MSBX5420/team_mount_evans/blob/master/Wordcloud%20Code)
* Further Data Ingesting Code [IngestingData_analysis.ipynb](https://github.com/MSBX5420/team_mount_evans/blob/master/IngestingData_analysis.ipynb)
* Added addition code to produce WordCloud [IngestingData.py](https://github.com/MSBX5420/team_mount_evans/blob/master/IngestingData.py)
* Updated Data Analysis Code [Ingestdata_analysis2.py](https://github.com/MSBX5420/team_mount_evans/blob/master/Ingestdata_analysis2.py)

### Other Documents

TO BE ADDED

## Help

Any questions may be directed to the [Design Document](https://github.com/MSBX5420/team_mount_evans/blob/master/Design%20Document.txt) or the below contributors to this project.

## Authors

* Stephen Lipsky
* Olivia Ponrick
* Nay Vichitpunt
* Caleb Gordon
* Sumner Crosby

