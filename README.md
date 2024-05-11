# Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

# Actions

## Load data S3 and copy to staging based on resources be provided
* Connect account AWS (Udacity)
    * Create User with AdministratorAccess
    * Create Key|Secret

* Open and run launch_clound.ipynb
    * Create file config with Key|Secret
    * Run step by step command liné to:
        * Create S3, Role, Redshift ...
        * Load and copy data
        * Public database
        * Check status redshift
        * Delete redshift/role when DONE

## Design database

Using the song and event datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

* Fact Table
    * songplays [songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent]

* Dimension Tables
    * users [user_id, first_name, last_name, gender, level]
    * songs [song_id, title, artist_id, year, duration]
    * artists [artist_id, name, location, latitude, longitude]
    * time [start_time, hour, day, week, month, year, weekday]

## Create SQL queries file to reference file .py running

* File sql_queries.py to use for reference to proccess create/insert/drop tables 

* File create_tables.py to implement create/insert/drop tables 

## Create .py file to processing query file above

* File etl.py to load and insert data to tables

## Create .ipynb to testing

* File etl.ipynb to testing following code in etl.py file

# IMPORTANT NOTES
Concurrent/Parallel instance launch: 
* We do not support launching multiple EC2, RDS, or Sagemaker instances unless specifically mentioned in the classroom. 
* Your AWS account will be suspended if you attempt at launching concurrent instances where they not required.

Personal Information in AWS: 
* Do not add any type of personal information to the AWS account and resources provided by Udacity for this course. 
* This includes: email addresses, resource names, tags, phone number, name, etc.

Deleting and Reusing Accounts: 
* Udacity will delete all resources in AWS accounts after six months of inactivity to ensure proper resource utilization and data security. 
* Deleting resources means that all work will be lost and will need to be redone.

