
# Summary 

This project aims to build an ETL pipeline for a database in Redshift, using staging tables to load the data and then do some transformation to insert the corresponding data into the fact and dimension table, using a star schema, as follows

## Fact table
Songplays table {songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent}
        
## Dimension Tables

Users -> user_id, first_name, last_name, gender, level

Songs -> song_id, title, artist_id, year, duration

Artists -> artist_id, name, location, lattitude, longitude

Time -> start_time, hour, day, week, month, year, weekday


# Installation

To set up the database on AWS, we created

- An user with administration policy, in order to be able to impersonate AWS to access the database and load the data
- A role added to the cluster using AWS console and added to dwh.cfg file
- A cluster on the same zhone as the data buckets, created using AWS

After set up the configuration using AWS console, the Python scripts should be run in the following order

python create_tables.py

python etl.py

# Files

sql_queries.py -> a script with SQL queries to drop tables, create tables and insert data into staging, fact and dimension tables.

create_tables.py -> a script that connects with the cluster database, drop tables to clean previous data and create staging, fact and dimension tables.

etl.py -> a script that copy the data into staging tables and do the transformations needed to load the selected data into fact and dimension tables.