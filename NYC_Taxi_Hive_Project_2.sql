Objective - The assignment is meant for you to apply learnings of the module on Hive on a real-life dataset. One of the major objectives of this assignment is gaining familiarity with how an analysis works in Hive and how you can gain insights from large datasets.
 
Problem Statement - New York City is a thriving metropolis and just like most other cities of similar size, one of the biggest problems its residents face is parking. The classic combination of a huge number of cars and a cramped geography is the exact recipe that leads to a large number of parking tickets.
 
In an attempt to scientifically analyse this phenomenon, the NYC Police Department regularly collects data related to parking tickets. This data is made available by NYC Open Data portal. We will try and perform some analysis on this data.

Download Dataset - https://data.cityofnewyork.us/browse?q=parking+tickets


-- Step 1 :  Create a local directory 

mkdir /tmp/nyc_dataset

-- Step 2 : Dump the NYC Taxi dataset into the local directory by using FileZilla

-- Step 3 : Create a Database

create database nyc_db;

-- Step 4 : Dump data into the HDFS 

hadoop fs -put /mkdir/nyc_taxi_hive/ParkingViolations2017.csv  /tmp/nyc_taxi_hive


-- Step 4 : Create the schema structure of the dataset

create table nyc_taxi_violation_analysis
(
summons_number integer,
plateid string,
registration_state string,
plate_type string,
issue_date string,
violation_code integer,
vehicle_body_type string,
vehicle_make string,
issuing_agency string,
street_code_one integer,
street_code_two integer,
street_code_three integer,
vehicle_expiration_date integer,
violation_location string,
violation_precinct integer,
issuer_precinct integer,
issuer_code integer,
issuer_command string,
issuer_squad string,
violation_time string,
time_first_observed string,
violation_county string,
violation_in_front_of_or_oppposite string,
hosue_number string,
street_name string,
intersecting_street string,
date_first_oibserved string,
law_section integer,
sub_division string,
violation_legal_code string,
days_parking_in_effect string,
from_hours_in_effect string,
to_hours_in_effect string,
vehicle_colour string,
unregistered_vehicle string,
vehicle_year integer,
meter_number string,
feet_from_curb integer,
violation_post_code string,
post_description string,
no_standing_or_stopping_violation string,
hydrant_violation string,
double_parking_violation string
)
row format delimited 
fields terminated by ',';


-- Step 5 : Load data into the schema structure

load data inpath '/tmp/ineuron_consultant/ParkingViolations2017.csv' into table nyc_taxi_violation_analysis;
------------------------------------------------------------------------------------------------------------------------------------------


Note: Consider only the year 2017 for analysis and not the Fiscal year.

The analysis can be divided into two parts:
 
Part-I: Examine the data

1.) Find the total number of tickets for the year.

		select count(summons_number) as 'Total tickets' from nyc_taxi_violation_analysis;


2.) Find out how many unique states the cars which got parking tickets came from.

		select COUNT(DISTINCT registration_state) as 'Registration state count' from nyc_taxi_violation_analysis;

3.) Some parking tickets don’t have addresses on them, which is cause for concern. Find out how many such tickets there are(i.e. tickets where either "Street Code 1" or "Street Code 2" or "Street Code 3" is empty )

		select count(summons_number) as 'Total tickets without address' from nyc_taxi_violation_analysis where (street_code_one or street_code_two or street_code_three) is NULL;

Part-II: Aggregation tasks

1.) How often does each violation code occur? (frequency of violation codes - find the top 5)

		select DISTINCT violation_code as 'Violation code' ,count(DISTINCT violation_code)as 'Count of violation codes' 
		from nyc_taxi_violation_analysis group by violation_code order by violation_code desc limit 5;

2.) How often does each vehicle body type get a parking ticket? How about the vehicle make? (find the top 5 for both)

		select summons_number as 'ticket number',distinct vehicle_body_type as 'vehicle body type',
		count(distinct vehicle_body_type) as 'count of vehicle body type', distinct vehicle_make as 'vehicle make' ,count(distinct vehicle_make) as 'count of vehicle make'
		from nyc_taxi_violation_analysis group by vehicle_body_type, vehicle_make order by vehicle_body_type, vehicle_make desc limit 5;

3.) A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of:

      a.) Violating Precincts (this is the precinct of the zone where the violation occurred)

		select distinct violation_location as 'violation location', count(distinct violation_location) as 'Violation Locations count', 
		distinct violation_precinct as 'Violation precinct', count(distinct violation_precinct) as 'Count of Violation Precinct' 
		from nyc_taxi_violation_analysis group by violation_location desc limit 5;

      b.) Issuer Precincts (this is the precinct that issued the ticket)

		select distinct violation_location as 'violation location', count(distinct violation_location) as 'Violation Locations count',
		distinct issuer_precinct as 'Issuer precinct', count(distinct issuer_precinct) as 'Issuer precinct count'
		from nyc_taxi_violation_analysis group by violation_location desc limit 5;

4.) Find the violation code frequency across 3 precincts which have issued the most number of tickets - do these precinct zones have an exceptionally high frequency of certain violation codes?
			
		-- Created a Dynamic partition table for violation code
		create table violation_code_frquency(
		violation_precinct integer,
		issuer_precinct integer
		)
		partition by (violation_code integer)
			
		insert overwrite table violation_code_frquency partition(violation_code) select distinct violation code as 'violation code', 
		select count(distinct violation_code) as 'count of violation codes', distinct violation_location as 'violation location',
		distinct violation_precinct as 'Violation precinct',distinct issuer_precinct as 'Issuer Precinct',
		from nyc_taxi_violation_analysis group by violation_code order by violation_code desc;
		  

		
5.) Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. For each of these groups, find the 3 most commonly occurring violations

		select violation_code, dateadd(hour, n, violation_time) violation_time, dateadd(minute, Duration_Mins, (dateadd(hour, n, violation_time))) End_Time
		FROM nyc_taxi_violation_analysis
		WHERE number >= 0
        AND number <= datediff(HOUR, t.violation_time, t.End_Time) /4
		group by violation_code order by violation_code desc limit 3;


6.) Now, try another direction. For the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part)

		
		select violation_code, dateadd(hour, n, violation_time) violation_time, dateadd(minute, Duration_Mins, (dateadd(hour, n, violation_time))) End_Time
		FROM nyc_taxi_violation_analysis
		WHERE number >= 0
        AND number <= datediff(HOUR, t.violation_time, t.End_Time) /4
		group by violation_code order by violation_code desc limit 1;

7.) Let’s try and find some seasonality in this data
      
a.) First, divide the year into some number of seasons, and find frequencies of tickets for each season. (Hint: A quick Google search reveals the following seasons in NYC: Spring(March, April, March); Summer(June, July, August); Fall(September, October, November); Winter(December, January, February))
      
		select count(DISTINCT summons_number),
			case when month in (12,1,2) and year = 2017 then 'Winter' 
			case when month in (3,4,5) and year = 2017 then 'Spring' 
			case when month in (6,7,8) and year = 2017 then 'Winter' 
			case when month in (9,10,11) and year = 2017 then 'Fall' 
		end as 'Frequency of tickets as per seasons'
		from nyc_taxi_violation_analysis;

b.)Then, find the 3 most common violations for each of these seasons.

		select distinct violation_code,(DISTINCT violation_code),
			case when month in (12,1,2) and year = 2017 then 'Winter' 
			case when month in (3,4,5) and year = 2017 then 'Spring' 
			case when month in (6,7,8) and year = 2017 then 'Winter' 
			case when month in (9,10,11) and year = 2017 then 'Fall' 
		end as 'Frequency of tickets as per seasons'
		from nyc_taxi_violation_analysis group by violation_code order by violation_code desc limit 3;


Note: Please ensure you make necessary optimizations to your queries like selecting the appropriate table format, using partitioned/bucketed tables. Marks will be awarded for keeping the performance also in mind.
Footer