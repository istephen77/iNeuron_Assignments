-- ANALYZING ROAD SAFETY IN THE UK.  DATASET SOURCE: UK DEPARTMENT OF TRANSPORT 

-- Create Database
 create database uk_road_safety_2015;
 
 -- Use Database
 use uk_road_safety_2015;
 show tables;
 
 -- Create table : Accident_2015 an load data from the dataset
 
 CREATE TABLE IF NOT EXISTS Accidents_2015 (
	Accident_Index VARCHAR(13) NOT NULL, 
	Location_Easting_OSGR DECIMAL(38, 0), 
	Location_Northing_OSGR DECIMAL(38, 0), 
	Longitude DECIMAL(38, 6), 
	Latitude DECIMAL(38, 6), 
	Police_Force DECIMAL(38, 0) NOT NULL, 
	Accident_Severity DECIMAL(38, 0) NOT NULL, 
	Number_of_Vehicles DECIMAL(38, 0) NOT NULL, 
	Number_of_Casualties DECIMAL(38, 0) NOT NULL, 
	Date VARCHAR(10) NOT NULL, 
	Day_of_Week DECIMAL(38, 0) NOT NULL, 
	`Time` DATETIME, 
	Local_Authority_District DECIMAL(38, 0) NOT NULL, 
	Local_Authority_Highway VARCHAR(9) NOT NULL, 
	first_Road_Class DECIMAL(38, 0) NOT NULL, 
	first_Road_Number DECIMAL(38, 0) NOT NULL, 
	Road_Type DECIMAL(38, 0) NOT NULL, 
	Speed_limit DECIMAL(38, 0) NOT NULL, 
	Junction_Detail DECIMAL(38, 0) NOT NULL, 
	Junction_Control DECIMAL(38, 0) NOT NULL, 
	`2nd_Road_Class` DECIMAL(38, 0) NOT NULL, 
	`2nd_Road_Number` DECIMAL(38, 0) NOT NULL, 
	Pedestrian_Crossing_Human_Control DECIMAL(38, 0) NOT NULL, 
	Pedestrian_Crossing_Physical_Facilities DECIMAL(38, 0) NOT NULL, 
	Light_Conditions DECIMAL(38, 0) NOT NULL, 
	Weather_Conditions DECIMAL(38, 0) NOT NULL, 
	Road_Surface_Conditions DECIMAL(38, 0) NOT NULL, 
	Special_Conditions_at_Site DECIMAL(38, 0) NOT NULL, 
	Carriageway_Hazards DECIMAL(38, 0) NOT NULL, 
	Urban_or_Rural_Area DECIMAL(38, 0) NOT NULL, 
	Did_Police_Officer_Attend_Scene_of_Accident DECIMAL(38, 0) NOT NULL, 
	LSOA_of_Accident_Location VARCHAR(9)
);

SET SESSION sql_mode = '';

show global variables like 'local_infile';

SHOW VARIABLES LIKE "secure_file_priv";

set global local_infile = True;

SET GLOBAL local_infile=1;

SET GLOBAL local_infile=ON;

LOAD DATA LOCAL INFILE 
'C:/Users/Public/Documents/iNeuron-Data_Analytics/SQL/iNeuron_SQL_Project_2/datasets/Accidents.csv'
INTO TABLE Accidents_2015
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 LINES;

SELECT * FROM Accidents_2015;
	
 -- Create table : Accident_2015 an load data from the dataset
 
 CREATE TABLE Vehicles (
	Accident_Index VARCHAR(13) NOT NULL, 
	Vehicle_Reference DECIMAL(38, 0) NOT NULL, 
	Vehicle_Type DECIMAL(38, 0) NOT NULL, 
	Towing_and_Articulation DECIMAL(38, 0) NOT NULL, 
	Vehicle_Manoeuvre DECIMAL(38, 0) NOT NULL, 
	Vehicle_Location_Restricted_Lane DECIMAL(38, 0) NOT NULL, 
	Junction_Location DECIMAL(38, 0) NOT NULL, 
	Skidding_and_Overturning DECIMAL(38, 0) NOT NULL, 
	Hit_Object_in_Carriageway DECIMAL(38, 0) NOT NULL, 
	Vehicle_Leaving_Carriageway DECIMAL(38, 0) NOT NULL, 
	Hit_Object_off_Carriageway DECIMAL(38, 0) NOT NULL, 
	first_Point_of_Impact DECIMAL(38, 0) NOT NULL, 
	Was_Vehicle_Left_Hand_Drive DECIMAL(38, 0) NOT NULL, 
	Journey_Purpose_of_Driver DECIMAL(38, 0) NOT NULL, 
	Sex_of_Driver DECIMAL(38, 0) NOT NULL, 
	Age_of_Driver DECIMAL(38, 0) NOT NULL, 
	Age_Band_of_Driver DECIMAL(38, 0) NOT NULL, 
	Engine_Capacity DECIMAL(38, 0) NOT NULL, 
	Propulsion_Code DECIMAL(38, 0) NOT NULL, 
	Age_of_Vehicle DECIMAL(38, 0) NOT NULL, 
	Driver_IMD_Decile DECIMAL(38, 0) NOT NULL, 
	Driver_Home_Area_Type DECIMAL(38, 0) NOT NULL, 
	Vehicle_IMD_Decile DECIMAL(38, 0) NOT NULL
);

LOAD DATA LOCAL INFILE 
'C:/Users/Public/Documents/iNeuron-Data_Analytics/SQL/iNeuron_SQL_Project_2/datasets/Vehicles.csv'
INTO TABLE Vehicles
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 LINES;

SELECT * FROM VEHICLES;

 -- Create table : Accident_2015 an load data from the dataset

CREATE TABLE vehicletypes (
	`code` DECIMAL(38, 0) NOT NULL, 
	label VARCHAR(37) NOT NULL
);

LOAD DATA LOCAL INFILE 
'C:/Users/Public/Documents/iNeuron-Data_Analytics/SQL/iNeuron_SQL_Project_2/datasets/vehicletypes.csv'
INTO TABLE vehicletypes
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 LINES;

SELECT * FROM Accidents_2015;
SELECT * FROM VEHICLES;
SELECT * FROM vehicletypes;

-- 1. Evaluate the median severity value of accidents caused by various Motorcycles.
SELECT AVG(A.ACCIDENT_SEVERITY) AS 'MEDIAN SERVERITY' , C.LABEL AS 'MOTORCYCLE' FROM ACCIDENTS_2015 A INNER JOIN 
VEHICLES B ON A.ACCIDENT_INDEX = B.ACCIDENT_INDEX
INNER JOIN VEHICLETYPES C ON B.VEHICLE_TYPE = C.`CODE` 
WHERE C.LABEL LIKE '%otorcycle%' GROUP BY C.LABEL ORDER BY C.LABEL;

-- 2. Evaluate Accident Severity and Total Accidents per Vehicle Type

SELECT B.VEHICLE_TYPE AS 'VEHICLE TYPE', A.ACCIDENT_SEVERITY AS 'ACCIDENT SEVERITY' ,
COUNT(B.VEHICLE_TYPE) AS 'NUMBER OF ACCIDENTS' FROM ACCIDENTS_2015 A INNER JOIN 
VEHICLES B ON A.ACCIDENT_INDEX = B.ACCIDENT_INDEX
INNER JOIN VEHICLETYPES C ON B.VEHICLE_TYPE = C.`CODE` 
 GROUP BY 1 ORDER BY 2,3;


-- 3. Calculate the Average Severity by vehicle type.

SELECT AVG(A.ACCIDENT_SEVERITY) AS 'AVG SEVERITY', C.LABEL AS 'VEHICLE TYPE' FROM ACCIDENTS_2015 A INNER JOIN
VEHICLES B ON A.ACCIDENT_INDEX = B.ACCIDENT_INDEX INNER JOIN
VEHICLETYPES C ON B.VEHICLE_TYPE = C.CODE GROUP BY C.LABEL ORDER BY C.LABEL;

-- 4. Calculate the Average Severity and Total Accidents by Motorcycle.

SELECT  AVG(A.ACCIDENT_SEVERITY) AS 'AVERAGE SEVERITY' ,
COUNT(B.VEHICLE_TYPE) AS 'NUMBER OF ACCIDENTS', C.LABEL AS 'MOTORCYLE' FROM ACCIDENTS_2015 A INNER JOIN 
VEHICLES B ON A.ACCIDENT_INDEX = B.ACCIDENT_INDEX
INNER JOIN VEHICLETYPES C ON B.VEHICLE_TYPE = C.`CODE` 
WHERE C.LABEL LIKE '%otorcycle%' 
GROUP BY C.LABEL
ORDER BY C.LABEL;

-- ********************************************************************************************************* --

-- ANALYZING WORLD POPULATION.  DATASET SOURCE : CIA WORLD FACTBOOK


create database worldpopulation;

use worldpopulation;

CREATE TABLE population (
	name VARCHAR(45) NOT NULL, 
	slug VARCHAR(43) NOT NULL, 
	value DECIMAL(38, 0) NOT NULL, 
	date_of_information VARCHAR(17) NOT NULL, 
	ranking DECIMAL(38, 0) NOT NULL, 
	region VARCHAR(33) NOT NULL
);

LOAD DATA LOCAL INFILE 
'C:/Users/Public/Documents/iNeuron-Data_Analytics/SQL/iNeuron_SQL_Project_2/datasets/population.csv'
INTO TABLE population
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 LINES;

CREATE TABLE population_growth (
	name VARCHAR(45) NOT NULL, 
	slug VARCHAR(43) NOT NULL, 
	value DECIMAL(38, 2) NOT NULL, 
	date_of_information VARCHAR(9) NOT NULL, 
	ranking DECIMAL(38, 0) NOT NULL, 
	region VARCHAR(33) NOT NULL
);

LOAD DATA LOCAL INFILE 
'C:/Users/Public/Documents/iNeuron-Data_Analytics/SQL/iNeuron_SQL_Project_2/datasets/population_growth.csv'
INTO TABLE population_growth
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 LINES;

SELECT * FROM POPULATION;
SELECT * FROM POPULATION_GROWTH;

-- 1. Which country has the highest population?

SELECT `NAME` AS COUNTRY FROM POPULATION ORDER BY RANKING LIMIT 1;

-- 2. Which country has the least number of people?

SELECT `NAME` AS COUNTRY FROM POPULATION ORDER BY RANKING DESC LIMIT 1;

-- 3. Which country is witnessing the highest population growth?

SELECT `NAME` AS COUNTRY, `VALUE` FROM POPULATION_GROWTH ORDER BY `VALUE` DESC LIMIT 1;

-- 4. Which country has an extraordinary number for the population?

SELECT `NAME` AS COUNTRY FROM POPULATION ORDER BY RANKING LIMIT 1;

-- 5. Which is the most densely populated country in the world?

SELECT `NAME` AS COUNTRY, `VALUE` FROM POPULATION ORDER BY `VALUE` DESC LIMIT 1;

