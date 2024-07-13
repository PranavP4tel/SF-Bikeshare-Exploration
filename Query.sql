-- Obtaining first and last station of each bike on 1st Jan 2018
SELECT
DISTINCT(bike_number) 
,FIRST_VALUE(start_station_name) OVER(
  PARTITION BY bike_number
  ORDER BY start_date
  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) AS first_station
, LAST_VALUE(end_station_name) OVER(
  PARTITION BY bike_number
  ORDER BY end_date
  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) AS last_station
FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
where start_date >='2018-01-01' and start_date<'2018-01-02'

--Estimation of people by Gender and customer Type
Select member_gender,subscriber_type, COUNT(1) as count
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
WHERE member_gender is not null
GROUP BY member_gender, subscriber_type
Order by count desc

--Estimation of bike trips over the years
Select EXTRACT(YEAR FROM start_date) as year, COUNT(1) as count
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
Group by year
order by year

-- Popularity of stations as a source and as a destination in 2018
select distinct(start_station_name), count(1) as station_popularity
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
where start_date>='2018-01-01'
group by start_station_name
order by station_popularity desc

select distinct(end_station_name), count(1) as station_popularity
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
where start_date>='2018-01-01'
group by end_station_name
order by station_popularity desc

-- Revealing most popular combinations of stations
select start_station_name, end_station_name, count(1) as popularity
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
where start_date>='2018-01-01'
group by start_station_name, end_station_name
order by popularity desc
limit 50

--Bike trips in various regions and corresponding stations within them
select b_region.name as region_name,b_station.name as station_name, count(1) as region_popularity
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` b_trips
inner join `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` b_station on CAST(b_station.station_id as int64) = b_trips.start_station_id
inner join `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` b_region on b_region.region_id =  b_station.region_id
where start_date>='2018-01-01'
group by b_region.name , b_station.name
order by region_popularity desc

select b_region.name as region_name, count(1) as region_popularity
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` b_trips
inner join `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` b_station on CAST(b_station.station_id as int64) = b_trips.start_station_id
inner join `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` b_region on b_region.region_id =  b_station.region_id
group by b_region.name
order by region_popularity desc

-- Age groups using the bikeshare services
--Customers with age beyond 70 years have been eliminated along with null values
with age_added as (
  select member_birth_year,
  (CAST(Extract(YEAR FROM CURRENT_DATE) AS int64) - member_birth_year) as member_age
  from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
  where member_birth_year is not null
),
age_groups as (
  select 
  member_birth_year,
  member_age,
  case
  when member_age>50 then '50+'
  when member_age>=30 then 'Middle_Age'
  else 'Young' 
  end as age_group
  from age_added
  where member_age<=70
)

select age_group, count(1) as popularity 
from age_groups
group by age_group
order by popularity desc

--Most used bikes on 1st Jan 2018, to undertand repair and maintenance of the same
select bike_number, count(1) as usage
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
where start_date>='2018-01-01'
group by bike_number 
order by usage desc

-- Bikeshare trip duration across the years, among various age groups
-- Only 2017 and 2018 data was returned, since birth year of the customers is not available for the previous years
with age_added as (
  select duration_sec, EXTRACT(YEAR FROM start_date) as trip_year  ,member_birth_year,
  (CAST(Extract(YEAR FROM CURRENT_DATE) AS int64) - member_birth_year) as member_age
  from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
  where member_birth_year is not null
),
age_groups as (
  select duration_sec, 
  trip_year,
  case
  when member_age>50 then '50+'
  when member_age>=30 then 'Middle_Age'
  else 'Young' 
  end as age_group
  from age_added
  where member_age<=70
)

select trip_year, age_group, ROUND(AVG(duration_sec)/60,0) as avg_trip_time_mins  
from age_groups
group by trip_year, age_group
order by trip_year

-- Avg trip time in mins across the years, dissected into months
select EXTRACT(YEAR FROM start_date) as trip_year, EXTRACT(MONTH from start_date) as trip_month,  ROUND(AVG(duration_sec)/60,0) as avg_trip_time_mins
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
group by trip_year, trip_month
order by trip_year, trip_month

-- Subscribers count over the years, drilled down into the months
select EXTRACT(YEAR FROM start_date) as trip_year, EXTRACT(MONTH from start_date) as trip_month, c_subscription_type as customer_type, COUNT(1) as count
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
where c_subscription_type is not null
group by trip_year, trip_month, customer_type
order by trip_year, trip_month, customer_type