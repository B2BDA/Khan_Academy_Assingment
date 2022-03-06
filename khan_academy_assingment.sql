-- CREATE DATABSE
create database test;

-- ACTIVATE DATABASE
use test;

-- drop table if exists users;

-- CREATE USER TABLE
create table users(
user_id CHAR(255) NOT NULL PRIMARY KEY,
registration_date DATE);

-- drop table if exists `usage`;

-- CREATE USAGE TABLE
create table `usage`(
user_id CHAR(255) NOT NULL, -- since in the data the values are repeating this cant be pk
usage_date DATE,
usage_location CHAR(255) NOT NULL,
time_spent INT NOT NULL);

-- INSERT VALUE INTO USER TABLE
INSERT INTO users
values ('aaa',"2019-01-03"),
('bbb',"2019-01-02"),
('ccc',"2019-01-15"),
('ddd',"2019-02-07");
-- select * from users;

-- INSERT VALUE INTO usages TABLE
INSERT INTO `usage`
values ('aaa',"2019-01-03","US",38),
('aaa',"2019-02-01","US",12),
('aaa',"2019-03-04","US",30),
('bbb',"2019-01-03","US",20),
('bbb',"2019-02-04","Canada",31),
('ccc',"2019-01-16","US",40),
('ddd',"2019-02-08","US",45)
;
-- select * from `usage`;


-- Calulate the rentention with respect to month of registration
 
with tab1 as (with tab as (select a.*, month(b.registration_date)as reg_month,
month(a.usage_date) as usage_month,
 b.registration_date, concat(year(b.registration_date),'-', month(b.registration_date)) as year_month_reg,
case when
month(b.registration_date) = month(usage_date)
then 1 else month(usage_date) end as month_number_from_reg,
case when time_spent >= 30 then 1 else 0 end as crossed_threshold
 from `usage` a left join users b
 on a.user_id = b.user_id)
select *,
sum(crossed_threshold) over(partition by reg_month, usage_month) as num_user_crossed_thres
 from tab q
 left join
 (
select year_month_reg_1, count( distinct user_id) as num_user_joined_per_cohort from 
(select *, 
concat(year(registration_date),'-', month(registration_date)) as year_month_reg_1 from users) as c
 group by 1) as p
 on q.year_month_reg = p.year_month_reg_1)
 select registration_month, 
 max(total_user) as total_users, 
 COALESCE(max(m1_retained),0) as m1_retention, 
 COALESCE(max(m2_retained),0) as m2_retention, 
 COALESCE(max(m3_retained),0) as m3_retention
  from (select distinct registration_month, total_user,
 case when 
month_number_from_reg=1 and reg_month = 1 then perc_retained
when 
month_number_from_reg=1 and reg_month = 2 then perc_retained
 else NULL end as m1_retained,
 case when 
month_number_from_reg=2 and reg_month = 1  then perc_retained 
when 
month_number_from_reg=2 and reg_month = 2  then perc_retained
else NULL end as m2_retained,
 case when 
month_number_from_reg=3 and reg_month = 1 
then perc_retained
when
month_number_from_reg=3 and reg_month = 3
then perc_retained else NULL end as m3_retained

 from 
 (select concat(DATE_FORMAT(registration_date,'%b'),',',YEAR(registration_date)) as registration_month,num_user_joined_per_cohort as total_user,
 month_number_from_reg,reg_month,
 concat(round((num_user_crossed_thres/num_user_joined_per_cohort)*100,0),'%') as perc_retained from tab1) as z) as x
 group by 1;