--Author: Amit Kumar
--Date: 12/01/2019
--Excecute below before executing HQL
--External table to load file in hive
--create HDFS LOCATION using : hadoop fs -mkdir /HADOOP/STAGE/CSV
--put text file in same hdfs location: hadoop fs -put local_dir/file.txt /HADOOP/STAGE/CSV


create schema if not exists stage;
drop table if exists stage.shopping_details purge;
create external table if not exists stage.shopping_details(
userID varchar(30),
productID varchar(30),
action string)
ROW FORMAT DELIMITED
FEILDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/HADOOP/STAGE/CSV'
tblproperties ("skip.header.line.count"="1");


--consist users and product which have AddToCart as action
drop table if exists stage.shopping_details_add purge;
create table if not exists stage.shopping_details_add
row format delimited 
fields terminated by '|' 
STORED AS TEXTFILE
LOCATION '/HADOOP/STAGE/SHOPPING_DETAILS_ADD'
as 
select 
userID,
productID
from stage.shopping_details where action='AddToCart'
group by
userID,
productID;

--consist users and product which have Purchase as action
drop table if exists stage.shopping_details_purchase purge;
create table if not exists stage.shopping_details_purchase
row format delimited 
fields terminated by '|' 
STORED AS TEXTFILE
LOCATION '/HADOOP/STAGE/SHOPPING_DETAILS_PURCHASE'
as 
select 
userID,
productID
from stage.shopping_details where action='Purchase'
group by
userID,
productID;


--selecting only user and product id's where customer has not purchased it 
--same can be done using NOT IN also
drop table if exists stage.shopping_details_final purge;
create table if not exists stage.shopping_details_final
row format delimited 
fields terminated by '|' 
STORED AS TEXTFILE
LOCATION '/HADOOP/STAGE/SHOPPING_DETAILS_FINAL'
as 
select 
a.userID,
a.productID
from stage.shopping_details_add a left outer join stage.shopping_details_purchase b
on a.userID=b.userID and a.productID=b.productID 
where b.userID is null and b.productID is null;


