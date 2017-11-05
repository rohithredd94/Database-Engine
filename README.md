# Database-Engine
Database engine based on a hybrid between MySQL and SQLite. Only supports actions on a single table at a time. Approach similar to InnoDB engine.

Steps to run the code:
1. Extract the archive to a local folder
2. Open eclipse and load the project from the extracted folder
3. Build and run the code

Supported commands and syntax:

SHOW DATABASES;
SHOW TABLES;
SHOW COLUMNS;

CREATE DATABASE database_name;
CREATE TABLE INVENTORY (column_name1 data_type1, column_name2 data_type2, column_name3 data_type3 ...);
NOTE: The primary key column row_id is initialized by default, so please dont add it to create table. Doing so will crash the code

UPDATE table_name SET column_name = value where condition;
NOTE: Operators: >, <, <=, >=, =, is null, is not null;

SELECT (column_name(s)) or * FROM table_name where condition;
NOTE: Operators: >, <, <=, >=, =, is null, is not null;

DELETE FROM TABLE table_name where condition;
NOTE: Operators: >, <, <=, >=, =, is null, is not null;

DROP TABLE table_name;

DROP DATABASE database_name;

EXIT; QUIT;

Sample Commands:
create database warehouse;

create table inventory (part_no int, price double, description text, availability int);

insert into table (part_no, price, description, availability) inventory values (10, 100.25, nut, 10);
insert into table (part_no, availability) inventory values (50, 100);

update inventory set description = "screw" where availability > 90;

select * from inventory;
select part_no from inventory;
select row_id,part_no from inventory where price > 100;
select row_id,part_no from inventory where availability is null;

delete from table inventory where row_id = 6;

drop table inventory;
