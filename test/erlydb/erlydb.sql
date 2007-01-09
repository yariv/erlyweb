drop table language;
drop table project;
drop table person;
drop table person_project;
drop table customer;
drop table store;
drop table item;
drop table customer_store;
drop table customer_customer;

create table language (
	id integer auto_increment primary key,
	name varchar(30),
	description text,
	paradigm varchar(30),
	creation_year integer)
	type=InnoDB;

create table project (
	id integer auto_increment primary key,
	name varchar(30),
	description text,
	language_id integer,
	index(language_id))
	type=InnoDB;


CREATE TABLE person (
 	id integer auto_increment primary key,
	type char(10),
	name varchar(30),
	age integer,
	country varchar(20),
	office integer,
	department varchar(30),
	genre varchar(30),
	instrument varchar(30),
	created_on timestamp,
	index(type)
 ) type =InnoDB;

create table person_project(
	person_id integer,
	project_id integer,
	primary key(person_id, project_id))
	type=InnoDB;
	
	
create table store (
	number integer,
	name char(20),
	primary key(number, name))
	type = InnoDB;

create table item (
	size integer,
	name char(20),
	store_name char(20),
	store_number integer,
	primary key(size, name))
	type = InnoDB;
	
create table customer (
	id integer auto_increment primary key,
	name char(20))
	type = InnoDB;
	
create table customer_store (
	store_number integer,
	store_name char(20),
	customer_id integer,
	primary key(store_number, store_name, customer_id))
	type = InnoDB;

create table customer_customer (
	customer_id1 integer,
	customer_id2 integer,
	primary key(customer_id1, customer_id2)
	) type= InnoDB;
	