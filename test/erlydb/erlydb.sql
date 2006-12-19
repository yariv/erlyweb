drop table language;
drop table project;
drop table person;
drop table developer_project;

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

create table developer_project(
	developer_id integer,
	project_id integer,
	primary key(developer_id, project_id))
	type=InnoDB;
	