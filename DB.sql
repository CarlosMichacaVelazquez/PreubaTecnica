CREATE TABLE person (
	id_person int NOT NULL,
	first_name varchar(255) NULL,
	last_name varchar(255) NULL,
	CONSTRAINT person_pkey PRIMARY KEY (id_person)
);

CREATE TABLE address_person (
	id_address int NOT NULL,
	address varchar(255) NULL,
	city varchar(255) NULL,
	county varchar(255) NULL,
	 id_person int NULL,
	CONSTRAINT address_person_pkey PRIMARY KEY (id_address)
);

CREATE TABLE person_information (
	id_information int NOT NULL,
	phone1 VARCHAR(255) NULL,
	phone2 VARCHAR(255) NULL,
	email varchar(255) NULL,
	 id_person int NULL,
	CONSTRAINT person_information_pkey PRIMARY KEY (id_information)
);

CREATE TABLE company (
	id_company int NOT NULL,
	company_name varchar(255) NULL,
	web varchar(255) NULL,
	state varchar(255) NULL,
	zip int NULL,
	 id_person int NULL,
	CONSTRAINT company_pkey PRIMARY KEY (id_company)
);


ALTER TABLE company
DROP CONSTRAINT company_person_id_fkey 

ALTER TABLE address_person
DROP CONSTRAINT address_person_person_id_fkey

ALTER TABLE person_information
DROP CONSTRAINT person_information_person_id_fkey

ALTER TABLE company 
ADD CONSTRAINT company_person_id_fkey 
FOREIGN KEY (id_person) REFERENCES person(id_person);

ALTER TABLE address_person 
ADD CONSTRAINT address_person_person_id_fkey 
FOREIGN KEY (id_person) REFERENCES person(id_person);

ALTER TABLE person_information 
ADD CONSTRAINT person_information_person_id_fkey 
FOREIGN KEY (id_person) REFERENCES person(id_person);


select * from person 
SELECT * FROM person_information
SELECT * FROM company
SELECT * FROM address_person

truncate table person 
truncate table person_information
truncate table company
truncate table address_person


SELECT ps.first_name, ps.last_name,c.company_name,aps.address,aps.city,aps.county,c.state,c.zip,psi.phone1,psi.phone2,psi.email,c.web FROM PERSON ps 
inner join person_information psi on ps.id_person = psi.id_person 
inner join address_person aps on ps.id_person = aps.id_person 
inner join company c on ps.id_person = c.id_person 
