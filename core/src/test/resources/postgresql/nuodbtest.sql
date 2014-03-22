/*Table structure for table `datatypes1`*/
DROP TABLE datatypes1 cascade;

CREATE TABLE datatypes1
(
  c1 bigserial,
  c2 char unique,
  c3 character varying(20),
  c4 NUMERIC(5,2),
  c5 numeric(5),
  c6 bigint primary key,
  c7 boolean,
  c8 character(20),
  c9 date, 
  c10 numeric(5,2), 
  c11 integer,
  c12 double precision
);

/*Dumping data for table `datatypes1`*/

INSERT INTO datatypes1 (
            c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, 
            c11,c12)
    VALUES (456,'G','test', 10.25, 100,6,false,'sample text', 'jan-08-1999', 99.9, 567,10.3);

INSERT INTO datatypes1 (
            c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, 
            c11,c12)
    VALUES (9624,'A','check',20.55, 300,3,'yes','abcd','sep-09-1987', 9.99, 345,11.8);

/*Table structure for table `datatypes2`*/
DROP TABLE datatypes2 cascade;

CREATE TABLE datatypes2
(
  k1 serial primary key NOT NULL,
  c1 time with time zone,
  c2 time without time zone,
  c3 timestamp with time zone,
  c4 timestamp without time zone,
  c5 text,
  c6 smallint,
  c7 real,
  c8 NUMERIC(5,2)
);
 
 /*Dumping data for table `datatypes2`*/

INSERT INTO datatypes2(
            c1, c2, c3, c4, c5, c6, c7, c8)
    VALUES ('12:00-07', '04:05:06.789', '2001-09-09 10:23:54+02', '2004-10-19 10:23:54+02', 'test', 111,17.12, 23.4);
	
INSERT INTO datatypes2(
            c1, c2, c3, c4, c5, c6, c7, c8)
    VALUES ('12:00-07', '04:05:06.789', '2004-10-19 10:23:54+02', '2004-10-19 10:23:54+02','sample text value' , 122,87.23, 86.7);

/*Table structure for table `datatypes3`*/
DROP TABLE datatypes3 cascade;

CREATE TABLE datatypes3
(
  fk1 int  REFERENCES datatypes2(k1),
  c1 money,
  c2 text,
  c3 text,
  c4 NUMERIC(5,2),
  c5 bytea
);        

/*Dumping data for table `datatypes3`*/

INSERT INTO  datatypes3(
		fk1,c1,c2,c3,c4,c5)
	VALUES (1,'$231.34','1 day 12 hours 59 min 10 sec','10.1.0.0/16',999.30,decode('013d7d16d7ad4fefb61bd95b765c8ceb', 'hex'));
INSERT INTO datatypes3(
		fk1,c1,c2,c3,c4,c5)
	VALUES (2,'$144.14','1 12:59:10','10.1.0.0/16',999.30,decode('013f7e16d7ff4fdfb61bd95e765c8edb', 'hex'));

/*Creating index for column c5*/
DROP INDEX idxc5;
create index idxc5 ON datatypes2(c5);

/*Creating index for column fk1*/
DROP INDEX idxfk1;
create index idxfk1 ON datatypes3(fk1);