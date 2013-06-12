DROP TABLE IF EXISTS "products" CASCADE;
create table products
(	pid int primary key,
	pname varchar(20),
	stock int,
	category varchar(20),
	cheapestproduct varchar(20),
	unitprice double,
	productcount int,
	unitinstock int
);
insert into products values(1,'Alice Mutton',0,'Beverages','CheapestProducts',4.5000,10,23);
insert into products values(2,'Chef Anton',1,'Condiments','CheapestProducts',7.4500,4,12);
insert into products values(3,'Gorgonzola',0,'Produce','CheapestProducts',25,3,10);
insert into products values(4,'Perth Pasties',0,'Seafood','CheapestProducts',38,5,5);
insert into products values(5,'Outback Lager',1,'Beverages','CheapestProducts',3.3,5,12);

DROP TABLE IF EXISTS "customers" CASCADE;
create table customers
(	companyname varchar(20),
	month int,
	customerid varchar(20) primary key,
	region varchar(20)
);
insert into customers values('Alfreds Futterkiste',8,'c101','Washington');
insert into customers values('Ana Trujillo',12,'c102','New York');
insert into customers values('Around the Horn',7,'c103','U K');
insert into customers values('Berglunds snabb',2,'c104','Canada');
insert into customers values('Du monde',11,'c105','Washington');

DROP TABLE IF EXISTS "orders" CASCADE;
create table orders
(	orderid varchar(20) primary key,
	orderdate date,
	total double,
	customerid varchar(20)
);
ALTER TABLE orders ADD FOREIGN KEY (customerid) REFERENCES customers (customerid);
insert into orders values('O10690','29-sep-12',814.50,'c101');
insert into orders values('O10691','16-feb-89',814.50,'c101');
insert into orders values('O10692','15-mar-89',320.00,'c104');
insert into orders values('O10693','16-sep-89',2082.00,'c104');
insert into orders values('O10694','1-dec-89',88.80,'c105');

DROP TABLE IF EXISTS "array" CASCADE;
create table array
(	num int,
	digits varchar(20)
);
insert into array values(5,'zero');
insert into array values(4,'one');
insert into array values(1,'two');
insert into array values(3,'three');
insert into array values(9,'four');
insert into array values(8,'five');
insert into array values(6,'six');
insert into array values(7,'seven');
insert into array values(2,'eight');
insert into array values(0,'nine');

DROP TABLE IF EXISTS "array1" CASCADE;
create table array1
(	words varchar(20),
	doub double,
	matchwords varchar(20) 
);
insert into array1 values('aPPLE',1.7,'from');
insert into array1 values('AbAcUs',2.3,'salt');
insert into array1 values('bRaNcH',1.9,'earn');
insert into array1 values('BlUeBeRrY',4.1,'last');
insert into array1 values('ClOvEr',2.9,'near');
insert into array1 values('cHeRry',3.3,'form');

DROP TABLE IF EXISTS "array2" CASCADE;
create table array2
(	numa int,
	amt int,
	numc int
);
insert into array2 values(0,20,1);
insert into array2 values(2,10,11);
insert into array2 values(4,40,3);
insert into array2 values(5,50,19);
insert into array2 values(6,10,41);
insert into array2 values(8,70,65);
insert into array2 values(9,30,19);

DROP TABLE IF EXISTS "array3" CASCADE;
create table array3
(	numb int,
	repeatnum int,
	vector int
);
insert into array3 values(1,2,0);
insert into array3 values(3,2,2);
insert into array3 values(5,3,4);
insert into array3 values(7,5,5);
insert into array3 values(8,5,6);

DROP TABLE IF EXISTS "array4" CASCADE;
create table array4
(	similarword varchar(20),
	empty int
);
insert into array4 (similarword) values('believe');
insert into array4 (similarword) values('relief');
insert into array4 (similarword) values('receipt');
insert into array4 (similarword) values('field');

DROP TABLE IF EXISTS "array5" CASCADE;
create table array5
(	cate_name varchar(20),
	name varchar(20),
	score int,pid int
);
ALTER TABLE array5 ADD FOREIGN KEY (pid) REFERENCES products (pid);
insert into array5 values('Beverages','Alice',50,1);
insert into array5 values('Condiments','Bob',40,2);
insert into array5 values('Vegetables','Cathy',45,10);