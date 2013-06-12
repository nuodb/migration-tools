DROP TABLE  "DATATYPES1"  CASCADE CONSTRAINTS;
  
CREATE TABLE  "DATATYPES1" 
   (	"c1" VARCHAR(20), 
	"c2" VARCHAR(20), 
	"c3" VARCHAR2(20), 
	"c4" CHAR(20), 
	"c5" CHAR(20), 
	"c6" VARCHAR(20), 
	"c7" NUMBER(7,2), 
	"c8" NUMERIC(7,2), 
	"c9" CHAR(20), 
	"c10" DEC(7,2), 
	"c11" VARCHAR2(20), 
	 PRIMARY KEY ("c6") ENABLE, 
	 UNIQUE ("c2") ENABLE
   );



 insert into datatypes1 values('test','check','sample test','check','test values','25',2345.34,45.67,'test',4532.86,'test');
 insert into datatypes1 values('test1','test','test values','test','sample input','30',32.93,4.92,'check',218.86,'check');


DROP SEQUENCE   "DATATYPES2_SEQ";

CREATE SEQUENCE   "DATATYPES2_SEQ"   start with 1 increment by 1 minvalue 1 maxvalue 10000;

DROP TABLE  "DATATYPES2" CASCADE CONSTRAINTS;

CREATE TABLE  "DATATYPES2" 
   (	"k1" CHAR(20), 
	"c1" VARCHAR2(20), 
	"c2" CHAR(20), 
	"c3" VARCHAR2(20), 
	"c4" DECIMAL(7,2), 
	"c5" VARCHAR2(20), 
	"c6" TIMESTAMP (6), 
	"c7" VARCHAR2(20), 
	"c8" CHAR(20), 
	"c9" VARCHAR2(20), 
	"c10" CHAR(20), 
	"c11" DATE, 
	 PRIMARY KEY ("k1") ENABLE
   );


CREATE OR REPLACE TRIGGER  "DATATYPES2_TRG" 
 BEFORE INSERT ON datatypes2 FOR EACH ROW
BEGIN
   SELECT DATATYPES2_SEQ.NEXTVAL INTO :NEW."k1" FROM DUAL;
END;
/


insert into datatypes2 ("c1","c2","c3","c4","c5","c6","c7","c8","c9","c10","c11") values('sample test','test values','check',768.91,'test',current_timestamp,'test','check','xyz','test','29-sep-12');

insert into datatypes2 ("c1","c2","c3","c4","c5","c6","c7","c8","c9","c10","c11") values('check','sample','test value',528.21,'check',current_timestamp,'sample','test','abcd','check','16-feb-89');

DROP TABLE  "DATATYPES3" CASCADE CONSTRAINTS;

CREATE TABLE  "DATATYPES3" 
   (	"fk1" CHAR(20), 
	"c1" BLOB, 
	"c2" CLOB, 
	"c3" RAW(1000), 
	 CONSTRAINT "FK1_KEY" FOREIGN KEY ("fk1")
	  REFERENCES  "DATATYPES2" ("k1") ENABLE
   );

insert into datatypes3 values(1,hextoraw('453d7a34'),hextoraw('453d7a34'),utl_raw.cast_to_raw('596F75207765726520646973636F6E6E65637465642066 726 F 6D207468652041494D207365727669996365207768656E20796F75207369676E656420696E2066726F6D20616E6F74686572206C6F636174'));

insert into datatypes3 values(2,hextoraw('453d7a34'),hextoraw('453d7a34'),utl_raw.cast_to_raw('596F75207765726520646973636F6E6E65637465642066 726 F 6D207468652041494D207365727669996365207768656E20796F75207369676E656420696E2066726F6D20616E6F74686572206C6F636174'));

DROP INDEX "idxfk1";

CREATE INDEX  "idxfk1" ON  datatypes3 ("fk1");

DROP INDEX "idxc5";

CREATE INDEX  "idxc5" ON  datatypes2 ("c5");