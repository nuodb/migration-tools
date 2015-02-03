connect to sample;

SET CURRENT SCHEMA = 'nuodbtest';

drop table datatypes1;
create table datatypes1(
	"c1" char(20),	
	"c2" int not null,	
	"c3" long varchar,	
	"c4" graphic(20),	
	"c5" vargraphic(20),	
	"c6" integer not null primary key,
	"c7" smallint,
	"c8" bigint,
	"c9" real);
alter table datatypes1 add unique("c2");

insert into datatypes1 values('test',6,'sample text','abc','test values',2,21,86,6.9);
insert into datatypes1 values('value',8,'text value','xyz','sample',6,22,87,8.3);

drop table datatypes2;
create table datatypes2(
	"k1" integer not null primary key GENERATED ALWAYS AS IDENTITY (START WITH 5 INCREMENT BY 1),
	"c1" long vargraphic,
	"c2" float,
	"c3" double,
	"c4" decimal,
	"c5" varchar(20),
	"c6" graphic(20),
	"c7" double precision,
	"c8" date,
	"c9" time,
	"c10" timestamp);

insert into datatypes2("c1","c2","c3","c4","c5","c6","c7","c8","c9","c10") values ('test',12.6,45.7,23.4,'sample text','abc',41.1,'1999-10-22',current timestamp,'2013-06-19-14.24.53.783000');
insert into datatypes2("c1","c2","c3","c4","c5","c6","c7","c8","c9","c10") values ('abc',11.62,46.3,51.7,'test value','xyz',31.8,'2000-12-29','16:19:10',current timestamp);

drop table datatypes3;
create table datatypes3(	
	"fk1" int,	
	"c1" dec,	
	"c2" numeric,	
	"c3" num,	
	"c4" character(20),
	"c5" blob);
alter table datatypes3 add foreign key ("fk1") references datatypes2;

insert into datatypes3 values(5,29.7,45.1,82.5,'G',blob('���x��n��2B��N�c͕�S�>oU��_c��6{9�NW?��6˶�5̵�5͵�6ζ�7ϸ�9к�<Ѿ�?���D���I���N���U�'));
insert into datatypes3 values(6,71.8,49.2,34.3,'A',blob('��p���g����tg�t�4��*��t�f�ZZ[�9�*g@��4�ŗv���3z2��x�+|�}Yt���4�ʩBh~]e�e�T��GE�=~��L'));

/*drop index "idxc5";*/
create index "idxc5" ON datatypes2("c5");

/*drop index "idxfk1";*/
create index "idxfk1" ON datatypes3("fk1");




