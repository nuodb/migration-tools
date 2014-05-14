
DROP TABLE testdata_smallint cascade;

/*Table structure for table testdata_smallint */
CREATE TABLE testdata_smallint (
  c1 smallint DEFAULT 0,
  c2 smallint NULL,
  c3 smallint NOT NULL,
  c4 smallint 
);

/*Dumping data for table testdata_smallint */
insert into testdata_smallint (c3)  values(266);
insert into testdata_smallint (c1,c2,c3,c4)  values(6,-3614,-32768,32767);
insert into testdata_smallint (c3,c4)  values(4372,-32768);

DROP TABLE testdata_integer cascade;

/*Table structure for table testdata_integer */
CREATE TABLE testdata_integer (
  c1 integer DEFAULT 3,
  c2 integer NULL,
  c3 integer NOT NULL,
  c4 integer 
);

/*Dumping data for table testdata_integer */
insert into testdata_integer (c3,c4)  values(83648,-2147483648);
insert into testdata_integer (c3)  values(47483);
insert into testdata_integer (c1,c2,c3,c4)  values(11,-2147483648,-7483648,2147483647);

DROP TABLE testdata_bigint cascade;

/*Table structure for table testdata_bigint */
CREATE TABLE testdata_bigint (
  c1 bigint DEFAULT 0,
  c2 bigint NULL,
  c3 bigint NOT NULL,
  c4 bigint 
);

/*Dumping data for table testdata_bigint */
insert into testdata_bigint (c3,c4)  values(33720368547,-9223372036854775808);
insert into testdata_bigint (c3)  values(37203685477580);
insert into testdata_bigint (c1,c2,c3,c4)  values(5,-72036854775808,-6854775808,9223372036854775807);

DROP TABLE testdata_real cascade;

/*Table structure for table testdata_real */
CREATE TABLE testdata_real(
  c1 real NULL default 12.222,
  c2 real NOT NULL,
  c3 real
);

/*Dumping data for table testdata_real */
insert into testdata_real (c2)  values(5423.677);
insert into testdata_real (c2,c3)  values(758.10,99999999999999999999999999999999999999.95310851286473213447630103066890160488);
insert into testdata_real (c1,c2)  values(5.6,8547.75);

DROP TABLE testdata_doubleprecision cascade;

/*Table structure for table testdata_doubleprecision */
CREATE TABLE testdata_doubleprecision(
  c1 double precision NULL default 729.52378,
  c2 double precision NOT NULL,
  c3 double precision
);

/*Dumping data for table testdata_doubleprecision */
insert into testdata_doubleprecision (c2)  values(54087423.6740877);
insert into testdata_doubleprecision (c2,c3)  values(7087458.108740,99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.4572087495);
insert into testdata_doubleprecision (c1,c2)  values(5740.674008,388547.72085);


DROP TABLE testdata_serial cascade;

/*Table structure for table testdata_serial */
CREATE TABLE testdata_serial (
  c1 serial 
);

/*Dumping data for table testdata_serial */
insert into testdata_serial (c1)  values(871);
insert into testdata_serial (c1)  values(971);
insert into testdata_serial (c1)  values(2147483647);

DROP TABLE testdata_smallserial cascade;

/*Table structure for table testdata_smallserial */
CREATE TABLE testdata_smallserial (
  c1 smallserial ,
  c2 integer
);

/*Dumping data for table testdata_smallserial */
insert into testdata_smallserial (c2)  values(276);
insert into testdata_smallserial (c1,c2)  values(125,376);
insert into testdata_smallserial (c1)  values(32767);

DROP TABLE testdata_bigserial cascade;

/*Table structure for table testdata_bigserial */
CREATE TABLE testdata_bigserial (
  c1 bigserial,
   c2 integer
);

/*Dumping data for table testdata_bigserial */
insert into testdata_bigserial (c1)  values(2036854775);
insert into testdata_bigserial  (c2)  values(2337);
insert into testdata_bigserial  (c1,c2) values(8547,9223372036854775807);

DROP TABLE testdata_char cascade;

/*Table structure for table testdata_char */
CREATE TABLE testdata_char (
  c1 char(20)  DEFAULT 'Test char',
  c2 char(20)  NULL,
  c3 char(10485760) NOT NULL,
  c4 char(20)
);

/*Dumping data for table testdata_char */
insert into testdata_char (c2,c3,c4)  values('rglmjrqpasdilemrccsi',' test DATA ',null);
insert into testdata_char (c1,c2,c3)  values('default char value','rglmjrqpasdilemrccs','Référence');
insert into testdata_char (c2,c3,c4)  values('user"s" log','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','¤¸ËÕ¨íÚ×È¬°Ð¿ÈëÌÇ¹øÕ');
insert into testdata_char (c1,c2,c3)  values('user /s log','user''s log','aåbäcö');
insert into testdata_char (c1,c2,c3)  values('Bientôt l été',' ','ÖÛ«Âé®üçÚ¾±²Ìñ¿øÞ°¯õÁæ­ûÊîâïÃÕôûçµé÷°Æêüª´å¾úô«áÄô');

DROP TABLE testdata_character cascade;

/*Table structure for table testdata_character */
CREATE TABLE testdata_character (
  c1 character(20)  DEFAULT 0,
  c2 character(20)  NULL,
  c3 character(10485760)  NOT NULL,
  c4 character(20)
);

/*Dumping data for table testdata_character */
insert into testdata_character (c2,c3,c4)  values('hadgocpaywfarefytrmp ',' SAMPLE TEST ',null);
insert into testdata_character (c1,c2,c3)  values('default character','rglmjrqpasdilemrccs','ÈÑÇÈÑ');
insert into testdata_character (c2,c3,c4)  values('"Double Quotes"','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','¤¸ËÕ¨íÚ×È¬°Ð¿ÈëÌÇ¹øÕ');
insert into testdata_character (c1,c2,c3)  values('Male / Female','user''s log','aåbäcö');
insert into testdata_character (c1,c2,c3)  values('æ—¥æœ¬èªž',' ','/?µ„?ö??f?ÝøÀÚ?t?ª®?Î???Ë???ðnø§˜?íèÏ??Õ"?Ë?»?âõ??');

DROP TABLE testdata_charactervarying cascade;

/*Table structure for table testdata_charactervarying */
CREATE TABLE testdata_charactervarying (
  c1 character varying(20) DEFAULT 0,
  c2 character varying(20) NULL,
  c3 character varying(10485760)  NOT NULL,
  c4 character varying(20)
);

/*Dumping data for table testdata_charactervarying */
insert into testdata_charactervarying (c2,c3,c4)  values('rglmjrqpasdilemrccsi',' test DATA ',null);
insert into testdata_charactervarying (c1,c2,c3)  values('default char value','rglmjrqpasdilemrccs','Référence');
insert into testdata_charactervarying (c2,c3,c4)  values('user"s" log','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','¤¸ËÕ¨íÚ×È¬°Ð¿ÈëÌÇ¹øÕ');
insert into testdata_charactervarying (c1,c2,c3)  values('user /s log','user''s log','aåbäcö');
insert into testdata_charactervarying (c1,c2,c3)  values('Bientôt l été',' ','ÖÛ«Âé®üçÚ¾±²Ìñ¿øÞ°¯õÁæ­ûÊîâïÃÕôûçµé÷°Æêüª´å¾úô«áÄô');

DROP TABLE testdata_varchar cascade;

/*Table structure for table testdata_varchar */
CREATE TABLE testdata_varchar (
  c1 varchar(20)  DEFAULT 0,
  c2 varchar(20) NULL,
  c3 varchar(10485760)   NOT NULL,
  c4 varchar(20)
);

/*Dumping data for table testdata_varchar */
insert into testdata_varchar (c2,c3,c4)  values('hadgocpaywfarefytrmp ',' SAMPLE TEST ',null);
insert into testdata_varchar (c1,c2,c3)  values('default character','rglmjrqpasdilemrccs','ÈÑÇÈÑ');
insert into testdata_varchar (c2,c3,c4)  values('"Double Quotes"','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','¤¸ËÕ¨íÚ×È¬°Ð¿ÈëÌÇ¹øÕ');
insert into testdata_varchar (c1,c2,c3)  values('Male / Female','user''s log','aåbäcö');
insert into testdata_varchar (c1,c2,c3)  values('æ—¥æœ¬èªž',' ','/?µ„?ö??f?ÝøÀÚ?t?ª®?Î???Ë???ðnø§˜?íèÏ??Õ"?Ë?»?âõ??');

DROP TABLE testdata_text cascade;

/*Table structure for table testdata_text */
CREATE TABLE testdata_text (
  c1 text  DEFAULT 0,
  c2 text  NULL,
  c3 text  NOT NULL,
  c4 text
);

/*Dumping data for table testdata_text */
insert into testdata_text (c2,c3,c4)  values('hadgocpaywfarefytrmp ',' SAMPLE TEST ',null);
insert into testdata_text (c1,c2,c3)  values('default character','rglmjrqpasdilemrccs','ÈÑÇÈÑ');
insert into testdata_text (c2,c3,c4)  values('"Double Quotes"','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','¤¸ËÕ¨íÚ×È¬°Ð¿ÈëÌÇ¹øÕ');
insert into testdata_text (c1,c2,c3)  values('Male / Female','user''s log','aåbäcö');
insert into testdata_text (c1,c2,c3)  values('æ—¥æœ¬èªž',' ','/?µ„?ö??f?ÝøÀÚ?t?ª®?Î???Ë???ðnø§˜?íèÏ??Õ"?Ë?»?âõ??');

DROP TABLE testdata_bytea cascade;

/*Table structure for table testdata_bytea */
CREATE TABLE testdata_bytea (
  c1 bytea  DEFAULT '\\000',
  c2 bytea  NULL,
  c3 bytea  NOT NULL
);

/*Dumping data for table testdata_bytea */
insert into testdata_bytea values('\\134','\\134','\\000');
insert into testdata_bytea values('\\134','\\134',decode('013d7d16d7ad4fefb61bd95b765c8ceb', 'hex'));

DROP TABLE testdata_boolean cascade;

/*Table structure for table testdata_boolean */
CREATE TABLE testdata_boolean (
  c1 boolean,
  c2 boolean  NULL,
  c3 boolean  NOT NULL
);

/*Dumping data for table testdata_boolean */
insert into testdata_boolean (c1,c3)  values('t',TRUE);
insert into testdata_boolean (c1,c2,c3)  values('y','yes','false');
insert into testdata_boolean (c1,c3)  values('no','on');
insert into testdata_boolean (c1,c2,c3)  values('0','off','n');

DROP TABLE testdata_timewithtimezone cascade;

/*Table structure for table testdata_timewithtimezone */
CREATE TABLE testdata_timewithtimezone (
  c1 time with time zone default '2003-04-12 04:05:06 America/New_York' ,
  c2 time with time zone  NULL,
  c3 time with time zone  NOT NULL,
  c4 time with time zone NOT NULL default CURRENT_TIMESTAMP
);

/*Dumping data for table testdata_timewithtimezone */
insert into testdata_timewithtimezone(c2,c3) values('00:00:00+1459','24:00:00-1459');

DROP TABLE testdata_timewithouttimezone cascade;

/*Table structure for table testdata_timewithouttimezone */
CREATE TABLE testdata_timewithouttimezone (
  c1 time without time zone default '04:05:06' ,
  c2 time without time zone  NULL,
  c3 time without time zone  NOT NULL,
  c4 time without time zone NOT NULL default CURRENT_TIMESTAMP
);

/*Dumping data for table testdata_timewithouttimezone */
insert into testdata_timewithouttimezone(c2,c3) values('00:00:00','24:00:00');

DROP TABLE testdata_timestampwithtimezone cascade;

/*Table structure for table testdata_timestampwithtimezone */
CREATE TABLE testdata_timestampwithtimezone (
  c1 timestamp with time zone default '1999-01-08 04:05:06 -8:00' ,
  c2 timestamp with time zone  NULL,
  c3 timestamp with time zone  NOT NULL,
  c4 timestamp with time zone NOT NULL default CURRENT_TIMESTAMP
);

/*Dumping data for table testdata_timestampwithtimezone */
insert into testdata_timestampwithtimezone(c2,c3) values('4713-01-08 04:05:06 BC -8:00','294276-01-08 04:05:06 AD -8:00');

DROP TABLE testdata_timestampwithouttimezone cascade;

/*Table structure for table testdata_timestampwithouttimezone */
CREATE TABLE testdata_timestampwithouttimezone (
  c1 timestamp without time zone default '1999-01-08 04:05:06' ,
  c2 timestamp without time zone  NULL,
  c3 timestamp without time zone  NOT NULL,
  c4 timestamp without time zone NOT NULL default CURRENT_TIMESTAMP
);

/*Dumping data for table testdata_timestampwithouttimezone */
insert into testdata_timestampwithouttimezone(c2,c3) values('4713-01-08 04:05:06 BC','294276-01-08 04:05:06 AD');


