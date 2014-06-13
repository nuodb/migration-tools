DROP TABLE testdata_bigint;

/*Table structure for table testdata_bigint */
CREATE TABLE testdata_bigint (
  c1 bigint DEFAULT 0,
  c2 bigint NULL,
  c3 bigint NOT NULL,
  c4 bigint 
);

/*Dumping data for table testdata_bigint */
insert into testdata_bigint (c3,c4)  values(233720368547758,-9223372036854775808);
insert into testdata_bigint (c3)  values(2037203685477580);
insert into testdata_bigint (c1,c2,c3,c4)  values(5,-6872036854775808,2036854775,9223372036854775807);

DROP TABLE testdata_integer;

/*Table structure for table testdata_integer */
CREATE TABLE testdata_integer (
  c1 integer DEFAULT 3,
  c2 integer NULL,
  c3 integer NOT NULL,
  c4 integer 
);

/*Dumping data for table testdata_integer */
insert into testdata_integer (c3,c4)  values(83647,-47483648);
insert into testdata_integer (c3)  values(7483648);
insert into testdata_integer (c1,c2,c3,c4)  values(11,-2147483648,-7483648,2147483647);

DROP TABLE testdata_smallint;

/*Table structure for table testdata_smallint */
CREATE TABLE testdata_smallint (
  c1 smallint DEFAULT 0,
  c2 smallint NULL,
  c3 smallint NOT NULL,
  c4 smallint 
);

/*Dumping data for table testdata_smallint */
insert into testdata_smallint (c3)  values(5678);
insert into testdata_smallint (c1,c2,c3,c4)  values(6,-2768,-32768,32767);
insert into testdata_smallint (c3,c4)  values(9999,-32768);

DROP TABLE  testdata_tinyint;

/*Table structure for table testdata_tinyint */
CREATE TABLE testdata_tinyint (
  c1 tinyint DEFAULT 0,
  c2 tinyint NULL,
  c3 tinyint NOT NULL,
  c4 tinyint 
);

/*Dumping data for table testdata_tinyint */
insert into  testdata_tinyint (c3,c4) values(128,0);
insert into  testdata_tinyint ( c2,c3,c4) values(null,127,255);
insert into  testdata_tinyint ( c2,c3,c4) values(89,127,255);
insert into  testdata_tinyint (c1,c3) values(66,122);

 DROP TABLE  testdata_varchar;

/*Table structure for table testdata_varchar */
CREATE TABLE "testdata_varchar"(
  c1 varchar(8000) DEFAULT 'Default varchar',
  c2 varchar(8000) NULL,
  c3 varchar(8000) NOT NULL,
  c4 varchar(20) COLLATE Latin1_General_CI_AS 
);

/*Dumping data for table testdata_varchar */
insert into testdata_varchar (c2,c3,c4)  values('rglmjrqpasdilemrccsi',' test DATA ',null);
insert into testdata_varchar (c1,c2,c3)  values('user /s log','"double Qua"','aåbäcö');
insert into testdata_varchar (c1,c2,c3,c4)  values('default char value','rglmjrqpasdilemrccs','Référence','fzlrnbffhhliqbqpflmk');
insert into testdata_varchar (c2,c3,c4)  values('user''s log','vkwvluxrntoygiuhmajcrzluinajsyryxvcoqaolmgyhmuofbtcxshbaitazkaxtqlpuvywbgorfenmsyjsnzhmnqhctqoeymnenxyicwrnhbqtejiyewhhqgaazfcycrjwoiksotrkgifoejpeqwrtoxafhyzmvefhirgnjrhocomfxqowcxalkeigurexatbdsxaxzuzjrermkhiburgedwfijyrkuexccaqujusajkziprvkogxogizhqytwcgeakpmpjymhizqruzzkfnrklizghszpwsw','¤¸ËÕ¨íÚ×È¬°Ð¿ÈëÌÇ¹øÕ');
insert into testdata_varchar (c1,c2,c3,c4)  values('æ—¥æœ¬èªž',' ','/?µ„?ö??f?ÝøÀÚ?t?ª®?Î???Ë???ðnø§˜?íèÏ??Õ"?Ë?»?âõ??','Bientôt l été');
insert into testdata_varchar (c1,c2,c3)  values('Bientôt l été',' ','ÖÛ«Âé®üçÚ¾±²Ìñ¿øÞ°¯õÁæ­ûÊîâïÃÕôûçµé«áÄô');

DROP TABLE testdata_nvarchar;

/*Table structure for table testdata_nvarchar */
CREATE TABLE testdata_nvarchar (
  c1 nvarchar(4000)  DEFAULT 'Default nvarchar',
  c2 nvarchar(4000)  NULL,
  c3 nvarchar(4000)  NOT NULL,
  c4 nvarchar(20)  COLLATE Latin1_General_CI_AS 
);

/*Dumping data for table testdata_nvarchar */
insert into testdata_nvarchar (c2,c3,c4)  values('hadgocpaywfarefytrmp ',' SAMPLE TEST ',null);
insert into testdata_nvarchar (c1,c2,c3)  values('default character','rglmjrqpasdilemrccs','ÈÑÇÈÑ');
insert into testdata_nvarchar (c2,c3,c4)  values('"Double Quotes"','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','¤¸ËÕ¨íÚ×È¬°Ð¿ÈëÌÇ¹øÕ');
insert into testdata_nvarchar (c1,c2,c3)  values('Male / Female','user''s log','aåbäcö');
insert into testdata_nvarchar (c1,c2,c3)  values('æ—¥æœ¬èªž',' ','/?µ„?ö??f?ÝøÀÚ?t?ª®?Î???Ë???ðnø§˜?íèÏ??Õ"?Ë?»?âõ??');

DROP TABLE testdata_char;

/*Table structure for table testdata_char */
CREATE TABLE testdata_char (
  c1 char(8000)  DEFAULT 'Default char',
  c2 char(20)  NULL,
  c3 char(20)  NOT NULL,
  c4 char  COLLATE Latin1_General_CI_AS 
);

/*Dumping data for table testdata_char */
insert into testdata_char (c2,c3,c4)  values('rglmjrqpasdilemrccsi',' test DATA ','G');
insert into testdata_char (c1,c2,c3,c4)  values('user /s log','"double Qua"','aåbäcö',null);
insert into testdata_char (c1,c2,c3,c4)  values('default char value','rglmjrqpasdilemrccs','Référence','A');
insert into testdata_char (c2,c3)  values('hadgocpaywfarefytrmp ',' SAMPLE TEST ');
insert into testdata_char (c1,c2,c3)  values('default character','rglmjrqpasdilemrccs','ÈÑÇÈÑ');
insert into testdata_char (c1,c2,c3,c4)  values('¤¸ËÕ¨íÚ×È¬°Ð¿ÈëÌÇ¹øÕ','"Double Quotes"','fzlrnbffhhliqbqpflmk','Õ');
insert into testdata_char (c1,c2,c3)  values('Male / Female','user''s log','aåbäcö');
insert into testdata_char (c1,c2,c3,c4)  values('/?µ„?ö??f?ÝøÀÚ?t?ª®?Î???Ë???ðnø§˜?íèÏ??Õ"?Ë?»?âõ??',' ','æ—¥æœ¬èªž','S');

DROP TABLE testdata_nchar;

/*Table structure for table testdata_nchar */
CREATE TABLE testdata_nchar (
  c1 nchar(4000)  DEFAULT 'Default nchar',
  c2 nchar(10)  NULL,
  c3 nchar(10)  NOT NULL,
  c4 nchar  COLLATE Latin1_General_CI_AS
);

/*Dumping data for table testdata_nchar */
insert into testdata_nchar (c1,c2,c3,c4)  values('default char value','kduvrcolkb','Référence','G');
insert into testdata_nchar (c1,c2,c3)  values('¤¸ËÕ¨íÚ×È¬°Ð¿ÈëÌÇ¹øÕ','"D Quotes"',' duvrcolk ');
insert into testdata_nchar (c1,c2,c3,c4)  values('Male / Female','user''s log','aåbäcö','D');
insert into testdata_char (c1,c2,c3)  values('/?µ„?ö??f?ÝøÀÚ?t?ª®?Î???Ë???ðnø§˜?íèÏ??Õ"?Ë?»?âõ??',' ','æ—¥æœ¬èªž');
insert into testdata_nchar (c1,c2,c3,c4)  values('default character','ggxuneyzug ','ÈÑÇÈÑ','g');
insert into testdata_nchar (c2,c3,c4)  values(' people''s',' test DATA',null);
insert into testdata_nchar (c1,c2,c3)  values('lhqbjtgzphafoqpktvcuitcwmwazdvkhimnvarnqxmkdamxrtsbhddbjtqnluuatxfyozmpfmnohfgouazdqonhhnpjuynerjrmkdwilcfqqtaowxqdusddqqwsyrgiesxrxejajnutszrfbrdvlleaypvklpknczzmkvzjrutkrdfhncnocxsqvewkjhjrsrznqnduiqzeuqarhmlrvbjoervshpvuaaklagljpthwrixpdniquczvzqb','"D Qua"','aåbäcö');

DROP TABLE testdata_text;

/*Table structure for table testdata_text */
CREATE TABLE testdata_text (
  c1 text  DEFAULT 'default text',
  c2 text  NULL,
  c3 text NOT NULL,
  c4 text  COLLATE Latin1_General_CI_AS 
);

/*Dumping data for table testdata_text */
insert into testdata_text (c2,c3,c4)  values('hadgocpaywfarefytrmp ',' SAMPLE TEST ',null);
insert into testdata_text (c1,c2,c3)  values('default character','rglmjrqpasdilemrccs','ÈÑÇÈÑ');
insert into testdata_text (c2,c3,c4)  values('"Double Quotes"','axemyrckwnxayxncbmirfkpdrjyusnnoqgsvifjvunezyglhyptyfrrgcsmnvqvuhwoblkucixkkfgqjevfovtlplboucgfjkhzu','¤¸ËÕ¨íÚ×È¬°Ð¿ÈëÌÇ¹øÕ');
insert into testdata_text (c1,c2,c3)  values('Male / Female','user''s log','aåbäcö');
insert into testdata_text (c1,c2,c3)  values('æ—¥æœ¬èªž',' ','/?µ„?ö??f?ÝøÀÚ?t?ª®?Î???Ë???ðnø§˜?íèÏ??Õ"?Ë?»?âõ??');
insert into testdata_text (c2,c3,c4)  values('gsasdkadgkkwaegg',' test DATA  ',null);
insert into testdata_text (c1,c2,c3)  values('user /s log','"double Qua"','aåbäcö');

DROP TABLE testdata_ntext;

/*Table structure for table testdata_ntext */
CREATE TABLE testdata_ntext (
 c1 ntext  DEFAULT 'default ntext',
  c2 ntext  NULL,
  c3 ntext NOT NULL,
  c4 ntext  COLLATE Latin1_General_CI_AS 
);

/*Dumping data for table testdata_ntext */
insert into testdata_ntext (c2,c3,c4)  values('hadgocpaywfarefytrmp ','  TEST ',null);
insert into testdata_ntext (c2,c3,c4)  values('"Double Quotes"','fyxulkvwclxnknlckjnlavsztnwemrynxqdwyxxaiqzoaqutek','¤¸ËÕ¨íÚ×È¬°Ð¿ÈëÌÇ¹øÕ');
insert into testdata_ntext (c1,c2,c3,c4)  values('Male / Female','user''s log','aåbäcö',' Data');
insert into testdata_ntext (c1,c2,c3)  values('æ—¥æœ¬èªž',' ','/?µ„?ö??f?ÝøÀÚ?t?ª®?Î???Ë???ðnø§˜?íèÏ??Õ"?Ë?»?âõ??');
insert into testdata_ntext (c1,c2,c3,c4)  values('Bientôt l été',' ','ÖÛ«Âé®üçÚ¾±²Ìñ¿øÞ°¯õÁæ­ûÊîâïÃÕôûçµé«áÄô','ÈÑÇÈÑ');

DROP TABLE testdata_bit;

/*Table structure for table testdata_bit */
CREATE TABLE testdata_bit (
  c1 bit default 0,
  c2 bit  NULL,
  c3 bit  NOT NULL
);

/*Dumping data for table testdata_bit */
insert into testdata_bit (c1,c3)  values('1',0);
insert into testdata_bit (c2,c3)  values('0',1);

DROP TABLE testdata_numeric;

/*Table structure for table testdata_numeric */
CREATE TABLE testdata_numeric (
  c1 numeric(38,10) DEFAULT 0,
  c2 numeric(38,10) NULL,
  c3 numeric(38,10) NOT NULL,
  c4 numeric (38,10)
);

/*Dumping data for table testdata_numeric */
insert into testdata_numeric (c3,c4)  values(111111111111111111.1111111111,-92233720.36854775808);
insert into testdata_numeric (c3)  values(20.37203685477580);
insert into testdata_numeric (c1,c2,c3,c4)  values(3.0002,-6872036854775.808,203685477.5,9.223372036854775807);

DROP TABLE testdata_decimal;

/*Table structure for table testdata_decimal */
CREATE TABLE testdata_decimal (
  c1 decimal(38,10) DEFAULT 0,
  c2 decimal(38,10) NULL,
  c3 decimal(38,10) NOT NULL,
  c4 decimal (38,10)
);

/*Dumping data for table testdata_decimal */
insert into testdata_decimal (c3,c4)  values(9999999999999999999999999999.1111111,-92233720.36854775808);
insert into testdata_decimal (c3)  values(9920.873997203685477580);
insert into testdata_decimal (c1,c2,c3,c4)  values(3.0002,-6872036854775.808,203685477.5,9.223372036854775807);

DROP TABLE testdata_money;

/*Table structure for table testdata_money */
CREATE TABLE testdata_money (
  c1 money DEFAULT 0,
  c2 money NULL,
  c3 money NOT NULL,
  c4 money 
);

/*Dumping data for table testdata_money */
insert into testdata_money (c3,c4)  values(-922337203685477.5808,-33720.36854775);
insert into testdata_money (c3)  values(20.9720368540);
insert into testdata_money (c1,c3,c4)  values(92233720368547.5807,37203685477.5808,6.6);

DROP TABLE testdata_smallmoney;

/*Table structure for table testdata_smallmoney */
CREATE TABLE testdata_smallmoney (
  c1 smallmoney DEFAULT 0,
  c2 smallmoney NULL,
  c3 smallmoney NOT NULL,
  c4 smallmoney 
);

/*Dumping data for table testdata_smallmoney */
insert into testdata_smallmoney (c3,c4)  values(-214748.3648,-4748.36);
insert into testdata_smallmoney (c3)  values(7203.68540);
insert into testdata_smallmoney (c1,c3,c4)  values(+214748.3647,85477.5808,6.6);

DROP TABLE testdata_real;

/*Table structure for table testdata_real */
CREATE TABLE testdata_real (
  c1 real DEFAULT 42799889467980.034,
  c2 real NULL,
  c3 real NOT NULL,
  c4 real 
);

/*Dumping data for table testdata_real */
insert into testdata_real (c3,c4)  values(-3.40E + 38,0);
insert into testdata_real (c3)  values( -1.18E - 38);
insert into testdata_real (c1,c3,c4)  values(1.18E - 38,85477.5808,3.40E + 38);

DROP TABLE testdata_float;

/*Table structure for table testdata_float */
CREATE TABLE testdata_float (
  c1 float DEFAULT 9889467980.980034,
  c2 float NULL,
  c3 float NOT NULL,
  c4 float 
);

/*Dumping data for table testdata_float */
insert into testdata_float (c3,c4)  values(-1.79E + 308,0);
insert into testdata_float (c3)  values(-2.23E - 308);
insert into testdata_float (c1,c3,c4)  values(2.23E + 308,854.5081,1.79E + 308);


DROP TABLE testdata_hierarchyid;

/*Table structure for table testdata_hierarchyid */
CREATE TABLE testdata_hierarchyid (
  c1 hierarchyid,
  c2 hierarchyid NULL,
  c3 hierarchyid NOT NULL,
);

/*Dumping data for table testdata_hierarchyid */
insert into testdata_hierarchyid (c2,c3)  values('/1/','/1/3/2/');
insert into testdata_hierarchyid (c3)  values('/1/3/');
insert into testdata_hierarchyid (c1,c3)  values(0x5ADE,0x5ADE);

DROP TABLE testdata_uniqueidentifier;

/*Table structure for table testdata_uniqueidentifier */
CREATE TABLE testdata_uniqueidentifier (
  c1 uniqueidentifier default '33C3CCBC-B6BB-4CAA-AB10-338AA95F366E',
  c2 uniqueidentifier NULL,
  c3 uniqueidentifier NOT NULL,
);

/*Dumping data for table testdata_uniqueidentifier */
insert into testdata_uniqueidentifier (c2,c3)  values('6F9619FF-8B86-D011-B42D-00C04FC964FF','E45E13D8-CFF0-4FC7-B7C9-1D53E95C502D');
insert into testdata_uniqueidentifier (c3)  values('82136767-396E-4B33-B9DD-FFD30FCF4680');
insert into testdata_uniqueidentifier (c2,c3)  values('EFA24EC9-F8F9-47CF-839F-D588F69D167F','546F7C14-BDDA-4226-B45C-B0DDCD43E7DB');

DROP TABLE testdata_image;

/*Table structure for table testdata_image */
CREATE TABLE testdata_image (
  c1 image default ' 0x07DBB03C213E01C75084B5F9D38132DEA785D356B35C30871CE32A80C35907C2D37E34E3EC2AEB9D46C274F47F92F24D038243358C13EBEB18769093C2F434F926629CAA80C359E76E14EB940C1C394C6FA7F83D314E858C13CB3ABF6172655C3041646F626520496D616765526561647971C9653C5C305C305C6E974944415478DAEC5D4D8C144514AE5D66517EC2368A1248804625C644B283C6408C84D9C483F1B2B3376F0C172F1E766172655C3041646F626520496D616765526561647971C9653C5C305C305C6E974944415478DAEC5D4D8C144514AE5D66517EC2368A1248804625C644B283C6408C84D9C483',
  c2 image NULL,
  c3 image NOT NULL,
);

/*Dumping data for table testdata_image */
insert into testdata_image (c2,c3)  values('','C:\Users\Ganesan\Desktop\player.jpg');
insert into testdata_image (c1,c3)  values('C:\Users\Ganesan\Desktop\player.jpg','0xF217AB7A4F011E9FFC9B7A330E43E00C2A365532788208F050445517E0C0EC3F99755C30388F1F35AA7CDF76047888716AC4384AD6A9C8CF49020F314E9D80A3629D5C5A5C305C0C9E8018A79E8C33C23A0FCB1B7A48031E0FEB468C5337E020EB3C4DF9FF70EF, 0x0000022A, 0xFFD8FFE000104A46494600010200006400640000FFEC05BF4475636B79000100040000001400020592000002C7004C006F006E0064006F006E002C00200');

DROP TABLE testdata_binary;

/*Table structure for table testdata_binary */
CREATE TABLE testdata_binary (
  c1 binary(8000) default CAST(1101  AS BINARY(1)),
  c2 binary NULL,
  c3 binary NOT NULL,
);

/*Dumping data for table testdata_binary */
insert into testdata_binary (c2,c3)  values(CAST(10100101  AS BINARY(1)),CAST(10111101  AS BINARY(1)));
insert into testdata_binary (c3)  values(CAST(1110010100  AS BINARY(1)));

DROP TABLE testdata_varbinary;

/*Table structure for table testdata_varbinary */
CREATE TABLE testdata_varbinary (
  c1 varbinary(8000) default CAST(1111101000  AS VARBINARY(1)),
  c2 varbinary NULL,
  c3 varbinary NOT NULL,
);

/*Dumping data for table testdata_varbinary */
insert into testdata_varbinary (c2,c3)  values(CAST(10111101  AS VARBINARY(1)),CAST(01  AS VARBINARY(1)));
insert into testdata_varbinary (c3)  values(CAST(1111111110  AS VARBINARY(1)));

DROP TABLE testdata_time;

/*Table structure for table testdata_time */
CREATE TABLE testdata_time (
	c1 time DEFAULT '12:32:51.1234123', 
	c2 time(7) NOT NULL, 
	c3 time
);

/*Dumping data for table testdata_time */
insert into testdata_time values('12:32:51.1234123','23:32:51.1234567','1955-12-13 19:21:55.123');
insert into testdata_time (C2,C3) values('00:00:00.0000000','23:59:59.9999999');
insert into testdata_time (C2,C3) values('23:32:51.1234567','1955-12-13 19:21:55.123');

DROP TABLE testdata_date;

/*Table structure for table testdata_date */
CREATE TABLE testdata_date (
	c1 date DEFAULT '12-29-33 23:20:51', 
	c2 date NOT NULL, 
	c3 date
);

/*Dumping data for table testdata_date */
insert into testdata_date values('11-21-38','12-29-33 23:20:51','12-29-33 23:20:51');
insert into testdata_date (C2,C3) values('0001-01-01','9999-12-31');
insert into testdata_date (C2,C3) values('12-29-33 23:20:51','11-21-38');

DROP TABLE testdata_smalldatetime;

/*Table structure for table testdata_smalldatetime */
CREATE TABLE testdata_smalldatetime (
	c1 smalldatetime DEFAULT '1955-12-13 12:43:31', 
	c2 smalldatetime NOT NULL, 
	c3 smalldatetime
);

/*Dumping data for table testdata_smalldatetime */
insert into testdata_smalldatetime (C2,C3) values('1900-10-18 10:03:22','2012-01-11 11:41:19');
insert into testdata_smalldatetime values('1955-12-13 12:43:31','2012-12-13 12:43:29','2001-12-13 12:43:29');
insert into testdata_smalldatetime (C2,C3) values('1955-08-24 12:43:29','1988-05-15 12:43:29');

DROP TABLE testdata_datetime;

/*Table structure for table testdata_datetime */
CREATE TABLE testdata_datetime (
	c1 datetime DEFAULT '12-23-35', 
	c2 datetime NOT NULL, 
	c3 datetime
);

/*Dumping data for table testdata_datetime */
insert into testdata_datetime values('2012-02-21T18:10:00','01-01-2012 12:00:00','2012-01-01T00:00:00');
insert into testdata_datetime (C2,C3) values('January 1, 1753 00:00:00','December 31, 9999 23:59:59.997');
insert into testdata_datetime (C2,C3) values('01-01-1988 12:00:00','MARCH 28, 9999 23:59:59.997');

DROP TABLE testdata_datetimeoffset;

/*Table structure for table testdata_datetimeoffset */
CREATE TABLE testdata_datetimeoffset (
	c1 datetimeoffset DEFAULT '12-13-25 12:32:10 +05:30', 
	c2 datetimeoffset(4) NOT NULL, 
	c3 datetimeoffset
);

/*Dumping data for table testdata_datetimeoffset */
insert into testdata_datetimeoffset values('12-13-25 12:32:10 +05:30','12-13-25 12:32:10 +05:30','12-13-25 12:32:10 +05:30');
insert into testdata_datetimeoffset (C2,C3) values('0001-01-01 12:32:10 -14:00','9999-12-31 23:59:59.9999999 +14:00');
insert into testdata_datetimeoffset (C2,C3) values('12-13-25 12:32:10 +05:30','12-13-25 12:32:10 +05:30');

DROP TABLE testdata_datetime2;

/*Table structure for table testdata_datetime2 */
CREATE TABLE testdata_datetime2 (
	c1 datetime2 DEFAULT '01-01-1988 12:00:00', 
	c2 datetime2 NOT NULL, 
	c3 datetime2
);

/*Dumping data for table testdata_datetime2 */
insert into testdata_datetime2 values('2012-02-21T18:10:00','01-01-2012 12:00:00','2012-01-01T00:00:00');
insert into testdata_datetime2 (c2) values('2025-12-10 12:32:30.92');
insert into testdata_datetime2 (C2,C3) values('2025-12-10 12:32:10.13','MARCH 28, 9999 23:59:59.997');
