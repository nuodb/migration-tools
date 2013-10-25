DROP TABLE IF EXISTS `precision1`;

CREATE TABLE `precision1` (
  `c1` tinyint(4) DEFAULT NULL,
  `c2` smallint(6) DEFAULT NULL,
  `c3` mediumint(9) NOT NULL DEFAULT '0',
  `c4` int(11) DEFAULT NULL,
  `c5` bigint(20) DEFAULT NULL
);
insert into `precision1` values (66,2687,678246,49,3720368547758);
insert into `precision1` values (127,32767,8388607,2147483647,9223372036854775807);
insert into `precision1` values ('-128','-32768','-8388608','-2147483648','-9223372036854775807');

DROP TABLE IF EXISTS `precision2`;

CREATE TABLE `precision2` (
  `c1` varchar(20) DEFAULT NULL,
  `c2` text(20),
  `c3` decimal(10,2) DEFAULT NULL,
  `c4` float(10,2) DEFAULT NULL,
  `c5` double(10,2) DEFAULT NULL,
  `c6` bit,
  `c7` char(20) NOT NULL
);

insert into `precision2` values ('sample text','sample data',23.22,4.6,416.7,1,'1234567890');
insert into `precision2` values ('sample text length20','total word lenght 20',12345678.22,98765432.66,34567891.17,0,'12345678900123456789');
insert into `precision2` values ('','sample data',23.22,4.6,416.7,1,'5291');
commit;

