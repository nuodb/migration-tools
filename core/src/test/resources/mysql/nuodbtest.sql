-- MySQL dump 10.13  Distrib 5.1.45, for Win64 (unknown)
--
-- Host: localhost    Database: nuodbtest
-- ------------------------------------------------------
-- Server version	5.1.45-community

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `datatypes1`
--

DROP TABLE IF EXISTS `datatypes1`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `datatypes1` (
  `c1` varchar(20) DEFAULT NULL,
  `c2` tinyint(4) DEFAULT NULL,
  `c3` text,
  `c4` date DEFAULT NULL,
  `c5` smallint(6) DEFAULT NULL,
  `c6` mediumint(9) NOT NULL DEFAULT '0',
  `c7` int(11) DEFAULT NULL,
  `c8` bigint(20) DEFAULT NULL,
  `c9` float(10,2) DEFAULT NULL,
  `c10` double DEFAULT NULL,
  `c11` bit,
  `c12` varbinary(90),
  `c13` binary(90),
  `c14` tinyblob,
  `c15` serial,
  PRIMARY KEY (`c6`),
  UNIQUE KEY `c2` (`c2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Dumping data for table `datatypes1`
--

LOCK TABLES `datatypes1` WRITE;
/*!40000 ALTER TABLE `datatypes1` DISABLE KEYS */;
INSERT INTO `datatypes1` VALUES ('test1',23,'sample text value','2012-09-29',45,345,67,8767,243.34,345.455,1,'^W.I_¼ÑÜ<~DÄx?Eö&^¼°\r^\^ÿ3F\r\nSoÆWloMoOuwu÷0ï%?.Térêz_}ï§_^X#!cƒ|µvü^\¿nòë^Xµ^F%.x~åx-Sk','^W.I_¼ÑÜ<~DÄx?Eö&^¼°\r^\^ÿ3F\r\nSoÆWloMoOuwu÷0ï%?.Térêz_}ï§_^X#!cƒ|µvü^\¿nòë^Xµ^F%.x~åx-Sk','^W.I_¼ÑÜ<~DÄx?Eö&^¼°\r^\^ÿ3F\r\nSoÆWloMoOuwu÷0ï%?.Térêz_}ï§_^X#!cƒ|µvü^\¿nòë^Xµ^F%.x~åx-Sk',10),('test 2',83,'','1995-03-19',454,3445,97,876765,123.54,235.565,0,'^W.I_¼ÑÜ<~DÄx?Eö&^¼°\r^\^ÿ3F\r\nSoÆWloMoOuwu÷0ï%?.Térêz_}ï§_^X#!cƒ|µvü^\¿nòë^Xµ^F%.x~åx-Sk','^W.I_¼ÑÜ<~DÄx?Eö&^¼°\r^\^ÿ3F\r\nSoÆWloMoOuwu÷0ï%?.Térêz_}ï§_^X#!cƒ|µvü^\¿nòë^Xµ^F%.x~åx-Sk','^W.I_¼ÑÜ<~DÄx?Eö&^¼°\r^\^ÿ3F\r\nSoÆWloMoOuwu÷0ï%?.Térêz_}ï§_^X#!cƒ|µvü^\¿nòë^Xµ^F%.x~åx-Sk',11);
/*!40000 ALTER TABLE `datatypes1` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER lastmodtrigger BEFORE INSERT ON `datatypes1` FOR EACH ROW SET NEW.c4 = NOW() */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `datatypes2`
--

DROP TABLE IF EXISTS `datatypes2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `datatypes2` (
  `k1` int(10) NOT NULL AUTO_INCREMENT,
  `c1` decimal(10,2) DEFAULT NULL,
  `c2` datetime DEFAULT NULL,
  `c3` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `c4` year(4) DEFAULT NULL,
  `c5` char(20) NOT NULL,
  `c6` ENUM('abcd', 'check', 'sample test') DEFAULT NULL,
  `c7` SET('one', 'two', '','three') DEFAULT NULL,
  PRIMARY KEY (`k1`),
  KEY `idx_c5` (`c5`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `datatypes2`
--

LOCK TABLES `datatypes2` WRITE;
/*!40000 ALTER TABLE `datatypes2` DISABLE KEYS */;
INSERT INTO `datatypes2` VALUES (1,'345.23','1986-12-29 23:45:59','1986-12-29 07:29:59',2012,'12345678900987654321','check',''),(2,'125.63','2000-10-19 23:45:59','2000-10-19 03:19:49',2013,'abcd','sample test','two');
/*!40000 ALTER TABLE `datatypes2` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `datatypes3`
--

DROP TABLE IF EXISTS `datatypes3`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `datatypes3` (
  `fk1` int(10) NOT NULL,
  `c1` tinytext,
  `c2` blob,
  `c3` mediumblob,
  `c4` mediumtext,
  `c5` longblob,
  `c6` longtext,
  KEY `idx_fk1` (`fk1`),
  CONSTRAINT `datatypes3_ibfk_1` FOREIGN KEY (`fk1`) REFERENCES `datatypes2` (`k1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `datatypes3`
--

LOCK TABLES `datatypes3` WRITE;
/*!40000 ALTER TABLE `datatypes3` DISABLE KEYS */;
INSERT INTO `datatypes3` VALUES (1,'nuodb','‰PNG\r\n\Z\n\0\0\0\rIHDR\0\0\0\0\0\0Z\0\0\0nRn1\0\0\0tEXtSoftware\0Adobe ImageReadyqÉe<\0\0\n—IDATxÚì]MŒE®]fQ~Â6ŠH€F%ÆD²ƒÆ@Œ„ÙÄƒñ²³7o/v¸yÛŞ7†ƒ.ÌŞ¼ÑÜ<˜ĞÄxËö&^¼°\rˆÿ³F\r\nŠõÆWloMõOuwu÷0ï%…™é®êz_}ï§ş#!©ƒ|µvü¿nòëµ‰.x~å×~-Sk<›2e8×øŸş7€Ÿ=µq«Æ@_â,~Ùxõy}W	ågÿq¥]ĞfÍ@sÁ–\'^¦\r=×S|Öaï®“tŸµx=/<J²Š«ø\nÌAç¹\ZêÙåu<O)—qXpöh~ùĞ«ù5[eˆ>MX|CÏLÊ—â7]ĞBØf€ÀÌãV	î‰7Ww±÷&‰Ê¹^Aî§ø@®A¥<Æék*Ñ/+÷ƒş‹ñuÌ9ËÕÇÓü=(ÒáJZCÿ£l¶Öœ{~]D“%Ø‡œå\"€siy)Ñiä¿ÎFs$iz¼gÊyF“ØNQÿ[X‡œåí2•8hÿéòÑÁô?]¹²‰ßÍQGx$änœ%É…Á2qÏš^`Åfİ™ãœói|˜ÆÎ¿ıÓ~Ñ, ®š>ÍÉİÀÙP„áCòçîO¸|°ÿİ<\';xÖÊcåÍ÷¾f³~.ÜñæÊ»4s1Ñ—Â\\¡)&\'ƒ¤2AÿÜm$“qø¢“`Ó¤\ZÖ\'4¹Ã}˜[i\"§?6÷™ª/C¤Ÿ‘”;ÉÀ1šxMé`†ÚJğ£HÊ\0N\ZÖ1C÷4YçN\nœ¤$ÆIdÍp?g—iğÄ)áwmN€“†uş2ã ‡¢;7Û°<!>ÇP„µùÓ2Ş§…ÓV³\0‡ã*€ÃYÂÓ~4ãì*ëFÂtô’òM‚C5Œëëv“Âô4ù&bœª€ÃYçnë”9Lo§ø=1N…ŒË:¿›KÆ…éÀ<vŠß{‡\nÇ:%DV*ğtÍàÆ¬3¨¹aº:/œà ëôFCò«zOŸü›z3Cà*6U2x‚ğPDUàÀì?™u\085ª|ßvxˆqjÄ8JÖ©ÈÏI1N€£b\Z\0\'€§Œ3Â:ËzHëFŒS7à ë<Mùÿpï(ûşŞ‘º¼‡ıŸs\n\nz²£ŒB¾¼õÍúû­3Œ;óäßì—‡†Nòşƒ?VùîÀ2—øå=µñAAO¦Ê,ìÒòÒ94]Ã‘ê=³›Ã™Çe&ÉEóäNúº¨±N@°6ÉRŞÌ#öÆéÛ&–ÏVq(ë¤î1‚G¬Œ²Ïñ“ß²Ã¯Ş!V!àè³Ï‡°o­éš.zz¿o÷»ìä‰«H¥™}Àïyƒgïìoq·¸;gø³»ÏZ;gcµàÃW|2E* à„\0S>î÷X`º½/¾‚pÙİûüÛlvïp….ä`lù~Î„ÁsÙ‡şà¶÷òKïX»{İB°Ä-¦#àL2pîÜÿlMO‡¥ØĞ€€S4j–‹Ò‘pPÄªÊ6Æ\"Up\n7A$2Aœº› €Ñ÷xEU%™ Oú+6¨x$EŒp\n6At‰‰U\n\Z˜gàä4AQlás`Ğ ä³œ”&ˆØ‚€3b‚,¶5w„5ˆ-8aĞˆ-HHHHHHHHH(ªŠœÖ)Br›m%üD>f¸Š\0·6Ñx>n<™Kò<\'µğİšl+O%Òğ®..ÔÁX¨]ı,[çâ&™[àİ”“ú¯~rw½Pİû‹}¼š\\Õ×xiÕ»i”‡Gõ„‚ø=Së9x‡mŞš$ĞN\'£²5/Êë§]­§\nzšå@çïñ2V$Ğ¸lû*Wh§€ƒgø»éˆ½†Ğé)Ğ8>ÎN›3\0šé9ÀNüº‰\r££Ôaãñ{/óËôî	â\\ĞÀğ)Éâ8ËğşĞíş;X>mÂÙˆ\0Šî\\A£X˜’³Ê=ş›÷Ê‹u´Í2K)¨[¼£èMÅoa-|‹?«­i¢½´0“M…bá¸,çs…C~¦<ÎûCÃy]«ø¹\'½ÀB›G4¤F]V€ÆE¤j ô\\é…»üsğ	nÔ4ı3ëàÑÛŞ/tŸ#5t¶© lü”®XP¬Åï_ÔNÌŞ†‘eÂú†w\\E´BŸõdvŸ–DG.˜7Êb\\ÃÀwü:¥PH¯f ™SÔixŞ\'¯ÿ¼4áw\\E ôUlP¤Ù§Áe³Ñ\r/Û1Gd)ë‚ÄH6ì\rN00Ò€ÿû‚¨n¿iÉ®mc\ZŞ`:ÔØeÛ÷™±ÑÁ®‹ô¥ i!(\"*l¾Âé]Y<ÂzQQ^§`ŸÇU¼0Ów”o\n\'UMKÎ­„Ô‚¡ª£p˜ëb¢dß¡›%|GğÈí c3\0R•W$›¾ã Ù„Š_óüZDæa*àlëó2^z«GÁ¦«9×Q4x×`ıåòìYÇJĞ¡R¢\'ÓÖfŠ™U¶9§H+äR²joCÍÄ›¶Ù2X^[‘?ÒÎ³**ß­ˆyÊ²\"-Ã>]¯h7\0g²¥(l“€£6—…œë€¬ã–ešQ¡a3b¥=ÄV˜Y~-K`ôu˜¸1À)r‡Q_b4Ûğ»øÓ$¹pzNZß´­³Õ$0%1ÅzÁŠ,30±³6h&8ã¦È²,@Ó•Z\Z¤ûñeÏÒgê±*›mM#Ï…N;íxØÄuEUªÈ&(t/n¬JDUN•†¹@WÈT™MJÚe\"!¹îy·àE€Èé\n‡|µ2Û>»\0Rf°¬BÒ\nÈH¾æŸàNœŒ01šã_rÄfòxFÇp\"ÕàhóGç\r$³k@‘}ƒfJÎğpÂUeNxUÀ	\nò;ì”¢œ®wp~NV¶YVø7}C QÍ“rePWàÈ¾@W×|ğß/HÊó€#7†—Åd…fn£zf\nbWRlª¨\'§ÿä×8{sô4”7§èánLyí(z™¯Ã<SOÌÀÈ8:¨ÄÆ…—…3\ne3˜Ø	ªÌã8x:\\1 ÌNÜú%t¤å^$Í¯_‡ßÛ“üÁuéE•‹“´zŠ)fnæTŞ±òZL½`XVŞ<T–ğõZ	f½^ÀjW(rxV&ÿ¼ @¬‡æÔt˜:	ÖNYæEş,¦p4—.ör¶¡Ä0htÙâŠ{’¡¹²‚&í gØ®Ô>ƒKhz\nÒEV6x‚ŠìG€FKyXf[áóXLÁ„.ş[\Z?h²\n¼·]Ây[.Ó˜ãSy8Šl1ı¬ë°A3Î¾æ0½Ê\0Mé)ƒ @Ëa.<w+&Æë”Óˆñ‚•Ñ~F$Ç1RæÈŠèéú#ws\Z	hyËm±èõñFNÚ`áÊ˜/«¢S[Êyµ>4#”Q”dHRÊ\0&­uÉãªn\0\0\0\0IEND®B`‚','‰PNG\r\n\Z\n\0\0\0\rIHDR\0\0\0\0\0\0Z\0\0\0nRn1\0\0\0tEXtSoftware\0Adobe ImageReadyqÉe<\0\0\n—IDATxÚì]MŒE®]fQ~Â6ŠH€F%ÆD²ƒÆ@Œ„ÙÄƒñ²³7o/v¸yÛŞ7†ƒ.ÌŞ¼ÑÜ<˜ĞÄxËö&^¼°\rˆÿ³F\r\nŠõÆWloMõOuwu÷0ï%…™é®êz_}ï§ş#!©ƒ|µvü¿nòëµ‰.x~å×~-Sk<›2e8×øŸş7€Ÿ=µq«Æ@_â,~Ùxõy}W	ågÿq¥]ĞfÍ@sÁ–\'^¦\r=×S|Öaï®“tŸµx=/<J²Š«ø\nÌAç¹\ZêÙåu<O)—qXpöh~ùĞ«ù5[eˆ>MX|CÏLÊ—â7]ĞBØf€ÀÌãV	î‰7Ww±÷&‰Ê¹^Aî§ø@®A¥<Æék*Ñ/+÷ƒş‹ñuÌ9ËÕÇÓü=(ÒáJZCÿ£l¶Öœ{~]D“%Ø‡œå\"€siy)Ñiä¿ÎFs$iz¼gÊyF“ØNQÿ[X‡œåí2•8hÿéòÑÁô?]¹²‰ßÍQGx$änœ%É…Á2qÏš^`Åfİ™ãœói|˜ÆÎ¿ıÓ~Ñ, ®š>ÍÉİÀÙP„áCòçîO¸|°ÿİ<\';xÖÊcåÍ÷¾f³~.ÜñæÊ»4s1Ñ—Â\\¡)&\'ƒ¤2AÿÜm$“qø¢“`Ó¤\ZÖ\'4¹Ã}˜[i\"§?6÷™ª/C¤Ÿ‘”;ÉÀ1šxMé`†ÚJğ£HÊ\0N\ZÖ1C÷4YçN\nœ¤$ÆIdÍp?g—iğÄ)áwmN€“†uş2ã ‡¢;7Û°<!>ÇP„µùÓ2Ş§…ÓV³\0‡ã*€ÃYÂÓ~4ãì*ëFÂtô’òM‚C5Œëëv“Âô4ù&bœª€ÃYçnë”9Lo§ø=1N…ŒË:¿›KÆ…éÀ<vŠß{‡\nÇ:%DV*ğtÍàÆ¬3¨¹aº:/œà ëôFCò«zOŸü›z3Cà*6U2x‚ğPDUàÀì?™u\085ª|ßvxˆqjÄ8JÖ©ÈÏI1N€£b\Z\0\'€§Œ3Â:ËzHëFŒS7à ë<Mùÿpï(ûşŞ‘º¼‡ıŸs\n\nz²£ŒB¾¼õÍúû­3Œ;óäßì—‡†Nòşƒ?VùîÀ2—øå=µñAAO¦Ê,ìÒòÒ94]Ã‘ê=³›Ã™Çe&ÉEóäNúº¨±N@°6ÉRŞÌ#öÆéÛ&–ÏVq(ë¤î1‚G¬Œ²Ïñ“ß²Ã¯Ş!V!àè³Ï‡°o­éš.zz¿o÷»ìä‰«H¥™}Àïyƒgïìoq·¸;gø³»ÏZ;gcµàÃW|2E* à„\0S>î÷X`º½/¾‚pÙİûüÛlvïp….ä`lù~Î„ÁsÙ‡şà¶÷òKïX»{İB°Ä-¦#àL2pîÜÿlMO‡¥ØĞ€€S4j–‹Ò‘pPÄªÊ6Æ\"Up\n7A$2Aœº› €Ñ÷xEU%™ Oú+6¨x$EŒp\n6At‰‰U\n\Z˜gàä4AQlás`Ğ ä³œ”&ˆØ‚€3b‚,¶5w„5ˆ-8aĞˆ-HHHHHHHHH(ªŠœÖ)Br›m%üD>f¸Š\0·6Ñx>n<™Kò<\'µğİšl+O%Òğ®..ÔÁX¨]ı,[çâ&™[àİ”“ú¯~rw½Pİû‹}¼š\\Õ×xiÕ»i”‡Gõ„‚ø=Së9x‡mŞš$ĞN\'£²5/Êë§]­§\nzšå@çïñ2V$Ğ¸lû*Wh§€ƒgø»éˆ½†Ğé)Ğ8>ÎN›3\0šé9ÀNüº‰\r££Ôaãñ{/óËôî	â\\ĞÀğ)Éâ8ËğşĞíş;X>mÂÙˆ\0Šî\\A£X˜’³Ê=ş›÷Ê‹u´Í2K)¨[¼£èMÅoa-|‹?«­i¢½´0“M…bá¸,çs…C~¦<ÎûCÃy]«ø¹\'½ÀB›G4¤F]V€ÆE¤j ô\\é…»üsğ	nÔ4ı3ëàÑÛŞ/tŸ#5t¶© lü”®XP¬Åï_ÔNÌŞ†‘eÂú†w\\E´BŸõdvŸ–DG.˜7Êb\\ÃÀwü:¥PH¯f ™SÔixŞ\'¯ÿ¼4áw\\E ôUlP¤Ù§Áe³Ñ\r/Û1Gd)ë‚ÄH6ì\rN00Ò€ÿû‚¨n¿iÉ®mc\ZŞ`:ÔØeÛ÷™±ÑÁ®‹ô¥ i!(\"*l¾Âé]Y<ÂzQQ^§`ŸÇU¼0Ów”o\n\'UMKÎ­„Ô‚¡ª£p˜ëb¢dß¡›%|GğÈí c3\0R•W$›¾ã Ù„Š_óüZDæa*àlëó2^z«GÁ¦«9×Q4x×`ıåòìYÇJĞ¡R¢\'ÓÖfŠ™U¶9§H+äR²joCÍÄ›¶Ù2X^[‘?ÒÎ³**ß­ˆyÊ²\"-Ã>]¯h7\0g²¥(l“€£6—…œë€¬ã–ešQ¡a3b¥=ÄV˜Y~-K`ôu˜¸1À)r‡Q_b4Ûğ»øÓ$¹pzNZß´­³Õ$0%1ÅzÁŠ,30±³6h&8ã¦È²,@Ó•Z\Z¤ûñeÏÒgê±*›mM#Ï…N;íxØÄuEUªÈ&(t/n¬JDUN•†¹@WÈT™MJÚe\"!¹îy·àE€Èé\n‡|µ2Û>»\0Rf°¬BÒ\nÈH¾æŸàNœŒ01šã_rÄfòxFÇp\"ÕàhóGç\r$³k@‘}ƒfJÎğpÂUeNxUÀ	\nò;ì”¢œ®wp~NV¶YVø7}C QÍ“rePWàÈ¾@W×|ğß/HÊó€#7†—Åd…fn£zf\nbWRlª¨\'§ÿä×8{sô4”7§èánLyí(z™¯Ã<SOÌÀÈ8:¨ÄÆ…—…3\ne3˜Ø	ªÌã8x:\\1 ÌNÜú%t¤å^$Í¯_‡ßÛ“üÁuéE•‹“´zŠ)fnæTŞ±òZL½`XVŞ<T–ğõZ	f½^ÀjW(rxV&ÿ¼ @¬‡æÔt˜:	ÖNYæEş,¦p4—.ör¶¡Ä0htÙâŠ{’¡¹²‚&í gØ®Ô>ƒKhz\nÒEV6x‚ŠìG€FKyXf[áóXLÁ„.ş[\Z?h²\n¼·]Ây[.Ó˜ãSy8Šl1ı¬ë°A3Î¾æ0½Ê\0Mé)ƒ @Ëa.<w+&Æë”Óˆñ‚•Ñ~F$Ç1RæÈŠèéú#ws\Z	hyËm±èõñFNÚ`áÊ˜/«¢S[Êyµ>4#”Q”dHRÊ\0&­uÉãªn\0\0\0\0IEND®B`‚','logo','‰PNG\r\n\Z\n\0\0\0\rIHDR\0\0\0\0\0\0Z\0\0\0nRn1\0\0\0tEXtSoftware\0Adobe ImageReadyqÉe<\0\0\n—IDATxÚì]MŒE®]fQ~Â6ŠH€F%ÆD²ƒÆ@Œ„ÙÄƒñ²³7o/v¸yÛŞ7†ƒ.ÌŞ¼ÑÜ<˜ĞÄxËö&^¼°\rˆÿ³F\r\nŠõÆWloMõOuwu÷0ï%…™é®êz_}ï§ş#!©ƒ|µvü¿nòëµ‰.x~å×~-Sk<›2e8×øŸş7€Ÿ=µq«Æ@_â,~Ùxõy}W	ågÿq¥]ĞfÍ@sÁ–\'^¦\r=×S|Öaï®“tŸµx=/<J²Š«ø\nÌAç¹\ZêÙåu<O)—qXpöh~ùĞ«ù5[eˆ>MX|CÏLÊ—â7]ĞBØf€ÀÌãV	î‰7Ww±÷&‰Ê¹^Aî§ø@®A¥<Æék*Ñ/+÷ƒş‹ñuÌ9ËÕÇÓü=(ÒáJZCÿ£l¶Öœ{~]D“%Ø‡œå\"€siy)Ñiä¿ÎFs$iz¼gÊyF“ØNQÿ[X‡œåí2•8hÿéòÑÁô?]¹²‰ßÍQGx$änœ%É…Á2qÏš^`Åfİ™ãœói|˜ÆÎ¿ıÓ~Ñ, ®š>ÍÉİÀÙP„áCòçîO¸|°ÿİ<\';xÖÊcåÍ÷¾f³~.ÜñæÊ»4s1Ñ—Â\\¡)&\'ƒ¤2AÿÜm$“qø¢“`Ó¤\ZÖ\'4¹Ã}˜[i\"§?6÷™ª/C¤Ÿ‘”;ÉÀ1šxMé`†ÚJğ£HÊ\0N\ZÖ1C÷4YçN\nœ¤$ÆIdÍp?g—iğÄ)áwmN€“†uş2ã ‡¢;7Û°<!>ÇP„µùÓ2Ş§…ÓV³\0‡ã*€ÃYÂÓ~4ãì*ëFÂtô’òM‚C5Œëëv“Âô4ù&bœª€ÃYçnë”9Lo§ø=1N…ŒË:¿›KÆ…éÀ<vŠß{‡\nÇ:%DV*ğtÍàÆ¬3¨¹aº:/œà ëôFCò«zOŸü›z3Cà*6U2x‚ğPDUàÀì?™u\085ª|ßvxˆqjÄ8JÖ©ÈÏI1N€£b\Z\0\'€§Œ3Â:ËzHëFŒS7à ë<Mùÿpï(ûşŞ‘º¼‡ıŸs\n\nz²£ŒB¾¼õÍúû­3Œ;óäßì—‡†Nòşƒ?VùîÀ2—øå=µñAAO¦Ê,ìÒòÒ94]Ã‘ê=³›Ã™Çe&ÉEóäNúº¨±N@°6ÉRŞÌ#öÆéÛ&–ÏVq(ë¤î1‚G¬Œ²Ïñ“ß²Ã¯Ş!V!àè³Ï‡°o­éš.zz¿o÷»ìä‰«H¥™}Àïyƒgïìoq·¸;gø³»ÏZ;gcµàÃW|2E* à„\0S>î÷X`º½/¾‚pÙİûüÛlvïp….ä`lù~Î„ÁsÙ‡şà¶÷òKïX»{İB°Ä-¦#àL2pîÜÿlMO‡¥ØĞ€€S4j–‹Ò‘pPÄªÊ6Æ\"Up\n7A$2Aœº› €Ñ÷xEU%™ Oú+6¨x$EŒp\n6At‰‰U\n\Z˜gàä4AQlás`Ğ ä³œ”&ˆØ‚€3b‚,¶5w„5ˆ-8aĞˆ-HHHHHHHHH(ªŠœÖ)Br›m%üD>f¸Š\0·6Ñx>n<™Kò<\'µğİšl+O%Òğ®..ÔÁX¨]ı,[çâ&™[àİ”“ú¯~rw½Pİû‹}¼š\\Õ×xiÕ»i”‡Gõ„‚ø=Së9x‡mŞš$ĞN\'£²5/Êë§]­§\nzšå@çïñ2V$Ğ¸lû*Wh§€ƒgø»éˆ½†Ğé)Ğ8>ÎN›3\0šé9ÀNüº‰\r££Ôaãñ{/óËôî	â\\ĞÀğ)Éâ8ËğşĞíş;X>mÂÙˆ\0Šî\\A£X˜’³Ê=ş›÷Ê‹u´Í2K)¨[¼£èMÅoa-|‹?«­i¢½´0“M…bá¸,çs…C~¦<ÎûCÃy]«ø¹\'½ÀB›G4¤F]V€ÆE¤j ô\\é…»üsğ	nÔ4ı3ëàÑÛŞ/tŸ#5t¶© lü”®XP¬Åï_ÔNÌŞ†‘eÂú†w\\E´BŸõdvŸ–DG.˜7Êb\\ÃÀwü:¥PH¯f ™SÔixŞ\'¯ÿ¼4áw\\E ôUlP¤Ù§Áe³Ñ\r/Û1Gd)ë‚ÄH6ì\rN00Ò€ÿû‚¨n¿iÉ®mc\ZŞ`:ÔØeÛ÷™±ÑÁ®‹ô¥ i!(\"*l¾Âé]Y<ÂzQQ^§`ŸÇU¼0Ów”o\n\'UMKÎ­„Ô‚¡ª£p˜ëb¢dß¡›%|GğÈí c3\0R•W$›¾ã Ù„Š_óüZDæa*àlëó2^z«GÁ¦«9×Q4x×`ıåòìYÇJĞ¡R¢\'ÓÖfŠ™U¶9§H+äR²joCÍÄ›¶Ù2X^[‘?ÒÎ³**ß­ˆyÊ²\"-Ã>]¯h7\0g²¥(l“€£6—…œë€¬ã–ešQ¡a3b¥=ÄV˜Y~-K`ôu˜¸1À)r‡Q_b4Ûğ»øÓ$¹pzNZß´­³Õ$0%1ÅzÁŠ,30±³6h&8ã¦È²,@Ó•Z\Z¤ûñeÏÒgê±*›mM#Ï…N;íxØÄuEUªÈ&(t/n¬JDUN•†¹@WÈT™MJÚe\"!¹îy·àE€Èé\n‡|µ2Û>»\0Rf°¬BÒ\nÈH¾æŸàNœŒ01šã_rÄfòxFÇp\"ÕàhóGç\r$³k@‘}ƒfJÎğpÂUeNxUÀ	\nò;ì”¢œ®wp~NV¶YVø7}C QÍ“rePWàÈ¾@W×|ğß/HÊó€#7†—Åd…fn£zf\nbWRlª¨\'§ÿä×8{sô4”7§èánLyí(z™¯Ã<SOÌÀÈ8:¨ÄÆ…—…3\ne3˜Ø	ªÌã8x:\\1 ÌNÜú%t¤å^$Í¯_‡ßÛ“üÁuéE•‹“´zŠ)fnæTŞ±òZL½`XVŞ<T–ğõZ	f½^ÀjW(rxV&ÿ¼ @¬‡æÔt˜:	ÖNYæEş,¦p4—.ör¶¡Ä0htÙâŠ{’¡¹²‚&í gØ®Ô>ƒKhz\nÒEV6x‚ŠìG€FKyXf[áóXLÁ„.ş[\Z?h²\n¼·]Ây[.Ó˜ãSy8Šl1ı¬ë°A3Î¾æ0½Ê\0Mé)ƒ @Ëa.<w+&Æë”Óˆñ‚•Ñ~F$Ç1RæÈŠèéú#ws\Z	hyËm±èõñFNÚ`áÊ˜/«¢S[Êyµ>4#”Q”dHRÊ\0&­uÉãªn\0\0\0\0IEND®B`‚','image binary');
/*!40000 ALTER TABLE `datatypes3` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `datatypesview`
--

DROP TABLE IF EXISTS `datatypesview`;
/*!50001 DROP VIEW IF EXISTS `datatypesview`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `datatypesview` (
  `d2c1` decimal(10,2),
  `d3c1` tinytext
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Final view structure for view `datatypesview`
--

/*!50001 DROP TABLE IF EXISTS `datatypesview`*/;
/*!50001 DROP VIEW IF EXISTS `datatypesview`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `datatypesview` AS select `d2`.`c1` AS `d2c1`,`d3`.`c1` AS `d3c1` from (`datatypes2` `d2` join `datatypes3` `d3`) where (`d2`.`k1` = `d3`.`fk1`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2013-03-03  0:49:36
