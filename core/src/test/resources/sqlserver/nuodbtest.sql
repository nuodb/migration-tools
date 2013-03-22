/*Table structure for table `datatypes1`*/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[datatypes1]') AND type in (N'U'))
DROP TABLE [dbo].[datatypes1]

CREATE TABLE [dbo].[datatypes1](
	[c1] [bigint] NULL,
	[c2] [char](1) NULL,
	[c3] [bit] NULL,
	[c4] [date] NULL,
	[c5] [datetime] NULL,
	[c6] [int] NOT NULL,
	[c7] [decimal](18, 0) NULL,
	[c8] [float] NULL,
	[c9] [datetime2](7) NULL,
	[c10] [money] NULL,
	[c11] [nchar](1) NULL,
	[c12] [timestamp] NULL,
PRIMARY KEY CLUSTERED 
(
	[c6] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY],
UNIQUE NONCLUSTERED 
(
	[c2] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

/*Dumping data for table `datatypes1`*/

insert into datatypes1 (c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11) values(453,'F',0,'01/02/03','2008-01-01 14:23:58.000','23',91.67,28.71,'2025-12-10 12:32:10.1234',56.91,'Z')
insert into datatypes1 (c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11) values(2872,'A',1,'07/13/10','2012-03-07 14:23:58.000','34',51.0,41.1,'2009-11-11 12:32:10.1234',28.91,'G')

/*Table structure for table `datatypes2`*/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[datatypes2]') AND type in (N'U'))
DROP TABLE [dbo].[datatypes2]


CREATE TABLE [dbo].[datatypes2](
	[k1] [int] IDENTITY(1,1) NOT NULL,
	[c1] [ntext] NULL,
	[c2] [text] NULL,
	[c3] [nvarchar](20) NULL,
	[c4] [real] NULL,
	[c5] [varchar](20) NULL,
	[c6] [smallint] NULL,
	[c7] [smallmoney] NULL,
	[c8] [smalldatetime] NULL,
	[c9] [time](7) NULL,
	[c10] [tinyint] NULL,
	[c11] [numeric](18, 0) NULL,
	[c12] [datetimeoffset](7) NULL,
	[c13] [timestamp] NULL,
PRIMARY KEY CLUSTERED 
(
	[k1] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

/*Dumping data for table `datatypes2`*/

insert into datatypes2 (c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12) values('test','value','sample text',917.412,'sample value',636,982.34,'2000-05-08 12:35:29.998','14:44:49.01',72,34,'2013-03-20 17:38:27.6030000 +00:00')
insert into datatypes2 (c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12) values('sample','test','testvalue',9.2,'test value',345,92.14,'1955-12-13 12:43:10','22:14:59.09',12,76,'2013-03-20 17:38:27.6030000 +00:00')

/*Table structure for table `datatypes3`*/

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[datatypes3]') AND type in (N'U'))
DROP TABLE [dbo].[datatypes3]

CREATE TABLE [dbo].[datatypes3](
	[fk1] [int] NULL,
	[c1] [image] NULL,
	[c2] [binary](4) NULL,
	[c3] [varbinary](max) NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

/*Dumping data for table `datatypes3`*/

INSERT INTO datatypes3
 SELECT 1,'€∞<!>«PÑµ˘”Å2ﬁßÖ”V≥\0á„*Ä√Y¬”~4„Ï*ÎùF¬tÙíÚMÇC5åÎÎvêì¬Ù4˘&bú™Ä√YÁnÎî9Loß¯=1NÖåÀ:øare\0Adobe ImageReadyq…e<\0\0\nóIDATx⁄Ï]MåEÆ]fQ~¬6äHÄF%∆D≤É∆@åÑŸƒÉÒ≤≥7o/vare\0Adobe ImageReadyq…e<\0\0\nóIDATx⁄Ï]MåEÆ]fQ~¬6äHÄF%∆D≤É∆@åÑŸƒÉÒ≤≥7o/v',CAST( 123456 AS BINARY(4) ), 
        (SELECT BulkColumn AS c3  FROM OPENROWSET( BULK 'E:\Users\ganesh\Desktop\img.jpg', Single_Blob) bc)

	INSERT INTO datatypes3
 SELECT 2,'Ú´zOü¸õz3C‡*6U2xÇPDU‡¿Ï?ôu\08è5™|ﬂvxàqjƒ8J÷©»œI1NùÄ£bù\Z\0\ûÄßûå3¬:ÀzHÎFåS7‡ Î<M˘ˇpÔ',CAST( 554 AS BINARY(4) ), 
        (SELECT BulkColumn AS c3  FROM OPENROWSET( BULK 'E:\Users\ganesh\Desktop\img.jpg', Single_Blob) bc)	

/*Creating index for column c5*/	
	
IF  EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[datatypes2]') AND name = N'idxc5')
DROP INDEX [idxc5] ON [dbo].[datatypes2] WITH ( ONLINE = OFF )
CREATE NONCLUSTERED INDEX [idxc5] ON [dbo].[datatypes2] 
(
	[c5] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
	
/*Creating index for column fk1*/	

IF  EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[datatypes3]') AND name = N'idxfk1')
DROP INDEX [idxfk1] ON [dbo].[datatypes3] WITH ( ONLINE = OFF )
CREATE NONCLUSTERED INDEX [idxfk1] ON [dbo].[datatypes3] 
(
	[fk1] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

