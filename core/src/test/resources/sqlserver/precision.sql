CREATE TABLE [dbo].[precision1](
	[k1] [int] ,
	[c1] [bigint] ,
	[c2] [smallint] ,
	[c3] [tinyint] 
);

INSERT [dbo].[precision1] VALUES(2147483647,9223372036854775807,32767,255);
INSERT [dbo].[precision1] VALUES(-2147483648,-9223372036854775808,-32768,0);


CREATE TABLE [dbo].[precision2](
	[c1] [nvarchar](20) ,
	[c2] [varchar](20) ,
	[c3] [numeric](7, 2),
	[c4] [char](1) ,
	[c5] [bit] ,
	[c6] [decimal](7, 2),
	[c7] [float] NULL,
	[c8] [nchar](1) NULL,
);


INSERT [dbo].[precision2] VALUES('total word','text length',25.34,N'F',0,76.84,15.25,N'M')
INSERT [dbo].[precision2] VALUES('total word lenght 20','sample text length20',54325.34,N'F',0,98765.34,56.65,N'M')
INSERT [dbo].[precision2] VALUES('lenght','sample',525.34,N'F',0,145.34,96.65,N'M')

