if exists(select * from sys.tables t join sys.schemas s on t.schema_id = s.schema_id where s.name = 'dbo' and t.name = 'InputData') drop table dbo.InputData; 
go

create table dbo.InputData
(
	ADate date not null,
	Product nvarchar(40) not null,
	OrderId int not null,
	OrderNum varchar(43) not null,
	Quantity decimal(9,3) not null,
	UnitPrice money not null,
	Amount money not null
) 
go