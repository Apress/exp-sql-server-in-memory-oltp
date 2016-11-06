sqlcmd -S "." -E -i createTable.sql -d InMemoryOLTP2014
bcp InMemoryOLTP2014.dbo.InputData in "factsales.dat" -T -f "FactSales.fmt" -b 50000 -S "."
pause
