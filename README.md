# bloombergAPI
Java wrapper for Bloomberg API with implementation for Bloomberg Data Point (BDP), Bloomberg Data History (BDH), and Bloomberg Data Set (BDS).

Included sample Java Dashboard that is used to generate historical reports and alerts using in-house calculations on specific data points like 20 day average 

//Get all securities under an index, "SPX Index"

getIndexMembers(String parentTicker)

//Retrieve most recent data point - BDP & BDS

getReferenceData(Set<String> tickerSet, Set<String> fieldSet)
  
//Retrieve historical data points - BDH

getHistoricalData(Set<String> tickerSet, Set<String> fieldSet, LocalDate leftDate, LocalDate rightDate)
