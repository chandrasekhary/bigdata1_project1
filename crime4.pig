REGISTER '/home/acadgild/project1/piggybank.jar';


loadData = LOAD '/home/acadgild/project1/Crimes.csv' USING 
org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX');

selectCols = FOREACH loadData GENERATE (chararray)$2 as DtTime,(chararray)$8 as Arrest;

filterSelectCols = FILTER selectCols BY (DtTime is not null) AND (Arrest == 'true');

dateSubstring = FOREACH filterSelectCols GENERATE 
ToDate(SUBSTRING(DtTime,0,19),'MM/dd/yyyy hh:mm:ss') as Dt,Arrest;

dateWiseArrest = FOREACH dateSubstring GENERATE GetMonth(Dt) as Month,GetYear(Dt) as Year,Arrest;

totalArrest = FILTER dateWiseArrest BY (Month>9 AND Year == 2014) OR (Month<11 and Year == 2015);

groupTotalArrest = GROUP totalArrest ALL;

countTotalArrest = FOREACH groupTotalArrest GENERATE COUNT(totalArrest.Arrest) as TotalArrests;

store countTotalArrest into '/home/acadgild/project1/output4/';

