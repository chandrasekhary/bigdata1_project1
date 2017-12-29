 
 
REGISTER '/home/acadgild/project1/piggybank.jar';

-- register the piggybank jar file to import the csv files
 
A = load '/home/acadgild/project1/Crimes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

-- load csv file. 



B = foreach A generate (long)$0 as id,(chararray)$14 as fbicode;

-- pickup the necessary coloumns with the type casting. $0 is id and $14 is fbi code


D = group B by fbicode;

-- grouping the releation D based on fbicode



E = foreach D generate group, COUNT(B.id);

-- finding the count for cases for each fbicode

 F= order E by $1 DESC;

-- sorting the records based on cases solved by fbicode


store F into '/home/acadgild/project1/output1/';

-- storing the result in output1 directory


/*
dump A;






-- describe D;

-- E = foreach D generate group, COUNT($1);

-- dump E;

*/
