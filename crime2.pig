 
 
REGISTER '/home/acadgild/project1/piggybank.jar';

-- register the piggybank jar file to import the csv files
 
A = load '/home/acadgild/project1/Crimes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

-- load csv file. 



B = foreach A generate (long)$0 as id,(chararray)$14 as fbicode;

-- pickup the necessary coloumns with the type casting. $0 is id and $14 is fbi code


D = FILTER B BY fbicode  == '32';


store D into '/home/acadgild/project1/output2/';

