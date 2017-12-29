 
 
REGISTER '/home/acadgild/project1/piggybank.jar';

-- register the piggybank jar file to import the csv files
 
A = load '/home/acadgild/project1/Crimes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

-- load csv file. 



B = foreach A generate (chararray)$5 as primarytype,(boolean)$8 as arrest, (int)$11 as dist;

-- pickup the necessary coloumns with the type casting. $5 primary type. type of crime. $8 arrested or not. $11 district

-- dump B;



C = group B by dist;

describe C;


D = FOREACH C {
        S = FILTER B BY primarytype == 'THEFT' AND arrest == true;
        GENERATE COUNT (S.$2), $0;
}

store D into '/home/acadgild/project1/output3/';
