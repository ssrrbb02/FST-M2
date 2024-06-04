-- Load input file from HDFS
inputDialogues4 = LOAD '/user/tripti/inputs/episodeIV_dialogues.txt' using PigStorage('\t') AS (name:chararray,line:chararray);
inputDialogues5 = LOAD '/user/tripti/inputs/episodeV_dialogues.txt' using PigStorage('\t')  AS (name:chararray,line:chararray);
inputDialogues6 = LOAD '/user/tripti/inputs/episodeVI_dialogues.txt' using PigStorage('\t')  AS (name:chararray,line:chararray);

--filter the first two lines
ranked4 = RANK inputDialogues4;
OnlyDialogues4 = FILTER ranked4 BY (rank_inputDialogues4>2);
ranked5 = RANK inputDialogues5;
OnlyDialogues5 = FILTER ranked5 BY (rank_inputDialogues5>2);
ranked6 = RANK inputDialogues6;
OnlyDialogues6 = FILTER ranked6 BY (rank_inputDialogues6>2);

--Merge the three files
inputData = UNION OnlyDialogues4,OnlyDialogues5,OnlyDialogues6;

--GroupByName
groupByName = GROUP inputData by name;

-- Count the number of line by each character
names = FOREACH groupByName GENERATE $0 as names, COUNT($1) as no_of_lines;
namesOrdered = ORDER names by no_of_lines DESC;

--remove the old result folder
rmf hdfs:///user/tripti/Outputs;

-- Store the result in HDFS
STORE namesOrdered INTO 'hdfs:///user/tripti/Outputs' using PigStorage('\t');