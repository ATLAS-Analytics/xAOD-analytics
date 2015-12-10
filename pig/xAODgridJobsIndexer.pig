rmf heatmap.csv

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER /afs/cern.ch/user/i/ivukotic/xAOD-analytics/xAOD-parser/target/xAODparser-*.jar
REGISTER /afs/cern.ch/user/i/ivukotic/xAOD-analytics/libs/json.jar


RECS = LOAD '/user/rucio01/nongrid_traces/2015-11-25.json'  using PigStorage as (Rec:chararray);

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec));
describe B;
-- dump B;


F = filter B BY PandaID > 0L;

D = foreach F generate (line::PandaID==0?0:1) as Grid, line::AccessedBranches as AB, line::AccessedContainers as AC, line::fileType as FT, line::timeentry;

G = GROUP D by FT;
S = FOREACH G GENERATE group, AVG(D.line::timeentry) as timestamp, FLATTEN(xAODparser.HeatMapCounts(D.AB)) as AccB, FLATTEN(xAODparser.HeatMapCounts(D.AC)) as AccC;
describe S;
dump S;

STORE S INTO 'heatmap.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');
