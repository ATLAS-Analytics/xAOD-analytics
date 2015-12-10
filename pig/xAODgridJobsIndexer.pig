rmf heatmapEvents.csv

REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER /afs/cern.ch/user/i/ivukotic/xAOD-analytics/xAOD-parser/target/xAODparser-*.jar
REGISTER /afs/cern.ch/user/i/ivukotic/xAOD-analytics/libs/json.jar

-- ****************** TRACES *************************

RECS = LOAD '/user/rucio01/nongrid_traces/2015-11-2*.json'  using PigStorage as (Rec:chararray);
--dump RECS;

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec));
describe B;
-- dump B;


F = filter B BY PandaID > 0L;

D = foreach F generate line::PandaID as PID, line::TaskID as TID, SIZE(line::accessedFiles) as AF, line::AccessedBranches as AB, line::AccessedContainers as AC, line::fileType as FT;

-- these + FT, should be grouped and indexed separately. group by (FT, RR, CS, ST) 
-- , line::ROOT_RELEASE as RR, line::ReadCalls as RC, line::ReadSize as RS, line::CacheSize as CS, line::storageType as ST;

-- ********************** PANDA  ********************


-- PAN = LOAD '/atlas/analytics/panda/jobs/2015-11-*' USING AvroStorage();
-- describe PAN;

-- PA = filter PAN by PRODSOURCELABEL matches 'user' AND NOT PRODUSERNAME matches 'gangarbt';

-- JO = JOIN PA BY PANDAID, D BY PID;
-- describe JO;

-- ******************** GROUPING per taskid *******************

-- G = GROUP JO by PA::TASKID;
-- S = FOREACH G GENERATE group, COUNT(JO), FLATTEN(xAODparser.HeatMap(JO.D::AB));
-- describe S;

G = GROUP D by (TID, FT);
S = FOREACH G GENERATE FLATTEN(group) as (TID,FT), COUNT(D), FLATTEN(xAODparser.HeatMap(D.AB)) as AccB, FLATTEN(xAODparser.HeatMap(D.AC)) as AccC;
describe S;

-- L = LIMIT S 100;
-- dump L;


STORE S INTO 'heatmapEvents.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');
