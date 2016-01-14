REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER /afs/cern.ch/user/i/ivukotic/xAOD-analytics/xAOD-parser/target/xAODparser-*.jar
REGISTER /afs/cern.ch/user/i/ivukotic/xAOD-analytics/libs/json.jar

REGISTER '/afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/pigCodes/Panda/JobArchive/elasticsearch-hadoop-pig-2.2.0-beta1.jar'

define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://cl-analytics.mwt2.org:9200');


RECS = LOAD 'hdfs://p01001532965510.cern.ch:9000//user/rucio01/nongrid_traces/$INPD.json'  using PigStorage as (Rec:chararray);

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec));
describe B;

C = foreach B generate line::timeentry as timestamp, line::PandaID as PandaID, line::TaskID as TaskID, SIZE(line::accessedFiles) as nAccessedFiles, SIZE(line::AccessedBranches) as nAccessedBranches, SIZE(line::AccessedContainers) as nAccessedContainers, line::fileType as FileType, line::IP as IP, line::ROOT_RELEASE as ROOT_RELEASE, line::ReadCalls as ReadCalls, line::ReadSize as ReadSize, line::CacheSize as CacheSize, line::storageType as StorageType;


STORE C INTO 'xaod_$INPD/jobs_data' USING EsStorage();


rmf heatmap.csv

D = foreach B generate (line::PandaID==0?0:1) as Grid, line::AccessedBranches as AB, line::AccessedContainers as AC, line::fileType as FT, line::timeentry;

G = GROUP D by (FT,Grid);
S = FOREACH G GENERATE FLATTEN(group) as (FileType, Grid), COUNT(D.Grid) as Jobs, ROUND(AVG(D.line::timeentry)) as timestamp, FLATTEN(xAODparser.HeatMapCounts(D.AB)) as AccB, FLATTEN(xAODparser.HeatMapCounts(D.AC)) as AccC;
describe S;
-- dump S;

STORE S INTO 'heatmap.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');
