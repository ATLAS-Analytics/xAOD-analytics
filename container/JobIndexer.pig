REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER ../xAOD-parser/target/xAODparser-*.jar
REGISTER ../libs/json.jar

REGISTER '/elasticsearch-hadoop/elasticsearch-hadoop-pig.jar';


define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://atlas-kibana.mwt2.org:9200','es.http.timeout = 5m');
SET pig.noSplitCombination TRUE;
SET default_parallel 5;

RECS = LOAD 'hdfs://analytix//user/rucio01/nongrid_traces/$INPD.json'  using PigStorage as (Rec:chararray);

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec));
describe B;

C = foreach B generate line::timeentry as timestamp, line::PandaID as PandaID, line::TaskID as TaskID, SIZE(line::accessedFiles) as nAccessedFiles, SIZE(line::AccessedBranches) as nAccessedBranches, SIZE(line::AccessedContainers) as nAccessedContainers, line::fileType as FileType, line::IP as IP, line::ROOT_RELEASE as ROOT_RELEASE, line::ReadCalls as ReadCalls, line::ReadSize as ReadSize, line::CacheSize as CacheSize, line::storageType as StorageType;


STORE C INTO 'xaod_job_accesses_$INDE/doc' USING EsStorage();


rmf heatmap.csv

D = foreach B generate (line::PandaID==0?0:1) as Grid, line::AccessedBranches as AB, line::AccessedContainers as AC, line::fileType as FT, line::timeentry;

G = GROUP D by (FT,Grid);
S = FOREACH G GENERATE FLATTEN(group) as (FileType, Grid), COUNT(D.Grid) as Jobs, ROUND(AVG(D.line::timeentry)) as timestamp, FLATTEN(xAODparser.HeatMapCounts(D.AB)) as AccB, FLATTEN(xAODparser.HeatMapCounts(D.AC)) as AccC;
describe S;
-- dump S;

STORE S INTO 'heatmap.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE');
