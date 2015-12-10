REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER /afs/cern.ch/user/i/ivukotic/xAOD-analytics/xAOD-parser/target/xAODparser-*.jar
REGISTER /afs/cern.ch/user/i/ivukotic/xAOD-analytics/libs/json.jar

REGISTER '/afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/pigCodes/Panda/JobArchive/elasticsearch-hadoop-pig-2.2.0-beta1.jar'

define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://cl-analytics.mwt2.org:9200');


RECS = LOAD '/user/rucio01/nongrid_traces/$INPD.json'  using PigStorage as (Rec:chararray);

B = FOREACH RECS GENERATE FLATTEN(xAODparser.Parser(Rec));
describe B;

F = filter B BY PandaID > 0L;

D = foreach F generate line::PandaID as PandaID, line::TaskID as TID, SIZE(line::accessedFiles) as nAccessedFiles, SIZE(line::AccessedBranches) as nAccessedBranches, SIZE(line::AccessedContainers) as nAccessedContainers, line::fileType as FileType, line::IP as IP, line::ROOT_RELEASE, line::ReadCalls, line::ReadSize, line::CacheSize, line::StorageType;


STORE D INTO 'xAOD_$INPD/jobs_data' USING EsStorage();