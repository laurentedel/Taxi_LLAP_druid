#!/bin/bash
set -e;

export DATA_DIR="$(pwd)/data"
export HDFS_DIR="/tmp/taxi_llap"

export START="2012"
export END="2012"

export DATABASE="ny_taxi"
export HIVE_PROTOCOL="http"  # binary | http
export LLAP=true
export PORT=10000
export HIVE_HOST="localhost"

export OVERWRITE_TABLE=true

# For modifying HS2 configuration
export AMBARI="52.53.144.127"
export CLUSTER="ledel-druidllap"
export USER="admin"
export PASS="supersecret1"

#### Setup ######
#create data dir
mkdir -p $DATA_DIR

#create sql load file
rm -f ddl/load_data_text.sql
touch ddl/load_data_text.sql

######  Download ######
cd $DATA_DIR

for YEAR in $( seq $START $END )
do
  for MONTH in {01..12}
  do
      if [ ! -f "yellow_tripdata_$YEAR-$MONTH.csv.bz2" ] ; then
        curl https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_$YEAR-$MONTH.csv -O
      fi
      if [ ! -f "yellow_tripdata_$YEAR-$MONTH.csv.bz2" ] ; then
        echo "compressing yellow_tripdata_$YEAR-$MONTH.csv.bz2"
        bzip2 --fast yellow_tripdata_$YEAR-$MONTH.csv
      fi
      echo "LOAD DATA INPATH '$HDFS_DIR/data/yellow_tripdata_$YEAR-$MONTH.csv.bz2' INTO TABLE $DATABASE.trips_raw ;" >> ../ddl/load_data_text.sql

    echo "yellow_tripdata_$YEAR-$MONTH.csv : OK"
    sleep 1
  done
done

#Push data to hdfs
if $(hadoop fs -test -d $HDFS_DIR ) ;
  then sudo -u hdfs hdfs dfs -rm -f -R -skipTrash $HDFS_DIR
fi

hdfs dfs -mkdir -p $HDFS_DIR/data
hdfs dfs -copyFromLocal -f ./*.csv.bz2 $HDFS_DIR/data
sudo -u hdfs hdfs dfs -chmod -R 777 $HDFS_DIR
sudo -u hdfs hdfs dfs -chown -R hive:hdfs $HDFS_DIR

####### create Hive Structure
#create table structure
if $OVERWRITE_TABLE; then
  sed -i "1s/^/DROP TABLE IF EXISTS trips_raw PURGE;/" ../ddl/taxi_create.sql
  sed -i '1i\\' ../ddl/taxi_create.sql
fi

sed -i "1s/^/use ${DATABASE};/" ../ddl/taxi_create.sql
sed -i '1i\\' ../ddl/taxi_create.sql
sed -i "1s/^/create database if not exists ${DATABASE};/" ../ddl/taxi_create.sql

sed -i "1s/^/use ${DATABASE};/" ../ddl/taxi_to_orc.sql



#build jdbc URL
if [ $HIVE_PROTOCOL == "http" ]
then
  export TRANSPORT_MODE=";transportMode=http;httpPath=cliservice"
  if $LLAP; then export PORT=10501; else export PORT=10001; fi
  ## must add line to change optimize.sh
else
  export TRANSPORT_MODE=""
  if $LLAP; then export PORT=10500; fi
fi

export JDBC_URL="jdbc:hive2://$HIVE_HOST:$PORT/$TRANSPORT_MODE"

#load data
if $OVERWRITE_TABLE; then
  echo "creating Hive structure"
  echo ""
  beeline -u $JDBC_URL -n hive -f ../ddl/taxi_create.sql
  echo "OK"
  echo ""
fi

echo "loading data"
echo ""
beeline -u $JDBC_URL -n hive -f ../ddl/load_data_text.sql
echo ""
echo "OK"


#Define the database
tail -n+2 ddl/taxi_to_orc.sql > ddl/tmp_file_orc ; mv ddl/tmp_file_orc ddl/taxi_to_orc.sql
sed -i "1 i\use $DATABASE;" ddl/taxi_to_orc.sql

tail -n+2 ddl/optimize.sql > ddl/tmp_file_opt ; mv ddl/tmp_file_opt ddl/optimize.sql
sed -i "1 i\use $DATABASE;" ddl/optimize.sql

# First modify Hive whitelist to be able to fill some parameters
curl -u $USER:$PASS "http://$AMBARI:8080/api/v1/clusters/$CLUSTER" -X PUT -H 'X-Requested-By: Ambari' --data '[{"Clusters":{"desired_config":[{"type":"hiveserver2-interactive-site","properties":{"hive.async.log.enabled":"false","hive.metastore.metrics.enabled":"true","hive.server2.metrics.enabled":"true","hive.service.metrics.hadoop2.component":"hiveserver2","hive.service.metrics.reporter":"HADOOP2","hive.security.authorization.sqlstd.confwhitelist":"hive\\.auto\\..*|hive\\.cbo\\..*|hive\\.convert\\..*|hive\\.druid\\..*|hive\\.exec\\.dynamic\\.partition.*|hive\\.exec\\.max\\.dynamic\\.partitions.*|hive\\.exec\\.compress\\..*|hive\\.exec\\.infer\\..*|hive\\.exec\\.mode.local\\..*|hive\\.exec\\.orc\\..*|hive\\.exec\\.parallel.*|hive\\.explain\\..*|hive\\.fetch.task\\..*|hive\\.groupby\\..*|hive\\.hbase\\..*|hive\\.index\\..*|hive\\.index\\..*|hive\\.intermediate\\..*|hive\\.jdbc\\..*|hive\\.join\\..*|hive\\.limit\\..*|hive\\.log\\..*|hive\\.mapjoin\\..*|hive\\.merge\\..*|hive\\.optimize\\..*|hive\\.orc\\..*|hive\\.outerjoin\\..*|hive\\.parquet\\..*|hive\\.ppd\\..*|hive\\.prewarm\\..*|hive\\.server2\\.thrift\\.resultset\\.default\\.fetch\\.size|hive\\.server2\\.proxy\\.user|hive\\.skewjoin\\..*|hive\\.smbjoin\\..*|hive\\.stats\\..*|hive\\.strict\\..*|hive\\.tez\\..*|hive\\.vectorized\\..*|fs\\.defaultFS|ssl\\.client\\.truststore\\.location|distcp\\.atomic|distcp\\.ignore\\.failures|distcp\\.preserve\\.status|distcp\\.preserve\\.rawxattrs|distcp\\.sync\\.folders|distcp\\.delete\\.missing\\.source|distcp\\.keystore\\.resource|distcp\\.liststatus\\.threads|distcp\\.max\\.maps|distcp\\.copy\\.strategy|distcp\\.skip\\.crc|distcp\\.copy\\.overwrite|distcp\\.copy\\.append|distcp\\.map\\.bandwidth\\.mb|distcp\\.dynamic\\..*|distcp\\.meta\\.folder|distcp\\.copy\\.listing\\.class|distcp\\.filters\\.class|distcp\\.options\\.skipcrccheck|distcp\\.options\\.m|distcp\\.options\\.numListstatusThreads|distcp\\.options\\.mapredSslConf|distcp\\.options\\.bandwidth|distcp\\.options\\.overwrite|distcp\\.options\\.strategy|distcp\\.options\\.i|distcp\\.options\\.p.*|distcp\\.options\\.update|distcp\\.options\\.delete|mapred\\.map\\..*|mapred\\.reduce\\..*|mapred\\.output\\.compression\\.codec|mapred\\.job\\.queue\\.name|mapred\\.output\\.compression\\.type|mapred\\.min\\.split\\.size|mapreduce\\.job\\.reduce\\.slowstart\\.completedmaps|mapreduce\\.job\\.queuename|mapreduce\\.job\\.tags|mapreduce\\.input\\.fileinputformat\\.split\\.minsize|mapreduce\\.map\\..*|mapreduce\\.reduce\\..*|mapreduce\\.output\\.fileoutputformat\\.compress\\.codec|mapreduce\\.output\\.fileoutputformat\\.compress\\.type|oozie\\..*|tez\\.am\\..*|tez\\.task\\..*|tez\\.runtime\\..*|tez\\.queue\\.name|hive\\.transpose\\.aggr\\.join|hive\\.exec\\.reducers\\.bytes\\.per\\.reducer|hive\\.client\\.stats\\.counters|hive\\.exec\\.default\\.partition\\.name|hive\\.exec\\.drop\\.ignorenonexistent|hive\\.counters\\.group\\.name|hive\\.default\\.fileformat\\.managed|hive\\.enforce\\.bucketmapjoin|hive\\.enforce\\.sortmergebucketmapjoin|hive\\.cache\\.expr\\.evaluation|hive\\.query\\.result\\.fileformat|hive\\.hashtable\\.loadfactor|hive\\.hashtable\\.initialCapacity|hive\\.ignore\\.mapjoin\\.hint|hive\\.limit\\.row\\.max\\.size|hive\\.mapred\\.mode|hive\\.map\\.aggr|hive\\.compute\\.query\\.using\\.stats|hive\\.exec\\.rowoffset|hive\\.variable\\.substitute|hive\\.variable\\.substitute\\.depth|hive\\.autogen\\.columnalias\\.prefix\\.includefuncname|hive\\.autogen\\.columnalias\\.prefix\\.label|hive\\.exec\\.check\\.crossproducts|hive\\.cli\\.tez\\.session\\.async|hive\\.compat|hive\\.display\\.partition\\.cols\\.separately|hive\\.error\\.on\\.empty\\.partition|hive\\.execution\\.engine|hive\\.exec\\.copyfile\\.maxsize|hive\\.exim\\.uri\\.scheme\\.whitelist|hive\\.file\\.max\\.footer|hive\\.insert\\.into\\.multilevel\\.dirs|hive\\.localize\\.resource\\.num\\.wait\\.attempts|hive\\.multi\\.insert\\.move\\.tasks\\.share\\.dependencies|hive\\.query\\.results\\.cache\\.enabled|hive\\.query\\.results\\.cache\\.wait\\.for\\.pending\\.results|hive\\.support\\.quoted\\.identifiers|hive\\.resultset\\.use\\.unique\\.column\\.names|hive\\.analyze\\.stmt\\.collect\\.partlevel\\.stats|hive\\.exec\\.schema\\.evolution|hive\\.server2\\.logging\\.operation\\.level|hive\\.server2\\.thrift\\.resultset\\.serialize\\.in\\.tasks|hive\\.support\\.special\\.characters\\.tablename|hive\\.exec\\.job\\.debug\\.capture\\.stacktraces|hive\\.exec\\.job\\.debug\\.timeout|hive\\.llap\\.io\\.enabled|hive\\.llap\\.io\\.use\\.fileid\\.path|hive\\.llap\\.daemon\\.service\\.hosts|hive\\.llap\\.execution\\.mode|hive\\.llap\\.auto\\.allow\\.uber|hive\\.llap\\.auto\\.enforce\\.tree|hive\\.llap\\.auto\\.enforce\\.vectorized|hive\\.llap\\.auto\\.enforce\\.stats|hive\\.llap\\.auto\\.max\\.input\\.size|hive\\.llap\\.auto\\.max\\.output\\.size|hive\\.llap\\.skip\\.compile\\.udf\\.check|hive\\.llap\\.client\\.consistent\\.splits|hive\\.llap\\.enable\\.grace\\.join\\.in\\.llap|hive\\.llap\\.allow\\.permanent\\.fns|hive\\.exec\\.max\\.created\\.files|hive\\.exec\\.reducers\\.max|hive\\.reorder\\.nway\\.joins|hive\\.output\\.file\\.extension|hive\\.exec\\.show\\.job\\.failure\\.debug\\.info|hive\\.exec\\.tasklog\\.debug\\.timeout|hive\\.query\\.id|hive\\.query\\.tag|hive\\.enforce\\.bucketing|hive\\.enforce\\.sorting"},"service_config_version_note":"whitelisting"}]}}]'
# find HS2 host
HOST=$(curl -s -u $USER:$PASS "http://$AMBARI:8080/api/v1/clusters/$CLUSTER/services/HIVE/components/HIVE_SERVER_INTERACTIVE" | grep host_name | sed -n 's/.*".*" : "\(.*\)".*/\1/p')
curl -u $USER:$PASS "http://$AMBARI:8080/api/v1/clusters/$CLUSTER/requests" -H 'X-Requested-By: Ambari' --data '{"RequestInfo":{"command":"RESTART","context":"Restart all components with Stale Configs for Hive","operation_level":{"level":"SERVICE","cluster_name":"'"$CLUSTER"'","service_name":"HIVE"}},"Requests/resource_filters":[{"service_name":"HIVE","component_name":"HIVE_SERVER_INTERACTIVE","hosts":"'"$HOST"'"}]}'

echo "sleep 2 minutes while HIVE_SERVER_INTERACTIVE is restarting"
sleep 120

#execute the scripts
echo "to ORC"
beeline -u $JDBC_URL -n hive -f ddl/taxi_to_orc.sql
#hive -v -f ddl/to_orc.sql

echo "calculate stats"
beeline -u $JDBC_URL -n hive -f ddl/optimize.sql
#hive -v -f ddl/optimize.sql

echo "to Druid"
beeline -u $JDBC_URL -n hive -f ddl/to_druid.sql
