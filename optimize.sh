#!/bin/bash
# don't use anymore, it's in download.sh
exit 0
set -e;
export DATABASE="NY_taxi"
export HIVE_PROTOCOL="http"  # binary | http
export LLAP=true
export PORT=10000
export HIVE_HOST="localhost"

#Define the database
tail -n+2 ddl/taxi_to_orc.sql > ddl/tmp_file_orc ; mv ddl/tmp_file_orc ddl/taxi_to_orc.sql
sed -i "1 i\use $DATABASE;" ddl/taxi_to_orc.sql

tail -n+2 ddl/optimize.sql > ddl/tmp_file_opt ; mv ddl/tmp_file_opt ddl/optimize.sql
sed -i "1 i\use $DATABASE;" ddl/optimize.sql

#build jdbc URL
if [ $HIVE_PROTOCOL == "http" ]
then
  export TRANSPORT_MODE=";transportMode=http;httpPath=cliservice"
  if $LLAP; then export PORT=10501; else export PORT=10001; fi
else
  export TRANSPORT_MODE=""
  if $LLAP; then export PORT=10500; fi
fi

export JDBC_URL="jdbc:hive2://$HIVE_HOST:$PORT/$TRANSPORT_MODE"

#execute the scripts
echo "to ORC"
beeline -u $JDBC_URL -n hive -f ddl/taxi_to_orc.sql
#hive -v -f ddl/to_orc.sql

echo "calculate stats"
beeline -u $JDBC_URL -n hive -f ddl/optimize.sql
#hive -v -f ddl/optimize.sql

echo "to Druid"
beeline -u $JDBC_URL -n hive -f ddl/to_druid.sql
