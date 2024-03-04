#!/bin/bash
################################Define hive table################################
HIVE_TABLE="flight_delays"
#####################################Hive Query######################################
HIVEQL_QUERY="SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS AvgCarrierDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"
#####################################Spark SQL Query##################################
SPARKSQL_QUERY="SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS AvgCarrierDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"
####################################Iterating 5 times################################
for i in {1..5}; do
    echo "Iteration Number : $i"
    HIVEQL_TIME=$( { time hive -e "$HIVEQL_QUERY"; } 2>&1 | grep real | awk '{print $2}' )
    echo "HiveQL Execution Time: $HIVEQL_TIME seconds"
    SPARKSQL_TIME=$( { time spark-sql -e "$SPARKSQL_QUERY"; } 2>&1 | grep real | awk '{print $2}' )
    echo "Spark SQL Execution Time: $SPARKSQL_TIME seconds"
done
####################################################################################