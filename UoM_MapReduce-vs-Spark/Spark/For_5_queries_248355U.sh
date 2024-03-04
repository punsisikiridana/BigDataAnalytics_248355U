#!/bin/bash
###############################Specify Hive Table################################
HIVE_TABLE="flight_delays"
###############################Specify HiveQL Queries#############################
QUERIES=(
    "SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS AvgCarrierDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"
    "SELECT Year, AVG((NASDelay / ArrDelay) * 100) AS AvgNASDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"
    "SELECT Year, AVG((WeatherDelay / ArrDelay) * 100) AS AvgWeatherDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"
    "SELECT Year, AVG((LateAircraftDelay / ArrDelay) * 100) AS AvgLateAircraftDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"
    "SELECT Year, AVG((SecurityDelay / ArrDelay) * 100) AS AvgSecurityDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"
)
###############################Specify Mapping####################################
MAPPING=(
    "Career delay query"
    "Nas delay query"
    "Weather delay query"
    "Late aircraft delay query"
    "Security delay query"
)
###############################Iterating and calculating time##################################
for ((index=0; index<${#QUERIES[@]}; index++)); do
    query="${QUERIES[$index]}"
    mapping="${MAPPING[$index]}"
    HIVEQL_TIME=$( { time hive -e "$query"; } 2>&1 | grep real | awk '{print $2}' )
    echo "HiveQL Execution Time: $HIVEQL_TIME seconds"
    SPARKSQL_TIME=$( { time spark-sql -e "$query"; } 2>&1 | grep real | awk '{print $2}' )
    echo "SparkSQL Execution Time: $SPARKSQL_TIME seconds"
done
##############################################################################################