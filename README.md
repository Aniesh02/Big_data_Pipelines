# Big_data_Pipelines /KAFKA-consumer and producer using geoip2 as streaming data
 * we will extract ip address from each message and then get Country ISO code. If it is US, we will send messages to partition 0 and for other countries,
 * we will send to the rest of the partitions using hash mod logic with partitions as 3 (which means data will for other Countries go into partition 1, 2, and 3).
 * Also if there are any invalid ips, we will send it to a different topic called retail_multi_invalid.
 * We will be using Java-based geoip2 provided by maxmind along with database with ip and country mapping.
