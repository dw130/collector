# collector


like logstash,
input --from kafka
filter --mapping data to influxdb point struct
output -- to influxdb

