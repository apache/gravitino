#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

./bin/spark-sql -v \
--conf spark.plugins="com.datastrato.gravitino.spark.connector.plugin.GravitinoSparkPlugin" \
--conf spark.sql.gravitino.uri=http://gravitino:8090 \
--conf spark.sql.gravitino.metalake=metalake_demo \
--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \
--conf spark.sql.session.timeZone=UTC 
