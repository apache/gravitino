#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

IP=$(hostname -I | awk '{print $1}')
sed -i "s|<value>hdfs://localhost:9000|<value>hdfs://${IP}:9000|g" /usr/local/hive/conf/hive-site.xml
echo 10000 50000 > /proc/sys/net/ipv4/ip_local_port_range

/bin/bash /usr/local/sbin/start.sh
