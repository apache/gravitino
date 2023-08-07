#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#

# Debug Graviton server
 export GRAVITON_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000"

# export JAVA_HOME
# export GRAVITON_HOME
# export GRAVITON_LOG_DIR     # Where log files are stored.  PWD by default.
# export GRAVITON_PID_DIR     # The pid files are stored. ${GRAVITON_HOME}/run by default.
# export GRAVITON_MEM         # Graviton jvm mem options Default -Xms1024m -Xmx1024m -XX:MaxMetaspaceSize=512m
