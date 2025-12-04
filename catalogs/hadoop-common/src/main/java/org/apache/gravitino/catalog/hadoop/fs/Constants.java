/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.hadoop.fs;

/** Constants used by the Hadoop file system catalog. */
public class Constants {

  // Name of the built-in local file system provider
  public static final String BUILTIN_LOCAL_FS_PROVIDER = "builtin-local";

  // Name of the built-in HDFS file system provider
  public static final String BUILTIN_HDFS_FS_PROVIDER = "builtin-hdfs";

  // Name of the configuration property for HDFS config resources
  public static final String CONFIG_RESOURCES = "config.resources";
  // Name of the configuration property to disable HDFS FileSystem cache
  public static final String FS_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
  // Name of the configuration property for Kerberos principal
  public static final String HADOOP_SECURITY_PRINCIPAL =
      "hadoop.security.authentication.kerberos.principal";
  // Name of the configuration property for Kerberos keytab
  public static final String HADOOP_SECURITY_KEYTAB =
      "hadoop.security.authentication.kerberos.keytab";
  // Name of the configuration property for Kerberos krb5.conf location
  public static final String HADOOP_KRB5_CONF = "hadoop.security.authentication.kerberos.krb5.conf";
  // Environment variable for Java Kerberos configuration
  public static final String SECURITY_KRB5_ENV = "java.security.krb5.conf";
  // Supported authentication types
  public static final String AUTH_KERBEROS = "kerberos";
  // Simple authentication type
  public static final String AUTH_SIMPLE = "simple";

  // The following parts are configuration keys for different file systems.

  // S3 specific configuration keys, in case of specific hadoop version binding, we will
  // map these keys to the corresponding hadoop s3 keys.
  public static final String S3_MAX_ERROR_RETRIES = "fs.s3a.attempts.maximum";

  public static final String S3_ESTABLISH_TIMEOUT = "fs.s3a.connection.establish.timeout";

  public static final String S3_RETRY_LIMIT = "fs.s3a.retry.limit";

  public static final String S3_RETRY_THROTTLE_LIMIT = "fs.s3a.retry.throttle.limit";

  // GCS specific configuration keys
  public static final String GCS_GCS_HTTP_CONNECT_TIMEOUT_KEY = "fs.gs.http.connect-timeout";
  public static final String GCS_HTTP_MAX_RETRY_KEY = "fs.gs.http.max.retry";

  // OSS specific configuration keys
  public static final String OSS_ESTABLISH_TIMEOUT_KEY = "fs.oss.connection.establish.timeout";
  public static final String OSS_MAX_ERROR_RETRIES_KEY = "fs.oss.attempts.maximum";

  // Azure Blob Storage specific configuration keys, please see: AbfsConfiguration
  public static final String ADLS_MAX_RETRIES = "fs.azure.io.retry.max.retries";

  // HDFS specific configuration keys
  public static final String HDFS_IPC_CLIENT_CONNECT_TIMEOUT_KEY = "ipc.client.connect.timeout";
  public static final String HDFS_IPC_PING_KEY = "ipc.client.ping";

  public static final String DEFAULT_CONNECTION_TIMEOUT = "5000";
}
