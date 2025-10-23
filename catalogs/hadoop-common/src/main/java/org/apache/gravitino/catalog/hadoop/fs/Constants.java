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
  public static final String HDFS_CONFIG_RESOURCES = "hdfs.config.resources";
  // Name of the configuration property to disable HDFS FileSystem cache
  public static final String FS_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
  // Name of the configuration property for HDFS authentication type
  public static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
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
  public static final String AUTH_SIMPlE = "simple";
}
