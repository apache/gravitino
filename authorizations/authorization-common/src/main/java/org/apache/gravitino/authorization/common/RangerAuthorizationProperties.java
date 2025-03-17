/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.authorization.common;

import com.google.common.base.Preconditions;
import java.util.Map;

/** The properties for Ranger authorization plugin. */
public class RangerAuthorizationProperties extends AuthorizationProperties {
  public static final String RANGER_PREFIX = "authorization.ranger";

  /** Ranger admin web URIs */
  public static final String RANGER_ADMIN_URL = "authorization.ranger.admin.url";

  /** Ranger service type */
  public static final String RANGER_SERVICE_TYPE = "authorization.ranger.service.type";

  /** Ranger service name */
  public static final String RANGER_SERVICE_NAME = "authorization.ranger.service.name";

  /** Ranger authentication type kerberos or simple */
  public static final String RANGER_AUTH_TYPE = "authorization.ranger.auth.type";

  /**
   * Ranger admin web login username(auth_type=simple), or kerberos principal(auth_type=kerberos)
   */
  public static final String RANGER_USERNAME = "authorization.ranger.username";

  /**
   * Ranger admin web login user password(auth_type=simple), or path of the keytab
   * file(auth_type=kerberos)
   */
  public static final String RANGER_PASSWORD = "authorization.ranger.password";

  public static final String RANGER_SERVICE_CREATE_IF_ABSENT =
      "authorization.ranger.service.create-if-absent";

  public static final String HADOOP_SECURITY_AUTHENTICATION =
      "authorization.ranger.hadoop.security.authentication";
  public static final String DEFAULT_HADOOP_SECURITY_AUTHENTICATION = "simple";
  public static final String HADOOP_RPC_PROTECTION = "authorization.ranger.hadoop.rpc.protection";
  public static final String DEFAULT_HADOOP_RPC_PROTECTION = "authentication";
  public static final String HADOOP_SECURITY_AUTHORIZATION =
      "authorization.ranger.hadoop.security.authorization";
  public static final String FS_DEFAULT_NAME = "authorization.ranger.fs.default.name";
  public static final String FS_DEFAULT_VALUE = "hdfs://127.0.0.1:8090";

  public static final String JDBC_DRIVER_CLASS_NAME = "authorization.ranger.jdbc.driverClassName";

  public static final String DEFAULT_JDBC_DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver";

  public static final String JDBC_URL = "authorization.ranger.jdbc.url";

  public static final String DEFAULT_JDBC_URL = "jdbc:hive2://127.0.0.1:8081";

  public RangerAuthorizationProperties(Map<String, String> properties) {
    super(properties);
  }

  @Override
  public String getPropertiesPrefix() {
    return RANGER_PREFIX;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(
        properties.containsKey(RANGER_ADMIN_URL),
        String.format(ErrorMessages.MISSING_REQUIRED_ARGUMENT, RANGER_ADMIN_URL));
    Preconditions.checkArgument(
        properties.containsKey(RANGER_SERVICE_TYPE),
        String.format(ErrorMessages.MISSING_REQUIRED_ARGUMENT, RANGER_SERVICE_TYPE));
    Preconditions.checkArgument(
        properties.containsKey(RANGER_AUTH_TYPE),
        String.format(ErrorMessages.MISSING_REQUIRED_ARGUMENT, RANGER_AUTH_TYPE));
    Preconditions.checkArgument(
        properties.containsKey(RANGER_USERNAME),
        String.format(ErrorMessages.MISSING_REQUIRED_ARGUMENT, RANGER_USERNAME));
    Preconditions.checkArgument(
        properties.containsKey(RANGER_PASSWORD),
        String.format(ErrorMessages.MISSING_REQUIRED_ARGUMENT, RANGER_PASSWORD));
    Preconditions.checkArgument(
        properties.get(RANGER_ADMIN_URL) != null,
        String.format(ErrorMessages.MISSING_REQUIRED_ARGUMENT, RANGER_ADMIN_URL));
    Preconditions.checkArgument(
        properties.get(RANGER_AUTH_TYPE) != null,
        String.format(ErrorMessages.MISSING_REQUIRED_ARGUMENT, RANGER_AUTH_TYPE));
    Preconditions.checkArgument(
        properties.get(RANGER_USERNAME) != null,
        String.format(ErrorMessages.MISSING_REQUIRED_ARGUMENT, RANGER_USERNAME));
    Preconditions.checkArgument(
        properties.get(RANGER_PASSWORD) != null,
        String.format(ErrorMessages.MISSING_REQUIRED_ARGUMENT, RANGER_PASSWORD));

    Preconditions.checkArgument(
        properties.get(RANGER_SERVICE_NAME) != null,
        String.format(ErrorMessages.MISSING_REQUIRED_ARGUMENT, RANGER_SERVICE_NAME));
  }
}
