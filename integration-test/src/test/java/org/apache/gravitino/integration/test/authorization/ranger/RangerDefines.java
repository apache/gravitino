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
package org.apache.gravitino.integration.test.authorization.ranger;

import org.apache.ranger.plugin.util.SearchFilter;

public class RangerDefines {
  // In the Ranger 2.4.0
  // security-admin/src/main/java/org/apache/ranger/service/RangerServiceDefService.java:L43
  public static final String IMPLICIT_CONDITION_EXPRESSION_NAME = "_expression";

  // In the Ranger 2.4.0
  // security-admin/src/main/java/org/apache/ranger/common/RangerSearchUtil.java:L159
  public static final String SEARCH_FILTER_SERVICE_NAME = SearchFilter.SERVICE_NAME;
  public static final String RESOURCE_DATABASE = "database"; // Hive resource database name
  public static final String RESOURCE_TABLE = "table"; // Hive resource table name
  public static final String RESOURCE_COLUMN = "column"; // Hive resource column name
  public static final String RESOURCE_PATH = "path"; // HDFS resource path name
  public static final String SEARCH_FILTER_DATABASE =
      SearchFilter.RESOURCE_PREFIX + RESOURCE_DATABASE;
  public static final String SEARCH_FILTER_TABLE = SearchFilter.RESOURCE_PREFIX + RESOURCE_TABLE;
  public static final String SEARCH_FILTER_COLUMN = SearchFilter.RESOURCE_PREFIX + RESOURCE_COLUMN;
  public static final String SEARCH_FILTER_PATH = SearchFilter.RESOURCE_PREFIX + RESOURCE_PATH;
  public static final String SERVICE_TYPE_HDFS = "hdfs"; // HDFS service type
  public static final String SERVICE_TYPE_HIVE = "hive"; // Hive service type
  public static final String OWNER_USER = "{OWNER}"; // {OWNER}: resource owner user variable
  public static final String CURRENT_USER = "{USER}"; // {USER}: current user variable
  public static final String PUBLIC_GROUP = "public"; // public group
  public static final String ACCESS_TYPE_HDFS_READ = "read"; // Read access type in the HDFS
  public static final String ACCESS_TYPE_HDFS_WRITE = "write"; // Write access type in the HDFS
  public static final String ACCESS_TYPE_HDFS_EXECUTE =
      "execute"; // execute access type in the HDFS
  public static final String ACCESS_TYPE_HIVE_ALL = "all"; // All access type in the Hive
  public static final String ACCESS_TYPE_HIVE_SELECT = "select"; // Select access type in the Hive
}
