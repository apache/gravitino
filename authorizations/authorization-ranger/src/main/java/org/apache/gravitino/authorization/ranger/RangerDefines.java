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
package org.apache.gravitino.authorization.ranger;

import org.apache.ranger.plugin.util.SearchFilter;

public class RangerDefines {
  // In the Ranger 2.4.0
  // apache/ranger/security-admin/src/main/java/org/apache/ranger/service/RangerServiceDefService.java:L43
  public static final String IMPLICIT_CONDITION_EXPRESSION_NAME = "_expression";

  // In the Ranger 2.4.0
  // apache/ranger/security-admin/src/main/java/org/apache/ranger/common/RangerSearchUtil.java:L159
  // Search filter constants
  public static final String SEARCH_FILTER_SERVICE_NAME = SearchFilter.SERVICE_NAME;
  // Hive resource database name
  public static final String RESOURCE_DATABASE = "database";
  // Hive resource table name
  public static final String RESOURCE_TABLE = "table";
  // Hive resource column name
  public static final String RESOURCE_COLUMN = "column";
  // HDFS resource path name
  public static final String RESOURCE_PATH = "path";
  // Search filter prefix database constants
  public static final String SEARCH_FILTER_DATABASE =
      SearchFilter.RESOURCE_PREFIX + RESOURCE_DATABASE;
  // Search filter prefix table constants
  public static final String SEARCH_FILTER_TABLE = SearchFilter.RESOURCE_PREFIX + RESOURCE_TABLE;
  // Search filter prefix column constants
  public static final String SEARCH_FILTER_COLUMN = SearchFilter.RESOURCE_PREFIX + RESOURCE_COLUMN;
  // Search filter prefix file path constants
  public static final String SEARCH_FILTER_PATH = SearchFilter.RESOURCE_PREFIX + RESOURCE_PATH;
  // Ranger service type HDFS
  public static final String SERVICE_TYPE_HDFS = "hdfs"; // HDFS service type
  // Ranger service type Hive
  public static final String SERVICE_TYPE_HIVE = "hive"; // Hive service type
  // {OWNER}: resource owner user variable
  public static final String OWNER_USER = "{OWNER}";
  // {USER}: current user variable
  public static final String CURRENT_USER = "{USER}";
  // public group
  public static final String PUBLIC_GROUP = "public";
  // Read access type in the HDFS
  public static final String ACCESS_TYPE_HDFS_READ = "read";
  // Write access type in the HDFS
  public static final String ACCESS_TYPE_HDFS_WRITE = "write";
  // execute access type in the HDFS
  public static final String ACCESS_TYPE_HDFS_EXECUTE = "execute";
  // All access type in the Hive
  public static final String ACCESS_TYPE_HIVE_ALL = "all";
  // Select access type in the Hive
  public static final String ACCESS_TYPE_HIVE_SELECT = "select";
  // update access type in the Hive
  public static final String ACCESS_TYPE_HIVE_UPDATE = "update";
  // create access type in the Hive
  public static final String ACCESS_TYPE_HIVE_CREATE = "create";
  // drop access type in the Hive
  public static final String ACCESS_TYPE_HIVE_DROP = "drop";
  // alter access type in the Hive
  public static final String ACCESS_TYPE_HIVE_ALTER = "alter";
  // index access type in the Hive
  public static final String ACCESS_TYPE_HIVE_INDEX = "index";
  // lock access type in the Hive
  public static final String ACCESS_TYPE_HIVE_LOCK = "lock";
  // read access type in the Hive
  public static final String ACCESS_TYPE_HIVE_READ = "read";
  // write access type in the Hive
  public static final String ACCESS_TYPE_HIVE_WRITE = "write";
  // repladmin access type in the Hive
  public static final String ACCESS_TYPE_HIVE_REPLADMIN = "repladmin";
  // serviceadmin access type in the Hive
  public static final String ACCESS_TYPE_HIVE_SERVICEADMIN = "serviceadmin";
}
