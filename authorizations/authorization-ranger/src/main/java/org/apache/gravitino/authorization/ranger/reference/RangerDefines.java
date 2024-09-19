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
package org.apache.gravitino.authorization.ranger.reference;

public class RangerDefines {
  // Ranger service type HDFS
  public static final String SERVICE_TYPE_HDFS = "hdfs"; // HDFS service type
  // Ranger service type Hive
  public static final String SERVICE_TYPE_HIVE = "hive"; // Hive service type

  // In the Ranger 2.4.0
  // agents-common/src/main/java/org/apache/ranger/plugin/util/SearchFilter.java
  // {OWNER}: resource owner user variable
  public static final String OWNER_USER = "{OWNER}";
  // {USER}: current user variable
  public static final String CURRENT_USER = "{USER}";
  // public group
  public static final String PUBLIC_GROUP = "public";

  public enum PolicyResource {
    // In the Ranger 2.4.0 agents-common/src/main/resources/service-defs/ranger-servicedef-hive.json
    DATABASE("database"),
    TABLE("table"),
    COLUMN("column");

    private final String name;

    PolicyResource(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
