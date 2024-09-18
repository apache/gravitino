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

/** RangerPrivilege interface is used to define the Ranger privileges. */
public interface RangerPrivilege {
  String getName();

  boolean isEquals(String value);

  /** Ranger Hive privileges enumeration. */
  enum RangerHivePrivilege implements RangerPrivilege {
    ALL("all"),
    SELECT("select"),
    UPDATE("update"),
    CREATE("create"),
    DROP("drop"),
    ALTER("alter"),
    INDEX("index"),
    LOCK("lock"),
    READ("read"),
    WRITE("write"),
    REPLADMIN("repladmin"),
    SERVICEADMIN("serviceadmin");

    private final String name; // Access a type in the Ranger policy item

    RangerHivePrivilege(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public boolean isEquals(String value) {
      return name.equalsIgnoreCase(value);
    }
  }

  /** Ranger HDFS privileges enumeration. */
  enum RangerHdfsPrivilege implements RangerPrivilege {
    READ("read"),
    WRITE("write"),
    EXECUTE("execute");

    private final String name; // Access a type in the Ranger policy item

    RangerHdfsPrivilege(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public boolean isEquals(String value) {
      return name.equalsIgnoreCase(value);
    }
  }
}
