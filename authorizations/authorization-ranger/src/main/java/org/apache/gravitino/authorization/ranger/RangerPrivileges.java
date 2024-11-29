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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.connector.authorization.AuthorizationPrivilege;

public class RangerPrivileges {
  /** Ranger Hive privileges enumeration. */
  public enum RangerHadoopSQLPrivilege implements AuthorizationPrivilege {
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

    RangerHadoopSQLPrivilege(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Privilege.Condition condition() {
      return null;
    }

    @Override
    public boolean equalsTo(String value) {
      return name.equalsIgnoreCase(value);
    }
  }

  public static class RangerHadoopSQLPrivilegeImpl implements AuthorizationPrivilege {
    private AuthorizationPrivilege rangerHivePrivilege;
    private Privilege.Condition condition;

    public RangerHadoopSQLPrivilegeImpl(
        AuthorizationPrivilege rangerHivePrivilege, Privilege.Condition condition) {
      this.rangerHivePrivilege = rangerHivePrivilege;
      this.condition = condition;
    }

    @Override
    public String getName() {
      return rangerHivePrivilege.getName();
    }

    @Override
    public Privilege.Condition condition() {
      return condition;
    }

    @Override
    public boolean equalsTo(String value) {
      return rangerHivePrivilege.equalsTo(value);
    }
  }

  /** Ranger HDFS privileges enumeration. */
  public enum RangerHdfsPrivilege implements AuthorizationPrivilege {
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
    public Privilege.Condition condition() {
      return null;
    }

    @Override
    public boolean equalsTo(String value) {
      return name.equalsIgnoreCase(value);
    }
  }

  static List<Class<? extends Enum<? extends AuthorizationPrivilege>>> allRangerPrivileges =
      Lists.newArrayList(RangerHadoopSQLPrivilege.class, RangerHdfsPrivilege.class);

  public static AuthorizationPrivilege valueOf(String name) {
    Preconditions.checkArgument(name != null, "Privilege name string cannot be null!");

    String strPrivilege = name.trim().toLowerCase();
    for (Class<? extends Enum<? extends AuthorizationPrivilege>> enumClass : allRangerPrivileges) {
      for (Enum<? extends AuthorizationPrivilege> privilege : enumClass.getEnumConstants()) {
        if (((AuthorizationPrivilege) privilege).equalsTo(strPrivilege)) {
          return (AuthorizationPrivilege) privilege;
        }
      }
    }
    throw new IllegalArgumentException("Unknown privilege name: " + name);
  }
}
