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
package org.apache.gravitino.authorization.jdbc;

import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.Privilege;

public class JdbcPrivilege implements AuthorizationPrivilege {

  private static final JdbcPrivilege SELECT_PRIVILEGE = new JdbcPrivilege(Type.SELECT);
  private static final JdbcPrivilege INSERT_PRIVILEGE = new JdbcPrivilege(Type.INSERT);
  private static final JdbcPrivilege UPDATE_PRIVILEGE = new JdbcPrivilege(Type.UPDATE);
  private static final JdbcPrivilege ALTER_PRIVILEGE = new JdbcPrivilege(Type.ALTER);
  private static final JdbcPrivilege DELETE_PRIVILEGE = new JdbcPrivilege(Type.DELETE);
  private static final JdbcPrivilege ALL_PRIVILEGE = new JdbcPrivilege(Type.ALL);
  private static final JdbcPrivilege CREATE_PRIVILEGE = new JdbcPrivilege(Type.CREATE);
  private static final JdbcPrivilege DROP_PRIVILEGE = new JdbcPrivilege(Type.DROP);
  private static final JdbcPrivilege USAGE_PRIVILEGE = new JdbcPrivilege(Type.USAGE);

  private final Type type;

  private JdbcPrivilege(Type type) {
    this.type = type;
  }

  @Override
  public String getName() {
    return type.getName();
  }

  @Override
  public Privilege.Condition condition() {
    return Privilege.Condition.ALLOW;
  }

  @Override
  public boolean equalsTo(String value) {
    return false;
  }

  static JdbcPrivilege valueOf(Type type) {
    switch (type) {
      case CREATE:
        return CREATE_PRIVILEGE;
      case DELETE:
        return DELETE_PRIVILEGE;
      case ALL:
        return ALL_PRIVILEGE;
      case DROP:
        return DROP_PRIVILEGE;
      case ALTER:
        return ALTER_PRIVILEGE;
      case INSERT:
        return INSERT_PRIVILEGE;
      case UPDATE:
        return UPDATE_PRIVILEGE;
      case SELECT:
        return SELECT_PRIVILEGE;
      case USAGE:
        return USAGE_PRIVILEGE;
      default:
        throw new IllegalArgumentException(String.format("Unsupported parameter type %s", type));
    }
  }

  public enum Type {
    SELECT("SELECT"),
    INSERT("INSERT"),
    UPDATE("UPDATE"),
    ALTER("ALTER"),
    DELETE("DELETE"),
    ALL("ALL PRIVILEGES"),
    CREATE("CREATE"),
    DROP("DROP"),
    USAGE("USAGE");

    private final String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
