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

import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.Privilege;

public enum JdbcPrivilege implements AuthorizationPrivilege {
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

  JdbcPrivilege(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Privilege.Condition condition() {
    return Privilege.Condition.ALLOW;
  }

  @Override
  public boolean equalsTo(String value) {
    return name.equals(value);
  }
}