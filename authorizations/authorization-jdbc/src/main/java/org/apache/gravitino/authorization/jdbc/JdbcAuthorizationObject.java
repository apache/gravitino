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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;

/**
 * JdbcAuthorizationObject is used for translating securable object to authorization securable
 * object. JdbcAuthorizationObject has the database and table name. When table name is null, the
 * object represents a database. The database can't be null.
 */
public class JdbcAuthorizationObject implements AuthorizationSecurableObject {

  public static final String ALL = "*";
  private String database;
  private String table;

  List<AuthorizationPrivilege> privileges;

  JdbcAuthorizationObject(String database, String table, List<AuthorizationPrivilege> privileges) {
    Preconditions.checkNotNull(database, "Jdbc authorization object database can't null");
    this.database = database;
    this.table = table;
    this.privileges = privileges;
  }

  @Nullable
  @Override
  public String parent() {
    if (table != null) {
      return database;
    }

    return null;
  }

  @Override
  public String name() {
    if (table != null) {
      return table;
    }

    return database;
  }

  @Override
  public List<String> names() {
    List<String> names = Lists.newArrayList();
    names.add(database);
    if (table != null) {
      names.add(table);
    }
    return names;
  }

  @Override
  public Type type() {
    if (table != null) {
      return () -> MetadataObject.Type.TABLE;
    }
    return () -> MetadataObject.Type.SCHEMA;
  }

  @Override
  public void validateAuthorizationMetadataObject() throws IllegalArgumentException {}

  @Override
  public List<AuthorizationPrivilege> privileges() {
    return privileges;
  }
}
