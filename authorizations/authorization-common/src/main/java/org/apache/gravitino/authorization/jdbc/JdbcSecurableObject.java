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

import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;

/**
 * JdbcAuthorizationObject is used for translating securable object to authorization securable
 * object. JdbcAuthorizationObject has the database and table name. When table name is null, the
 * object represents a database. The database can't be null.
 */
public class JdbcSecurableObject extends JdbcMetadataObject
    implements AuthorizationSecurableObject {

  public static final String ALL = "*";

  List<AuthorizationPrivilege> privileges;

  private JdbcSecurableObject(
      String parent,
      String name,
      JdbcMetadataObject.Type type,
      List<AuthorizationPrivilege> privileges) {
    super(parent, name, type);
    this.privileges = privileges;
  }

  public static JdbcSecurableObject create(
      String schema, String table, List<AuthorizationPrivilege> privileges) {
    String parent = table == null ? null : schema;
    String name = table == null ? schema : table;
    JdbcMetadataObject.Type type =
        table == null
            ? JdbcMetadataObject.Type.fromMetadataType(MetadataObject.Type.SCHEMA)
            : JdbcMetadataObject.Type.fromMetadataType(MetadataObject.Type.TABLE);

    JdbcSecurableObject object = new JdbcSecurableObject(parent, name, type, privileges);
    object.validateAuthorizationMetadataObject();
    return object;
  }

  @Override
  public List<AuthorizationPrivilege> privileges() {
    return privileges;
  }
}
