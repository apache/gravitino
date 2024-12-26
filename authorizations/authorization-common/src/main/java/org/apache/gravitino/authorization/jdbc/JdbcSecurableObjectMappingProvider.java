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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationPrivilegesMappingProvider;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;

/**
 * JdbcSecurableObjectMappingProvider is used for translating securable object to authorization
 * securable object.
 */
public class JdbcSecurableObjectMappingProvider implements AuthorizationPrivilegesMappingProvider {

  private final Map<Privilege.Name, Set<AuthorizationPrivilege>> privilegeMapping =
      ImmutableMap.of(
          Privilege.Name.CREATE_TABLE, Sets.newHashSet(JdbcPrivilege.CREATE),
          Privilege.Name.CREATE_SCHEMA, Sets.newHashSet(JdbcPrivilege.CREATE),
          Privilege.Name.SELECT_TABLE, Sets.newHashSet(JdbcPrivilege.SELECT),
          Privilege.Name.MODIFY_TABLE,
              Sets.newHashSet(
                  JdbcPrivilege.SELECT,
                  JdbcPrivilege.UPDATE,
                  JdbcPrivilege.DELETE,
                  JdbcPrivilege.INSERT,
                  JdbcPrivilege.ALTER),
          Privilege.Name.USE_SCHEMA, Sets.newHashSet(JdbcPrivilege.USAGE));

  private final Map<Privilege.Name, MetadataObject.Type> privilegeScopeMapping =
      ImmutableMap.of(
          Privilege.Name.CREATE_TABLE, MetadataObject.Type.TABLE,
          Privilege.Name.CREATE_SCHEMA, MetadataObject.Type.SCHEMA,
          Privilege.Name.SELECT_TABLE, MetadataObject.Type.TABLE,
          Privilege.Name.MODIFY_TABLE, MetadataObject.Type.TABLE,
          Privilege.Name.USE_SCHEMA, MetadataObject.Type.SCHEMA);

  private final Set<AuthorizationPrivilege> ownerPrivileges = ImmutableSet.of();

  private final Set<MetadataObject.Type> allowObjectTypes =
      ImmutableSet.of(
          MetadataObject.Type.METALAKE,
          MetadataObject.Type.CATALOG,
          MetadataObject.Type.SCHEMA,
          MetadataObject.Type.TABLE);

  @Override
  public Map<Privilege.Name, Set<AuthorizationPrivilege>> privilegesMappingRule() {
    return privilegeMapping;
  }

  @Override
  public Set<AuthorizationPrivilege> ownerMappingRule() {
    return ownerPrivileges;
  }

  @Override
  public Set<Privilege.Name> allowPrivilegesRule() {
    return privilegeMapping.keySet();
  }

  @Override
  public Set<MetadataObject.Type> allowMetadataObjectTypesRule() {
    return allowObjectTypes;
  }

  @Override
  public List<AuthorizationSecurableObject> translatePrivilege(SecurableObject securableObject) {
    List<AuthorizationSecurableObject> authObjects = Lists.newArrayList();
    List<AuthorizationPrivilege> databasePrivileges = Lists.newArrayList();
    List<AuthorizationPrivilege> tablePrivileges = Lists.newArrayList();
    JdbcSecurableObject databaseObject;
    JdbcSecurableObject tableObject;
    switch (securableObject.type()) {
      case METALAKE:
      case CATALOG:
        convertJdbcPrivileges(securableObject, databasePrivileges, tablePrivileges);

        if (!databasePrivileges.isEmpty()) {
          databaseObject =
              JdbcSecurableObject.create(JdbcSecurableObject.ALL, null, databasePrivileges);
          authObjects.add(databaseObject);
        }

        if (!tablePrivileges.isEmpty()) {
          tableObject =
              JdbcSecurableObject.create(
                  JdbcSecurableObject.ALL, JdbcSecurableObject.ALL, tablePrivileges);
          authObjects.add(tableObject);
        }
        break;

      case SCHEMA:
        convertJdbcPrivileges(securableObject, databasePrivileges, tablePrivileges);
        if (!databasePrivileges.isEmpty()) {
          databaseObject =
              JdbcSecurableObject.create(securableObject.name(), null, databasePrivileges);
          authObjects.add(databaseObject);
        }

        if (!tablePrivileges.isEmpty()) {
          tableObject =
              JdbcSecurableObject.create(
                  securableObject.name(), JdbcSecurableObject.ALL, tablePrivileges);
          authObjects.add(tableObject);
        }
        break;

      case TABLE:
        convertJdbcPrivileges(securableObject, databasePrivileges, tablePrivileges);
        if (!tablePrivileges.isEmpty()) {
          MetadataObject metadataObject =
              MetadataObjects.parse(securableObject.parent(), MetadataObject.Type.SCHEMA);
          tableObject =
              JdbcSecurableObject.create(
                  metadataObject.name(), securableObject.name(), tablePrivileges);
          authObjects.add(tableObject);
        }
        break;

      default:
        throw new IllegalArgumentException(
            String.format("Don't support metadata object type %s", securableObject.type()));
    }

    return authObjects;
  }

  @Override
  public List<AuthorizationSecurableObject> translateOwner(MetadataObject metadataObject) {
    List<AuthorizationSecurableObject> objects = Lists.newArrayList();
    switch (metadataObject.type()) {
      case METALAKE:
      case CATALOG:
        objects.add(
            JdbcSecurableObject.create(
                JdbcSecurableObject.ALL, null, Lists.newArrayList(JdbcPrivilege.ALL)));
        objects.add(
            JdbcSecurableObject.create(
                JdbcSecurableObject.ALL,
                JdbcSecurableObject.ALL,
                Lists.newArrayList(JdbcPrivilege.ALL)));
        break;
      case SCHEMA:
        objects.add(
            JdbcSecurableObject.create(
                metadataObject.name(), null, Lists.newArrayList(JdbcPrivilege.ALL)));
        objects.add(
            JdbcSecurableObject.create(
                metadataObject.name(),
                JdbcSecurableObject.ALL,
                Lists.newArrayList(JdbcPrivilege.ALL)));
        break;
      case TABLE:
        MetadataObject schema =
            MetadataObjects.parse(metadataObject.parent(), MetadataObject.Type.SCHEMA);
        objects.add(
            JdbcSecurableObject.create(
                schema.name(), metadataObject.name(), Lists.newArrayList(JdbcPrivilege.ALL)));
        break;
      default:
        throw new IllegalArgumentException(
            "Don't support metadata object type " + metadataObject.type());
    }
    return objects;
  }

  @Override
  public AuthorizationMetadataObject translateMetadataObject(MetadataObject metadataObject) {
    throw new UnsupportedOperationException("Not supported");
  }

  private void convertJdbcPrivileges(
      SecurableObject securableObject,
      List<AuthorizationPrivilege> databasePrivileges,
      List<AuthorizationPrivilege> tablePrivileges) {
    for (Privilege privilege : securableObject.privileges()) {
      if (privilegeScopeMapping.get(privilege.name()) == MetadataObject.Type.SCHEMA) {
        databasePrivileges.addAll(privilegeMapping.get(privilege.name()));
      } else if (privilegeScopeMapping.get(privilege.name()) == MetadataObject.Type.TABLE) {
        tablePrivileges.addAll(privilegeMapping.get(privilege.name()));
      }
    }
  }
}
