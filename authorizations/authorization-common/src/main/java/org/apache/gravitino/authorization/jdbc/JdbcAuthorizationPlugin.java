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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Unstable;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JdbcSQLBasedAuthorizationPlugin is the base class for all JDBC-based authorization plugins. For
 * example, JdbcHiveAuthorizationPlugin is the JDBC-based authorization plugin for Hive. Different
 * JDBC-based authorization plugins can inherit this class and implement their own SQL statements.
 */
@Unstable
public abstract class JdbcAuthorizationPlugin implements AuthorizationPlugin, JdbcAuthorizationSQL {

  private static final String GROUP_PREFIX = "GRAVITINO_GROUP_";
  private static final Logger LOG = LoggerFactory.getLogger(JdbcAuthorizationPlugin.class);

  protected BasicDataSource dataSource;
  protected JdbcSecurableObjectMappingProvider mappingProvider;

  protected JdbcAuthorizationPlugin(Map<String, String> config) {
    // Initialize the data source
    dataSource = new BasicDataSource();
    JdbcAuthorizationProperties jdbcAuthProperties = new JdbcAuthorizationProperties(config);
    jdbcAuthProperties.validate();

    String jdbcUrl = config.get(JdbcAuthorizationProperties.JDBC_URL);
    dataSource.setUrl(jdbcUrl);
    dataSource.setDriverClassName(config.get(JdbcAuthorizationProperties.JDBC_DRIVER));
    dataSource.setUsername(config.get(JdbcAuthorizationProperties.JDBC_USERNAME));
    dataSource.setPassword(config.get(JdbcAuthorizationProperties.JDBC_PASSWORD));
    dataSource.setDefaultAutoCommit(true);
    dataSource.setMaxTotal(20);
    dataSource.setMaxIdle(5);
    dataSource.setMinIdle(0);
    dataSource.setLogAbandoned(true);
    dataSource.setRemoveAbandonedOnBorrow(true);
    dataSource.setTestOnBorrow(BaseObjectPoolConfig.DEFAULT_TEST_ON_BORROW);
    dataSource.setTestWhileIdle(BaseObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE);
    dataSource.setNumTestsPerEvictionRun(BaseObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN);
    dataSource.setTestOnReturn(BaseObjectPoolConfig.DEFAULT_TEST_ON_RETURN);
    dataSource.setLifo(BaseObjectPoolConfig.DEFAULT_LIFO);
    mappingProvider = new JdbcSecurableObjectMappingProvider();

    try (Connection ignored = getConnection()) {
      LOG.info("Load the JDBC driver first");
    } catch (SQLException se) {
      LOG.error("Jdbc authorization plugin exception: ", se);
      throw toAuthorizationPluginException(se);
    }
  }

  @Override
  public void close() throws IOException {
    if (dataSource != null) {
      try {
        dataSource.close();
        dataSource = null;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Boolean onMetadataUpdated(MetadataObjectChange... changes) throws RuntimeException {
    // This interface mainly handles the metadata object rename change and delete change.
    // The privilege for JdbcSQLBasedAuthorizationPlugin will be renamed or deleted automatically.
    // We don't need to do any other things.
    return true;
  }

  @Override
  public Boolean onRoleCreated(Role role) throws AuthorizationPluginException {
    List<String> sqls = getCreateRoleSQL(role.name());
    boolean createdNewly = true;
    for (String sql : sqls) {
      if (!executeUpdateSQL(sql, "already exists")) {
        createdNewly = false;
      }
    }

    if (!createdNewly) {
      return true;
    }

    if (role.securableObjects() != null) {
      for (SecurableObject object : role.securableObjects()) {
        onRoleUpdated(role, RoleChange.addSecurableObject(role.name(), object));
      }
    }

    return true;
  }

  @Override
  public Boolean onRoleAcquired(Role role) throws AuthorizationPluginException {
    throw new UnsupportedOperationException("Doesn't support to acquired a role");
  }

  @Override
  public Boolean onRoleDeleted(Role role) throws AuthorizationPluginException {
    List<String> sqls = getDropRoleSQL(role.name());
    for (String sql : sqls) {
      executeUpdateSQL(sql);
    }
    return true;
  }

  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes)
      throws AuthorizationPluginException {
    for (RoleChange change : changes) {
      if (change instanceof RoleChange.AddSecurableObject) {
        SecurableObject object = ((RoleChange.AddSecurableObject) change).getSecurableObject();
        grantObjectPrivileges(role, object);
      } else if (change instanceof RoleChange.RemoveSecurableObject) {
        SecurableObject object = ((RoleChange.RemoveSecurableObject) change).getSecurableObject();
        revokeObjectPrivileges(role, object);
      } else if (change instanceof RoleChange.UpdateSecurableObject) {
        RoleChange.UpdateSecurableObject updateChange = (RoleChange.UpdateSecurableObject) change;
        SecurableObject addObject = updateChange.getNewSecurableObject();
        SecurableObject removeObject = updateChange.getSecurableObject();
        revokeObjectPrivileges(role, removeObject);
        grantObjectPrivileges(role, addObject);
      } else {
        throw new IllegalArgumentException(
            String.format("RoleChange is not supported - %s", change));
      }
    }
    return true;
  }

  @Override
  public Boolean onGrantedRolesToUser(List<Role> roles, User user)
      throws AuthorizationPluginException {

    for (Role role : roles) {
      onRoleCreated(role);
      List<String> sqls = getGrantRoleSQL(role.name(), "USER", user.name());
      for (String sql : sqls) {
        executeUpdateSQL(sql);
      }
    }
    return true;
  }

  @Override
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user)
      throws AuthorizationPluginException {

    for (Role role : roles) {
      onRoleCreated(role);
      List<String> sqls = getRevokeRoleSQL(role.name(), "USER", user.name());
      for (String sql : sqls) {
        executeUpdateSQL(sql);
      }
    }
    return true;
  }

  @Override
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {

    for (Role role : roles) {
      onRoleCreated(role);
      List<String> sqls =
          getGrantRoleSQL(role.name(), "USER", String.format("%s%s", GROUP_PREFIX, group.name()));
      for (String sql : sqls) {
        executeUpdateSQL(sql);
      }
    }
    return true;
  }

  @Override
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {

    for (Role role : roles) {
      onRoleCreated(role);
      List<String> sqls =
          getRevokeRoleSQL(role.name(), "USER", String.format("%s%s", GROUP_PREFIX, group.name()));
      for (String sql : sqls) {
        executeUpdateSQL(sql);
      }
    }
    return true;
  }

  @Override
  public Boolean onUserAdded(User user) throws AuthorizationPluginException {
    List<String> sqls = getCreateUserSQL(user.name());
    for (String sql : sqls) {
      executeUpdateSQL(sql);
    }
    return true;
  }

  @Override
  public Boolean onUserRemoved(User user) throws AuthorizationPluginException {
    List<String> sqls = getDropUserSQL(user.name());
    for (String sql : sqls) {
      executeUpdateSQL(sql);
    }
    return true;
  }

  @Override
  public Boolean onUserAcquired(User user) throws AuthorizationPluginException {
    throw new UnsupportedOperationException("Doesn't support to acquired a user");
  }

  @Override
  public Boolean onGroupAdded(Group group) throws AuthorizationPluginException {
    String name = String.format("%s%s", GROUP_PREFIX, group.name());
    List<String> sqls = getCreateUserSQL(name);
    for (String sql : sqls) {
      executeUpdateSQL(sql);
    }
    return true;
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws AuthorizationPluginException {
    String name = String.format("%s%s", GROUP_PREFIX, group.name());
    List<String> sqls = getDropUserSQL(name);
    for (String sql : sqls) {
      executeUpdateSQL(sql);
    }
    return true;
  }

  @Override
  public Boolean onGroupAcquired(Group group) throws AuthorizationPluginException {
    throw new UnsupportedOperationException("Doesn't support to acquired a group");
  }

  @Override
  public Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner)
      throws AuthorizationPluginException {
    if (newOwner.type() == Owner.Type.USER) {
      onUserAdded(
          UserEntity.builder()
              .withName(newOwner.name())
              .withId(0L)
              .withAuditInfo(AuditInfo.EMPTY)
              .build());
    } else if (newOwner.type() == Owner.Type.GROUP) {
      onGroupAdded(
          GroupEntity.builder()
              .withName(newOwner.name())
              .withId(0L)
              .withAuditInfo(AuditInfo.EMPTY)
              .build());
    } else {
      throw new IllegalArgumentException(
          String.format("Don't support owner type %s", newOwner.type()));
    }

    List<AuthorizationSecurableObject> authObjects = mappingProvider.translateOwner(metadataObject);
    for (AuthorizationSecurableObject authObject : authObjects) {
      List<String> sqls =
          getSetOwnerSQL(
              authObject.type().metadataObjectType(), authObject.fullName(), preOwner, newOwner);
      for (String sql : sqls) {
        executeUpdateSQL(sql);
      }
    }
    return true;
  }

  @Override
  public List<String> getCreateUserSQL(String username) {
    return Lists.newArrayList(String.format("CREATE USER %s", username));
  }

  @Override
  public List<String> getDropUserSQL(String username) {
    return Lists.newArrayList(String.format("DROP USER %s", username));
  }

  @Override
  public List<String> getCreateRoleSQL(String roleName) {
    return Lists.newArrayList(String.format("CREATE ROLE %s", roleName));
  }

  @Override
  public List<String> getDropRoleSQL(String roleName) {
    return Lists.newArrayList(String.format("DROP ROLE %s", roleName));
  }

  @Override
  public List<String> getGrantPrivilegeSQL(
      String privilege, String objectType, String objectName, String roleName) {
    return Lists.newArrayList(
        String.format("GRANT %s ON %s %s TO ROLE %s", privilege, objectType, objectName, roleName));
  }

  @Override
  public List<String> getRevokePrivilegeSQL(
      String privilege, String objectType, String objectName, String roleName) {
    return Lists.newArrayList(
        String.format(
            "REVOKE %s ON %s %s FROM ROLE %s", privilege, objectType, objectName, roleName));
  }

  @Override
  public List<String> getGrantRoleSQL(String roleName, String grantorType, String grantorName) {
    return Lists.newArrayList(
        String.format("GRANT ROLE %s TO %s %s", roleName, grantorType, grantorName));
  }

  @Override
  public List<String> getRevokeRoleSQL(String roleName, String revokerType, String revokerName) {
    return Lists.newArrayList(
        String.format("REVOKE ROLE %s FROM %s %s", roleName, revokerType, revokerName));
  }

  @VisibleForTesting
  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  public void executeUpdateSQL(String sql) {
    executeUpdateSQL(sql, null);
  }

  /**
   * Convert the object name contains `*` to a list of AuthorizationSecurableObject.
   *
   * @param object The object contains the name with `*` to be converted
   * @return The list of AuthorizationSecurableObject
   */
  protected List<AuthorizationSecurableObject> convertResourceAll(
      AuthorizationSecurableObject object) {
    List<AuthorizationSecurableObject> authObjects = Lists.newArrayList();
    authObjects.add(object);
    return authObjects;
  }

  protected List<AuthorizationPrivilege> filterUnsupportedPrivileges(
      List<AuthorizationPrivilege> privileges) {
    return privileges;
  }

  protected AuthorizationPluginException toAuthorizationPluginException(SQLException se) {
    return new AuthorizationPluginException(
        "JDBC authorization plugin fail to execute SQL, error code: %d", se.getErrorCode());
  }

  public boolean executeUpdateSQL(String sql, String ignoreErrorMsg) {
    try (final Connection connection = getConnection()) {
      try (final Statement statement = connection.createStatement()) {
        statement.executeUpdate(sql);
        return true;
      }
    } catch (SQLException se) {
      if (ignoreErrorMsg != null && se.getMessage().contains(ignoreErrorMsg)) {
        return false;
      }
      LOG.error("JDBC authorization plugin exception: ", se);
      throw toAuthorizationPluginException(se);
    }
  }

  private void grantObjectPrivileges(Role role, SecurableObject object) {
    List<AuthorizationSecurableObject> authObjects = mappingProvider.translatePrivilege(object);
    for (AuthorizationSecurableObject authObject : authObjects) {
      List<AuthorizationSecurableObject> convertedObjects = Lists.newArrayList();
      if (authObject.name().equals(JdbcSecurableObject.ALL)) {
        convertedObjects.addAll(convertResourceAll(authObject));
      } else {
        convertedObjects.add(authObject);
      }

      for (AuthorizationSecurableObject convertedObject : convertedObjects) {
        List<String> privileges =
            filterUnsupportedPrivileges(authObject.privileges()).stream()
                .map(AuthorizationPrivilege::getName)
                .collect(Collectors.toList());
        // We don't grant the privileges in one SQL, because some privilege has been granted, it
        // will cause the failure of the SQL. So we grant the privileges one by one.
        for (String privilege : privileges) {
          List<String> sqls =
              getGrantPrivilegeSQL(
                  privilege,
                  convertedObject.metadataObjectType().name(),
                  convertedObject.fullName(),
                  role.name());
          for (String sql : sqls) {
            executeUpdateSQL(sql, "is already granted");
          }
        }
      }
    }
  }

  private void revokeObjectPrivileges(Role role, SecurableObject removeObject) {
    List<AuthorizationSecurableObject> authObjects =
        mappingProvider.translatePrivilege(removeObject);
    for (AuthorizationSecurableObject authObject : authObjects) {
      List<AuthorizationSecurableObject> convertedObjects = Lists.newArrayList();
      if (authObject.name().equals(JdbcSecurableObject.ALL)) {
        convertedObjects.addAll(convertResourceAll(authObject));
      } else {
        convertedObjects.add(authObject);
      }

      for (AuthorizationSecurableObject convertedObject : convertedObjects) {
        List<String> privileges =
            filterUnsupportedPrivileges(authObject.privileges()).stream()
                .map(AuthorizationPrivilege::getName)
                .collect(Collectors.toList());
        for (String privilege : privileges) {
          // We don't revoke the privileges in one SQL, because some privilege has been revoked, it
          // will cause the failure of the SQL. So we revoke the privileges one by one.
          List<String> sqls =
              getRevokePrivilegeSQL(
                  privilege,
                  convertedObject.metadataObjectType().name(),
                  convertedObject.fullName(),
                  role.name());
          for (String sql : sqls) {
            executeUpdateSQL(sql, "Cannot find privilege Privilege");
          }
        }
      }
    }
  }
}
