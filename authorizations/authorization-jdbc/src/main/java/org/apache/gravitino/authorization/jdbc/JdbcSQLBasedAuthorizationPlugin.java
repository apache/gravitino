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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JdbcSQLBasedAuthorizationPlugin is the base class for all JDBC-based authorization plugins. For
 * example, JdbcHiveAuthorizationPlugin is the JDBC-based authorization plugin for Hive. Different
 * JDBC-based authorization plugins can inherit this class and implement their own SQL statements.
 */
@Unstable
abstract class JdbcSQLBasedAuthorizationPlugin
    implements AuthorizationPlugin, JdbcAuthorizationSQL {

  private static final String CONFIG_PREFIX = "authorization.jdbc.";
  public static final String JDBC_DRIVER = CONFIG_PREFIX + "driver";
  public static final String JDBC_URL = CONFIG_PREFIX + "url";
  public static final String JDBC_USERNAME = CONFIG_PREFIX + "username";
  public static final String JDBC_PASSWORD = CONFIG_PREFIX + "password";

  private static final String GROUP_PREFIX = "GRAVITINO_GROUP_";
  private static final Logger LOG = LoggerFactory.getLogger(JdbcSQLBasedAuthorizationPlugin.class);

  protected BasicDataSource dataSource;
  protected JdbcSecurableObjectMappingProvider mappingProvider;

  public JdbcSQLBasedAuthorizationPlugin(Map<String, String> config) {
    // Initialize the data source
    dataSource = new BasicDataSource();
    String jdbcUrl = config.get(JDBC_URL);
    dataSource.setUrl(jdbcUrl);
    dataSource.setDriverClassName(config.get(JDBC_DRIVER));
    dataSource.setUsername(config.get(JDBC_USERNAME));
    dataSource.setPassword(config.get(JDBC_PASSWORD));
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
  }

  @Override
  public void close() throws IOException {
    if (dataSource != null) {
      try {
        dataSource.close();
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
    beforeExecuteSQL();

    String sql = getCreateRoleSQL(role.name());
    executeUpdateSQL(sql, "already exists");

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
    beforeExecuteSQL();

    String sql = getDropRoleSQL(role.name());
    executeUpdateSQL(sql);
    return null;
  }

  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes)
      throws AuthorizationPluginException {
    beforeExecuteSQL();

    onRoleCreated(role);
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
        throw new IllegalArgumentException(String.format("Don't support RoleChange %s", change));
      }
    }
    return true;
  }

  @Override
  public Boolean onGrantedRolesToUser(List<Role> roles, User user)
      throws AuthorizationPluginException {
    beforeExecuteSQL();

    onUserAdded(user);

    for (Role role : roles) {
      onRoleCreated(role);
      String sql = getGrantRoleSQL(role.name(), "USER", user.name());
      executeUpdateSQL(sql);
    }
    return true;
  }

  @Override
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user)
      throws AuthorizationPluginException {
    beforeExecuteSQL();

    onUserAdded(user);

    for (Role role : roles) {
      onRoleCreated(role);
      String sql = getRevokeRoleSQL(role.name(), "USER", user.name());
      executeUpdateSQL(sql);
    }
    return true;
  }

  @Override
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {
    beforeExecuteSQL();

    onGroupAdded(group);

    for (Role role : roles) {
      onRoleCreated(role);
      String sql =
          getGrantRoleSQL(role.name(), "USER", String.format("%s%s", GROUP_PREFIX, group.name()));
      executeUpdateSQL(sql);
    }
    return true;
  }

  @Override
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {
    beforeExecuteSQL();

    onGroupAdded(group);

    for (Role role : roles) {
      onRoleCreated(role);
      String sql =
          getRevokeRoleSQL(role.name(), "USER", String.format("%s%s", GROUP_PREFIX, group.name()));
      executeUpdateSQL(sql);
    }
    return true;
  }

  @Override
  public Boolean onUserAdded(User user) throws AuthorizationPluginException {
    beforeExecuteSQL();

    String sql = getCreateUserSQL(user.name());
    executeUpdateSQL(sql);
    return true;
  }

  @Override
  public Boolean onUserRemoved(User user) throws AuthorizationPluginException {
    beforeExecuteSQL();

    String sql = getDropUserSQL(user.name());
    executeUpdateSQL(sql);
    return true;
  }

  @Override
  public Boolean onUserAcquired(User user) throws AuthorizationPluginException {
    throw new UnsupportedOperationException("Doesn't support to acquired a user");
  }

  @Override
  public Boolean onGroupAdded(Group group) throws AuthorizationPluginException {
    beforeExecuteSQL();

    String name = String.format("%s%s", GROUP_PREFIX, group.name());
    String sql = getCreateUserSQL(name);
    executeUpdateSQL(sql);
    return true;
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws AuthorizationPluginException {
    beforeExecuteSQL();

    String name = String.format("%s%s", GROUP_PREFIX, group.name());
    String sql = getDropUserSQL(name);
    executeUpdateSQL(sql);
    return true;
  }

  @Override
  public Boolean onGroupAcquired(Group group) throws AuthorizationPluginException {
    throw new UnsupportedOperationException("Doesn't support to acquired a group");
  }

  @Override
  public Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner)
      throws AuthorizationPluginException {
    beforeExecuteSQL();

    if (newOwner.type() == Owner.Type.USER) {
      onUserAdded(new TemporaryUser(newOwner.name()));
    } else if (newOwner.type() == Owner.Type.GROUP) {
      onGroupAdded(new TemporaryGroup(newOwner.name()));
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
  public String getCreateUserSQL(String username) {
    return String.format("CREATE USER %s", username);
  }

  @Override
  public String getDropUserSQL(String username) {
    return String.format("DROP USER %s", username);
  }

  @Override
  public String getCreateRoleSQL(String roleName) {
    return String.format("CREATE ROLE %s", roleName);
  }

  @Override
  public String getDropRoleSQL(String roleName) {
    return String.format("DROP ROLE %s", roleName);
  }

  @Override
  public String getGrantPrivilegeSQL(
      String privilege, String objectType, String objectName, String roleName) {
    return String.format(
        "GRANT %s ON %s %s TO ROLE %s", privilege, objectType, objectName, roleName);
  }

  @Override
  public String getRevokePrivilegeSQL(
      String privilege, String objectType, String objectName, String roleName) {
    return String.format(
        "REVOKE %s ON %s %s FROM ROLE %s", privilege, objectType, objectName, roleName);
  }

  @Override
  public String getGrantRoleSQL(String roleName, String grantorType, String grantorName) {
    return String.format("GRANT ROLE %s TO %s %s", roleName, grantorType, grantorName);
  }

  @Override
  public String getRevokeRoleSQL(String roleName, String revokerType, String revokerName) {
    return String.format("REVOKE ROLE %s FROM %s %s", roleName, revokerType, revokerName);
  }

  protected void beforeExecuteSQL() {
    // Do nothing by default.
  }

  @VisibleForTesting
  Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  protected void executeUpdateSQL(String sql) {
    executeUpdateSQL(sql, null);
  }

  /**
   * Convert the object name contains `*` to a list of AuthorizationSecurableObject.
   *
   * @param object The
   * @return
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
        "Jdbc authorization plugin fail to execute SQL, error code: %d", se.getErrorCode());
  }

  void executeUpdateSQL(String sql, String ignoreErrorMsg) {
    try (final Connection connection = getConnection()) {
      try (final Statement statement = connection.createStatement()) {
        statement.executeUpdate(sql);
      }
    } catch (SQLException se) {
      if (ignoreErrorMsg != null && se.getMessage().contains(ignoreErrorMsg)) {
        return;
      }
      LOG.error("Jdbc authorization plugin exception: ", se);
      throw toAuthorizationPluginException(se);
    }
  }

  private void grantObjectPrivileges(Role role, SecurableObject object) {
    List<AuthorizationSecurableObject> authObjects = mappingProvider.translatePrivilege(object);
    for (AuthorizationSecurableObject authObject : authObjects) {
      List<AuthorizationSecurableObject> convertedObjects = Lists.newArrayList();
      if (authObject.name().equals(JdbcAuthorizationObject.ALL)) {
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
          String sql =
              getGrantPrivilegeSQL(
                  privilege,
                  convertedObject.metadataObjectType().name(),
                  convertedObject.fullName(),
                  role.name());
          executeUpdateSQL(sql, "is already granted");
        }
      }
    }
  }

  private void revokeObjectPrivileges(Role role, SecurableObject removeObject) {
    List<AuthorizationSecurableObject> authObjects =
        mappingProvider.translatePrivilege(removeObject);
    for (AuthorizationSecurableObject authObject : authObjects) {
      List<AuthorizationSecurableObject> convertedObjects = Lists.newArrayList();
      if (authObject.name().equals(JdbcAuthorizationObject.ALL)) {
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
          String sql =
              getRevokePrivilegeSQL(
                  privilege,
                  convertedObject.metadataObjectType().name(),
                  convertedObject.fullName(),
                  role.name());
          executeUpdateSQL(sql, "Cannot find privilege Privilege");
        }
      }
    }
  }
}
