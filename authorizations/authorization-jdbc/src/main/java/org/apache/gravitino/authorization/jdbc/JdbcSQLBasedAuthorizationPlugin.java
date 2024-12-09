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
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
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
import org.apache.gravitino.meta.UserEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public abstract class JdbcSQLBasedAuthorizationPlugin implements AuthorizationPlugin, JdbcAuthorizationSQL {


    private static final String GRAVITINO_METALAKE_OWNER_ROLE = "GRAVITINO_METALAKE_OWNER_ROLE";
    private static final String GROUP_PREFIX = "GRAVITINO_GROUP_";

    protected DataSource dataSource;
    protected JdbcSecurableObjectMappingProvider mappingProvider;

    @Override
    public void close() throws IOException {
        // do nothing
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
        String sql = getCreateRoleSQL(role.name());
        executeUpdateSQL(sql);

        for (SecurableObject object : role.securableObjects()) {
            List<AuthorizationSecurableObject> authObjects = mappingProvider.translatePrivilege(object);
            for (AuthorizationSecurableObject authObject : authObjects) {
                List<String> privileges = authObject.privileges().stream().map(AuthorizationPrivilege::getName).collect(Collectors.toList());
                sql = getGrantPrivilegeSQL(StringUtils.join(privileges ,","), authObject.fullName(), role.name());
                executeUpdateSQL(sql);
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
        String sql = getDropRoleSQL(role.name());
        executeUpdateSQL(sql);
        return null;
    }

    @Override
    public Boolean onRoleUpdated(Role role, RoleChange... changes) throws AuthorizationPluginException {
        onRoleCreated(role);
        for (RoleChange change : changes) {
            if (change instanceof RoleChange.AddSecurableObject) {
                SecurableObject object = ((RoleChange.AddSecurableObject) change).getSecurableObject();
                grantObjectPrivileges(role, object);
            } else if (change instanceof RoleChange.RemoveSecurableObject) {
                SecurableObject object = ((RoleChange.RemoveSecurableObject) change).getSecurableObject();
                revokeObjectPrivileges(role, object);
            } else if (change instanceof  RoleChange.UpdateSecurableObject) {
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
    public Boolean onGrantedRolesToUser(List<Role> roles, User user) throws AuthorizationPluginException {
        onUserAdded(user);
        String sql = getGrantRoleSQL(StringUtils.join(roles, ","), user.name());
        executeUpdateSQL(sql);
        return true;
    }

    @Override
    public Boolean onRevokedRolesFromUser(List<Role> roles, User user) throws AuthorizationPluginException {
        onUserAdded(user);
        String sql = getRevokeRoleSQL(StringUtils.join(roles, ","), user.name());
        executeUpdateSQL(sql);
        return true;
    }

    @Override
    public Boolean onGrantedRolesToGroup(List<Role> roles, Group group) throws AuthorizationPluginException {
        onGroupAdded(group);
        String sql = getGrantRoleSQL(StringUtils.join(roles, ","), String.format("%s%s", GROUP_PREFIX, group.name()));
        executeUpdateSQL(sql);
        return true;
    }

    @Override
    public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group) throws AuthorizationPluginException {
        onGroupAdded(group);
        String sql = getRevokeRoleSQL(StringUtils.join(roles, ","), String.format("%s%s", GROUP_PREFIX, group.name()));
        executeUpdateSQL(sql);
        return true;
    }

    @Override
    public Boolean onUserAdded(User user) throws AuthorizationPluginException {
        String sql = getCreateRoleSQL(user.name());
        executeUpdateSQL(sql);
        return true;
    }

    @Override
    public Boolean onUserRemoved(User user) throws AuthorizationPluginException {
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
        String name = String.format("%s%s", GROUP_PREFIX, group.name());
        String sql = getCreateUserSQL(name);
        executeUpdateSQL(sql);
        return true;
    }


    @Override
    public Boolean onGroupRemoved(Group group) throws AuthorizationPluginException {
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
    public Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner) throws AuthorizationPluginException {
        List<AuthorizationSecurableObject> authObjects = mappingProvider.translateOwner(metadataObject);
        if (preOwner != null) {
            String name;
            if (preOwner.type() == Owner.Type.USER) {
                name = preOwner.name();
                onUserAdded(new TemporaryUser(preOwner.name()));
            } else {
                name = String.format("%s%s", GROUP_PREFIX, preOwner.name());
                onGroupAdded(new TemporaryGroup(preOwner.name()));
            }

            if (metadataObject.type() == MetadataObject.Type.METALAKE) {
                String sql = getRevokeRoleSQL(GRAVITINO_METALAKE_OWNER_ROLE, name);
                executeUpdateSQL(sql);
            } else {
                for (AuthorizationSecurableObject authObject : authObjects) {
                    String sql = getRevokeAllPrivilegesSQL(authObject.fullName(), name);
                    executeUpdateSQL(sql);
                }
            }

        }
        if (newOwner != null) {
            String name;
            if (newOwner.type() == Owner.Type.USER) {
                name = newOwner.name();
                onUserAdded(new TemporaryUser(newOwner.name()));
            } else {
                name = String.format("%s%s", GROUP_PREFIX, newOwner.name());
                onUserAdded(new TemporaryUser(newOwner.name()));
            }

            if (metadataObject.type() == MetadataObject.Type.METALAKE) {
                String sql = getGrantRoleSQL(GRAVITINO_METALAKE_OWNER_ROLE, name);
                executeUpdateSQL(sql);
            } else {
                for (AuthorizationSecurableObject authObject : authObjects) {
                    String sql = getGrantAllPrivilegesSQL(authObject.fullName(), name);
                    executeUpdateSQL(sql);
                }
            }
        }
        return true;
    }

    @Override
    public String getCreateUserSQL(String... args) {
        Preconditions.checkArgument(args.length == 1, "args length must be 1");
        return String.format("CREATE USER %s", args[0]);
    }

    @Override
    public String getDropUserSQL(String... args) {
        Preconditions.checkArgument(args.length == 1, "args length must be 1");
        return String.format("DROP USER %s", args[0]);
    }


    @Override
    public String getCreateRoleSQL(String... args) {
        Preconditions.checkArgument(args.length == 1, "args length must be 1");
        return String.format("CREATE ROLE %s", args[0]);
    }

    @Override
    public String getDropRoleSQL(String... args) {
        Preconditions.checkArgument(args.length == 1, "args length must be 1");
        return String.format("DROP ROLE %s", args[0]);
    }

    @Override
    public String getGrantPrivilegeSQL(String... args) {
        return String.format("GRANT %s ON %s TO ROLE %s", args[0], args[1], args[2]);
    }

    @Override
    public String getRevokePrivilegeSQL(String... args) {
        return String.format("REVOKE %s ON %s FROM ROLE %s", args[0], args[1], args[2]);
    }

    @Override
    public String getGrantRoleSQL(String... args) {
        return String.format("GRANT ROLE %s TO %s", args[0], args[1]);
    }

    @Override
    public String getRevokeRoleSQL(String... args) {
        return String.format("REVOKE ROLE %s FROM %s", args[0], args[1]);
    }

    @Override
    public String getGrantAllPrivilegesSQL(String... args) {
        return String.format("GRANT ALL PRIVILEGES ON %s TO %s", args[0], args[1]);
    }

    @Override
    public String getRevokeAllPrivilegesSQL(String... args) {
        return String.format("REVOKE ALL PRIVILEGES ON %s FROM %s", args[0], args[1]);
    }


    protected Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }


    private AuthorizationPluginException toAuthorizationPluginException(SQLException se) {
        String errMsg = String.format("Jdbc authorization plugin fail to execute SQL, error code: %d", se.getErrorCode());
        return new AuthorizationPluginException(errMsg);
    }

    private void executeUpdateSQL(String sql) {
        try (final Connection connection = getConnection()) {
            try (final Statement statement = connection.createStatement()) {
                statement.executeUpdate(sql);
            }
        } catch (SQLException se) {
            if (se.getErrorCode() == 1061) {
                return;
            } else {
                throw toAuthorizationPluginException(se);
            }
        }
    }


    private void grantObjectPrivileges(Role role, SecurableObject object) {
        List<AuthorizationSecurableObject> authObjects = mappingProvider.translatePrivilege(object);
        for (AuthorizationSecurableObject authObject : authObjects) {
            List<String> privileges = authObject.privileges().stream().map(AuthorizationPrivilege::getName).collect(Collectors.toList());
            String sql = getGrantPrivilegeSQL(StringUtils.join(privileges, ","), authObject.fullName(), role.name());
            executeUpdateSQL(sql);
        }
    }

    private void revokeObjectPrivileges(Role role, SecurableObject removeObject) {
        List<AuthorizationSecurableObject> authObjects = mappingProvider.translatePrivilege(removeObject);
        for (AuthorizationSecurableObject authObject : authObjects) {
            List<String> privileges = authObject.privileges().stream().map(AuthorizationPrivilege::getName).collect(Collectors.toList());
            String sql = getRevokePrivilegeSQL(StringUtils.join(privileges, ","), authObject.fullName(), role.name());
            executeUpdateSQL(sql);
        }
    }

    private static class TemporaryUser implements User {
        private String name;

        TemporaryUser(String name) {
            this.name = name;
        }
        @Override
        public String name() {
            return name;
        }

        @Override
        public List<String> roles() {
            return ImmutableList.of();
        }

        @Override
        public Audit auditInfo() {
            return null;
        }
    }

    private static class TemporaryGroup implements Group {
        private String name;

        TemporaryGroup(String name) {
            this.name = name;
        }
        @Override
        public String name() {
            return name;
        }

        @Override
        public List<String> roles() {
            return ImmutableList.of();
        }

        @Override
        public Audit auditInfo() {
            return null;
        }
    }
}
