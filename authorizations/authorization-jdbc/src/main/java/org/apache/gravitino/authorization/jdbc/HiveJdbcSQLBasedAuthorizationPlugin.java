package org.apache.gravitino.authorization.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Hive doesn't support the user and group management, so we just return true here.
 */
public class HiveJdbcSQLBasedAuthorizationPlugin extends JdbcSQLBasedAuthorizationPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(HiveJdbcSQLBasedAuthorizationPlugin.class);

    public HiveJdbcSQLBasedAuthorizationPlugin(Map<String, String> config) {
        super(config);
    }

    @Override
    public Boolean onUserAdded(User user) throws AuthorizationPluginException {
        // Hive doesn't support the user and group management, so we just return true here.
        return true;
    }

    @Override
    public Boolean onUserRemoved(User user) throws AuthorizationPluginException {
        // Hive doesn't support the user and group management, so we just return true here.
        return true;
    }

    @Override
    public Boolean onGroupAdded(Group group) throws AuthorizationPluginException {
        // Hive doesn't support the user and group management, so we just return true here.
        return true;
    }

    @Override
    public Boolean onGroupRemoved(Group group) throws AuthorizationPluginException {
        // Hive doesn't support the user and group management, so we just return true here.
        return true;
    }

    @Override
    public List<String> getSetOwnerSQL(MetadataObject.Type type, String... args) {
        if (args[0].contains(JdbcAuthorizationObject.ALL)) {
            return ImmutableList.of();
        }

        if (type == MetadataObject.Type.TABLE) {
            return ImmutableList.of(String.format("ALTER TABLE %s SET OWNER USER %s", args[0], args[2]));
        } else if (type == MetadataObject.Type.SCHEMA) {
            return ImmutableList.of(String.format("ALTER DATABASE %s SET OWNER USER %s", args[0], args[2]));
        } else {
            throw new IllegalArgumentException("Unsupported metadata object type");
        }
    }

    @Override
    protected void beforeExecuteSQL() {
        String sql = "set role admin";
        executeUpdateSQL(sql);
    }

    @Override
    protected List<AuthorizationPrivilege> filterUnsupportedPrivileges(List<AuthorizationPrivilege> privileges) {
        List<AuthorizationPrivilege> filteredPrivileges = Lists.newArrayList();
        for (AuthorizationPrivilege privilege : privileges) {
            String name = privilege.getName();
            if (!name.equals(JdbcPrivilege.Type.CREATE.name()) && !name.equals(JdbcPrivilege.Type.DROP.name()) && !name.equals(JdbcPrivilege.Type.ALTER.name())) {
                filteredPrivileges.add(privilege);
            }
        }
        return filteredPrivileges;
    }

    @Override
    protected List<AuthorizationSecurableObject> convertResourceAll(AuthorizationSecurableObject object) {
        if (object.type().metadataObjectType().equals(MetadataObject.Type.TABLE)) {
            if (object.name().equals(JdbcAuthorizationObject.ALL) && object.parent().equals(JdbcAuthorizationObject.ALL)) {
                List<AuthorizationSecurableObject> objects = Lists.newArrayList();
                for (String database : getAllDatabases()) {
                    objects.add(new JdbcAuthorizationObject(database, null, object.privileges()));
                }
                return objects;
            } else if (object.name().equals(JdbcAuthorizationObject.ALL)) {
                AuthorizationSecurableObject newObject = new JdbcAuthorizationObject(object.parent(), null, object.privileges());
                return Lists.newArrayList(newObject);
            } else {
                throw new IllegalArgumentException("");
            }
        } else if (object.type().metadataObjectType().equals(MetadataObject.Type.SCHEMA)) {
            if (object.name().equals(JdbcAuthorizationObject.ALL)) {
                List<AuthorizationSecurableObject> objects = Lists.newArrayList();
                for (String database : getAllDatabases()) {
                    objects.add(new JdbcAuthorizationObject(database, null, object.privileges()));
                }
                return objects;
            } else {
                throw new IllegalArgumentException("");
            }
        } else {
            throw new IllegalArgumentException("");
        }
    }

    List<String> getAllDatabases() {
        try (final Connection connection = getConnection()) {
            try (final Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
                    List<String> databaseNames = Lists.newArrayList();
                    while (resultSet.next()) {
                        databaseNames.add(resultSet.getString(1));
                    }
                    return databaseNames;
                }
            }
        } catch (SQLException se) {
            LOG.error("Jdbc authorization plugin exception: ", se);
            throw toAuthorizationPluginException(se);
        }

    }
}
