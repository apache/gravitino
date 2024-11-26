package org.apache.gravitino.authorization.jdbc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;

import javax.annotation.Nullable;
import java.util.List;

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
    public void validateAuthorizationMetadataObject() throws IllegalArgumentException {

    }

    @Override
    public List<AuthorizationPrivilege> privileges() {
        return privileges;
    }
}
