package org.apache.gravitino.authorization.jdbc;

import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.Privilege;

public class JdbcPrivilege implements AuthorizationPrivilege {

    private final static JdbcPrivilege SELECT_PRI = new JdbcPrivilege(Type.SELECT);
    private final static JdbcPrivilege INSERT_PRI = new JdbcPrivilege(Type.INSERT);
    private final static JdbcPrivilege UPDATE_PRI = new JdbcPrivilege(Type.UPDATE);
    private final static JdbcPrivilege ALTER_PRI = new JdbcPrivilege(Type.ALTER);
    private final static JdbcPrivilege DELETE_PRI = new JdbcPrivilege(Type.DELETE);
    private final static JdbcPrivilege ALL_PRI = new JdbcPrivilege(Type.ALL);
    private final static JdbcPrivilege CREATE_PRI = new JdbcPrivilege(Type.CREATE);
    private final static JdbcPrivilege DROP_PRI = new JdbcPrivilege(Type.DROP);

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

    static JdbcPrivilege valueOf(String type) {
        return valueOf(Type.valueOf(type));
    }

    static JdbcPrivilege valueOf(Type type) {
        switch (type) {
            case CREATE:
                return CREATE_PRI;
            case DELETE:
                return DELETE_PRI;
            case ALL:
                return ALL_PRI;
            case DROP:
                return DROP_PRI;
            case ALTER:
                return ALTER_PRI;
            case INSERT:
                return INSERT_PRI;
            case UPDATE:
                return UPDATE_PRI;
            case SELECT:
                return SELECT_PRI;
            default:
                throw new IllegalArgumentException(String.format("Unsupported parameter type %s", type));
        }
    }

    public enum Type {
        SELECT("Select"),
        INSERT("Insert"),
        UPDATE("Update"),
        ALTER("Alter"),
        DELETE("Delete"),
        ALL("ALL PRIVILEGES"),
        CREATE("Create"),
        DROP("Drop");

        private final String name;

        Type(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
