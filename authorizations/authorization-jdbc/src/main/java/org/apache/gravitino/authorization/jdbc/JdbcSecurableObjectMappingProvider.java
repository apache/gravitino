package org.apache.gravitino.authorization.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationPrivilegesMappingProvider;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcSecurableObjectMappingProvider implements AuthorizationPrivilegesMappingProvider {

    private final Map<Privilege.Name, Set<AuthorizationPrivilege>> privilegeMapping = ImmutableMap.of(
            Privilege.Name.CREATE_TABLE, Sets.newHashSet(JdbcPrivilege.valueOf(JdbcPrivilege.Type.CREATE)),
            Privilege.Name.CREATE_SCHEMA, Sets.newHashSet(JdbcPrivilege.valueOf(JdbcPrivilege.Type.CREATE)),
            Privilege.Name.SELECT_TABLE, Sets.newHashSet(JdbcPrivilege.valueOf(JdbcPrivilege.Type.SELECT)),
            Privilege.Name.MODIFY_TABLE, Sets.newHashSet(JdbcPrivilege.valueOf(JdbcPrivilege.Type.SELECT), JdbcPrivilege.valueOf(JdbcPrivilege.Type.UPDATE), JdbcPrivilege.valueOf(JdbcPrivilege.Type.DELETE), JdbcPrivilege.valueOf(JdbcPrivilege.Type.INSERT), JdbcPrivilege.valueOf(JdbcPrivilege.Type.ALTER)),
            Privilege.Name.USE_SCHEMA, Sets.newHashSet(JdbcPrivilege.valueOf(JdbcPrivilege.Type.SELECT))
    );

    private final Map<Privilege.Name, PrivilegeScope> privilegeScopeMapping = ImmutableMap.of(
            Privilege.Name.CREATE_TABLE, PrivilegeScope.TABLE,
            Privilege.Name.CREATE_SCHEMA, PrivilegeScope.DATABASE,
            Privilege.Name.SELECT_TABLE, PrivilegeScope.TABLE,
            Privilege.Name.MODIFY_TABLE, PrivilegeScope.TABLE,
            Privilege.Name.USE_SCHEMA, PrivilegeScope.DATABASE
    );

    private final Set<AuthorizationPrivilege> ownerPrivileges = ImmutableSet.of();

    private final Set<MetadataObject.Type> allowObjectTypes = ImmutableSet.of(MetadataObject.Type.METALAKE, MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE);

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
        JdbcAuthorizationObject databaseObject;
        JdbcAuthorizationObject tableObject;
        switch (securableObject.type()) {
            case METALAKE:
            case CATALOG:
                convertPluginPrivileges(securableObject, databasePrivileges, tablePrivileges);

                if (!databasePrivileges.isEmpty()) {
                    databaseObject = new JdbcAuthorizationObject(JdbcAuthorizationObject.ALL, null, databasePrivileges);
                    authObjects.add(databaseObject);
                }

                if (!tablePrivileges.isEmpty()) {
                    tableObject = new JdbcAuthorizationObject(JdbcAuthorizationObject.ALL, JdbcAuthorizationObject.ALL, tablePrivileges);
                    authObjects.add(tableObject);
                }
                break;

            case SCHEMA:
                convertPluginPrivileges(securableObject, databasePrivileges, tablePrivileges);
                if (!databasePrivileges.isEmpty()) {
                    databaseObject = new JdbcAuthorizationObject(securableObject.name(), null, databasePrivileges);
                    authObjects.add(databaseObject);
                }

                if (!tablePrivileges.isEmpty()) {
                    tableObject = new JdbcAuthorizationObject(securableObject.name(), JdbcAuthorizationObject.ALL, tablePrivileges);
                    authObjects.add(tableObject);
                }
                break;

            case TABLE:
                convertPluginPrivileges(securableObject, databasePrivileges, tablePrivileges);
                if (!tablePrivileges.isEmpty()) {
                    MetadataObject metadataObject = MetadataObjects.parse(securableObject.parent(), MetadataObject.Type.SCHEMA);
                    tableObject = new JdbcAuthorizationObject(metadataObject.name(), securableObject.name(), tablePrivileges);
                    authObjects.add(tableObject);
                }
                break;

            default:
                throw new IllegalArgumentException(String.format("Don't support metadata object type %s", securableObject.type()));
        }

        return authObjects;
    }


    @Override
    public List<AuthorizationSecurableObject> translateOwner(MetadataObject metadataObject) {
        List<AuthorizationSecurableObject> objects = Lists.newArrayList();
        JdbcPrivilege allPri = JdbcPrivilege.valueOf(JdbcPrivilege.Type.ALL);
        switch (metadataObject.type()) {
            case METALAKE:
            case CATALOG:
                objects.add(new JdbcAuthorizationObject(JdbcAuthorizationObject.ALL, null, Lists.newArrayList(allPri)));
                objects.add(new JdbcAuthorizationObject(JdbcAuthorizationObject.ALL, JdbcAuthorizationObject.ALL, Lists.newArrayList(allPri)));
                break;
            case SCHEMA:
                objects.add(new JdbcAuthorizationObject(metadataObject.name(), null, Lists.newArrayList(allPri)));
                objects.add(new JdbcAuthorizationObject(metadataObject.name(), JdbcAuthorizationObject.ALL, Lists.newArrayList(allPri)));
                break;
            case TABLE:
                MetadataObject schema = MetadataObjects.parse(metadataObject.parent(), MetadataObject.Type.SCHEMA);
                objects.add(new JdbcAuthorizationObject(schema.name(), metadataObject.name(), Lists.newArrayList(allPri)));
                break;
            default:
                throw new IllegalArgumentException("");

        }
        return objects;
    }

    @Override
    public AuthorizationMetadataObject translateMetadataObject(MetadataObject metadataObject) {
        throw new UnsupportedOperationException("Not supported");
    }


    private void convertPluginPrivileges(SecurableObject securableObject, List<AuthorizationPrivilege> databasePrivileges, List<AuthorizationPrivilege> tablePrivileges) {
        for (Privilege privilege : securableObject.privileges()) {
            if (privilegeScopeMapping.get(privilege.name()) == PrivilegeScope.DATABASE) {
                databasePrivileges.addAll(privilegeMapping.get(privilege.name()));
            } else if (privilegeScopeMapping.get(privilege.name()) == PrivilegeScope.TABLE) {
                tablePrivileges.addAll(privilegeMapping.get(privilege.name()));
            }
        }
    }

    enum PrivilegeScope {
        DATABASE,
        TABLE
    }
}
