package org.apache.gravitino.connector.authorization;

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.*;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.exceptions.AuthorizationPluginException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class ChainedAuthorizationPlugin implements AuthorizationPlugin {
    public abstract List<AuthorizationPlugin> plugins();



    @Override
    public void close() throws IOException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.close();
        }
    }

    @Override
    public Boolean onMetadataUpdated(MetadataObjectChange... changes) throws RuntimeException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onMetadataUpdated(changes);
        }
        return true;
    }

    @Override
    public Boolean onRoleCreated(Role role) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onRoleCreated(role);
        }
        return true;
    }

    @Override
    public Boolean onRoleAcquired(Role role) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onRoleAcquired(role);
        }
        return true;
    }

    @Override
    public Boolean onRoleDeleted(Role role) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onRoleDeleted(role);
        }
        return true;
    }

    @Override
    public Boolean onRoleUpdated(Role role, RoleChange... changes) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onRoleUpdated(role, changes);
        }
        return true;
    }

    @Override
    public Boolean onGrantedRolesToUser(List<Role> roles, User user) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onGrantedRolesToUser(roles, user);
        }
        return true;
    }

    @Override
    public Boolean onRevokedRolesFromUser(List<Role> roles, User user) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin: plugins()) {
            plugin.onRevokedRolesFromUser(roles, user);
        }
        return true;
    }

    @Override
    public Boolean onGrantedRolesToGroup(List<Role> roles, Group group) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onGrantedRolesToGroup(roles, group);
        }
        return true;
    }

    @Override
    public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onRevokedRolesFromGroup(roles, group);
        }
        return true;
    }

    @Override
    public Boolean onUserAdded(User user) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onUserAdded(user);
        }
        return true;
    }

    @Override
    public Boolean onUserRemoved(User user) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onUserRemoved(user);
        }
    }

    @Override
    public Boolean onUserAcquired(User user) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin: plugins()) {
            plugin.onUserAcquired(user);
        }
    }

    @Override
    public Boolean onGroupAdded(Group group) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onGroupAdded(group);
        }
        return true;
    }

    @Override
    public Boolean onGroupRemoved(Group group) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onGroupRemoved(group);
        }
        return true;
    }

    @Override
    public Boolean onGroupAcquired(Group group) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onGroupAcquired(group);
        }
        return true;
    }

    @Override
    public Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner) throws AuthorizationPluginException {
        for (AuthorizationPlugin plugin : plugins()) {
            plugin.onOwnerSet(metadataObject, preOwner, newOwner);
        }
        return true;
    }
}
