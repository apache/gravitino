package com.datastrato.gravitino.authorization;

import java.util.Set;

/**
 * This interface is used for mapping a given userName to a set of groups which it belongs to.
 * This is useful for specifying a common group of users to provide them privileges,
 */
public interface GroupMappingServiceProvider {

    /**
     * Get the groups the user belongs to.
     * @param userName User's Name
     * @return set of groups that the user belongs to. Empty in case of an invalid user.
     */
    Set<String> getGroups(String userName);
}
