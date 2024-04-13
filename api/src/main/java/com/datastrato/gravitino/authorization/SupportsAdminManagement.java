package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;

public interface SupportsAdminManagement {

    /**
     * Adds a new metalake admin.
     *
     * @param user The name of the User.
     * @return The added User instance.
     * @throws UserAlreadyExistsException If a User with the same identifier already exists.
     * @throws RuntimeException If adding the User encounters storage issues.
     */
    User addMetalakeAdmin(String user);


    /**
     * Removes a metalake admin.
     *
     * @param user The name of the User.
     * @return `true` if the User was successfully removed, `false` otherwise.
     * @throws RuntimeException If removing the User encounters storage issues.
     */
    boolean removeMetalakeAdmin(String user);


}
