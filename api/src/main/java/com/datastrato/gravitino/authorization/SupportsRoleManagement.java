package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.RoleAlreadyExistsException;

import java.util.List;
import java.util.Map;

public interface SupportsRoleManagement {

  /**
   * Loads a Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @return The loading Role instance.
   * @throws NoSuchRoleException If the Role with the given identifier does not exist.
   * @throws RuntimeException If loading the Role encounters storage issues.
   */
  Role loadRole(String metalake, String role) throws NoSuchRoleException;

  /**
   * Drops a Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @return `true` if the Role was successfully dropped, `false` otherwise.
   * @throws RuntimeException If dropping the User encounters storage issues.
   */
  boolean dropRole(String metalake, String role);

  /**
   * Creates a new Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @param properties The properties of the Role.
   * @param privilegeEntityIdentifier The privilege entity identifier of the Role.
   * @param privilegeEntityType The privilege entity type of the Role.
   * @param privileges The privileges of the Role.
   * @return The created Role instance.
   * @throws RoleAlreadyExistsException If a Role with the same identifier already exists.
   * @throws RuntimeException If creating the Role encounters storage issues.
   */
  public Role createRole(
          String metalake,
          String role,
          Map<String, String> properties,
          Resource resource,
          List<Privilege> privileges)
          throws RoleAlreadyExistsException;
}
