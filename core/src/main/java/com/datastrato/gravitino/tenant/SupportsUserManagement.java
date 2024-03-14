package com.datastrato.gravitino.tenant;

import com.datastrato.gravitino.User;
import java.util.Map;

/** The interface for the user management. */
public interface SupportsUserManagement {

  User createUser(String metalake, String userName, Map<String, String> properties);

  boolean dropUser(String metalake, String userName);

  User loadUser(String metalake, String userName);
}
