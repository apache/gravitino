package com.datastrato.gravitino.tenant;

import com.datastrato.gravitino.User;
import java.util.Map;

/** The interface for the user management. */
public interface SupportsUserManagement {

  public User createUser(String metalake, String userName, Map<String, String> properties);

  public boolean dropUser(String metalake, String userName);

  public User loadUser(String metalake, String userName);
}
