/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino;

import com.google.common.base.Preconditions;
import java.security.Principal;

public class UserPrincipal implements Principal {

  private final String username;

  public UserPrincipal(final String username) {
    Preconditions.checkNotNull(username, "UserPrincipal must have the username");
    this.username = username;
  }

  @Override
  public String getName() {
    return username;
  }

  @Override
  public int hashCode() {
    return username.hashCode();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof UserPrincipal) {
      UserPrincipal that = (UserPrincipal) o;
      return this.username.equals(that.username);
    }
    return false;
  }

  @Override
  public String toString() {
    return "[principal: " + this.username + "]";
  }
}
