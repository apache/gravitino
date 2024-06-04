/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

/**
 * Authorization Role interface.
 */
public interface AuthorizationRole {
  // Create a role.
  Role createRole(String name) throws UnsupportedOperationException;

  // Drop a role.
  Boolean dropRole(Role role) throws UnsupportedOperationException;

  // Grant a role to a user.
  Boolean toUser(String userName) throws UnsupportedOperationException;

  // Grant a role to a group.
  Boolean toGroup(String groupName) throws UnsupportedOperationException;

  // Update a role.
  Role updateRole(String name, RoleChange... changes) throws UnsupportedOperationException;
}
