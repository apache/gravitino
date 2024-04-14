/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

public interface SupportsGrantsManagement {

  boolean grantRoleToUser(String metalake, String role, String user);

  boolean grantRoleToGroup(String metalake, String role, String group);

  boolean revokeRoleFromUser(String metalake, String role, String user);

  boolean revokeRoleFromGroup(String metalake, String role, String group);
}
