/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.authorization.jdbc;

import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Unstable;
import org.apache.gravitino.authorization.Owner;

/** Interface for SQL operations of the underlying access control system. */
@Unstable
interface JdbcAuthorizationSQL {

  /**
   * Get SQL statements for creating a user.
   *
   * @param username the username to create
   * @return a SQL statement to create a user
   */
  String getCreateUserSQL(String username);

  /**
   * Get SQL statements for creating a group.
   *
   * @param username the username to drop
   * @return a SQL statement to drop a user
   */
  String getDropUserSQL(String username);

  /**
   * Get SQL statements for creating a role.
   *
   * @param roleName the role name to create
   * @return a SQL statement to create a role
   */
  String getCreateRoleSQL(String roleName);

  /**
   * Get SQL statements for dropping a role.
   *
   * @param roleName the role name to drop
   * @return a SQL statement to drop a role
   */
  String getDropRoleSQL(String roleName);

  /**
   * Get SQL statements for granting privileges.
   *
   * @param privilege the privilege to grant
   * @param objectType the object type in the database system
   * @param objectName the object name in the database system
   * @param roleName the role name to grant
   * @return a sql statement to grant privilege
   */
  String getGrantPrivilegeSQL(
      String privilege, String objectType, String objectName, String roleName);

  /**
   * Get SQL statements for revoking privileges.
   *
   * @param privilege the privilege to revoke
   * @param objectType the object type in the database system
   * @param objectName the object name in the database system
   * @param roleName the role name to revoke
   * @return a sql statement to revoke privilege
   */
  String getRevokePrivilegeSQL(
      String privilege, String objectType, String objectName, String roleName);

  /**
   * Get SQL statements for granting role.
   *
   * @param roleName the role name to grant
   * @param grantorType the grantor type, usually USER or ROLE
   * @param grantorName the grantor name
   * @return a sql statement to grant role
   */
  String getGrantRoleSQL(String roleName, String grantorType, String grantorName);

  /**
   * Get SQL statements for revoking roles.
   *
   * @param roleName the role name to revoke
   * @param revokerType the revoker type, usually USER or ROLE
   * @param revokerName the revoker name
   * @return a sql statement to revoke role
   */
  String getRevokeRoleSQL(String roleName, String revokerType, String revokerName);

  /**
   * Get SQL statements for setting owner.
   *
   * @param type The metadata object type
   * @param objectName the object name in the database system
   * @param preOwner the previous owner of the object
   * @param newOwner the new owner of the object
   * @return the sql statement list to set owner
   */
  List<String> getSetOwnerSQL(
      MetadataObject.Type type, String objectName, Owner preOwner, Owner newOwner);
}
