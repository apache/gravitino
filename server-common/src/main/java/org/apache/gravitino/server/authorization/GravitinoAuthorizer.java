/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization;

/** Used for metadata authorization. */
public interface GravitinoAuthorizer {

  /**
   * Perform authorization and return the authorization result.
   *
   * @param userId the user id in use_meta table
   * @param resourceType for example, CATALOG, SCHEMA,TABLE, etc.
   * @param resourceId the resource id.
   * @param privilege for example, CREATE_CATALOG, CREATE_TABLE, etc.
   * @return authorization result.
   */
  boolean authorize(long userId, String resourceType, long resourceId, String privilege);
}
