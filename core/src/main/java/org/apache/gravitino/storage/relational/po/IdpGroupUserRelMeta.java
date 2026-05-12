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
package org.apache.gravitino.storage.relational.po;

/** Abstract built-in IdP group-user relation metadata model used by core storage SPI. */
public interface IdpGroupUserRelMeta {

  /**
   * Returns the relation id.
   *
   * @return the relation id
   */
  Long getId();

  /**
   * Returns the group id.
   *
   * @return the group id
   */
  Long getGroupId();

  /**
   * Returns the user id.
   *
   * @return the user id
   */
  Long getUserId();

  /**
   * Returns the current version.
   *
   * @return the current version
   */
  Long getCurrentVersion();

  /**
   * Returns the last version.
   *
   * @return the last version
   */
  Long getLastVersion();

  /**
   * Returns the deletion timestamp.
   *
   * @return the deletion timestamp
   */
  Long getDeletedAt();
}
