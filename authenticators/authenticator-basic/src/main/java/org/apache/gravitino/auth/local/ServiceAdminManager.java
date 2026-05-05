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

package org.apache.gravitino.auth.local;

/** The manager used by the service admin initializer. */
public interface ServiceAdminManager {

  /**
   * Check whether the service admin already exists.
   *
   * @param userName The service admin name.
   * @return True if the service admin exists, otherwise false.
   */
  boolean serviceAdminExists(String userName);

  /**
   * Initialize the service admin with the specified password.
   *
   * @param userName The service admin name.
   * @param password The plain text password.
   */
  void initializeServiceAdmin(String userName, String password);
}
