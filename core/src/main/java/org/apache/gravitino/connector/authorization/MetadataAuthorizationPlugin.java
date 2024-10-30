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
package org.apache.gravitino.connector.authorization;

import org.apache.gravitino.authorization.MetadataObjectChange;

/**
 * Interface for authorization User and Group plugin operation of the underlying access control
 * system.
 */
interface MetadataAuthorizationPlugin {
  /**
   * After updating a metadata object in Gravitino, this method is called to update the role in the
   * underlying system. <br>
   *
   * @param changes metadata object changes apply to the role.
   * @return True if the update operation is successful; False if the update operation fails.
   * @throws RuntimeException If update role encounters storage issues.
   */
  Boolean onMetadataUpdated(MetadataObjectChange... changes) throws RuntimeException;
}
