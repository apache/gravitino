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

package org.apache.gravitino.audit;

/** The interface define unified audit log schema. */
public interface AuditLog {
  /**
   * The user who do the operation.
   *
   * @return user name.
   */
  String user();

  /**
   * The operation name.
   *
   * @return operation name.
   */
  String operation();

  /**
   * The identifier of the resource.
   *
   * @return resource identifier name.
   */
  String identifier();

  /**
   * The timestamp of the operation.
   *
   * @return operation timestamp.
   */
  long timestamp();

  /**
   * The operation is successful or not.
   *
   * @return true if the operation is successful, false otherwise.
   */
  boolean successful();
}
