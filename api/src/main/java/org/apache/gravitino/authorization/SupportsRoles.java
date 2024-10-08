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
package org.apache.gravitino.authorization;

import org.apache.gravitino.annotation.Evolving;

/**
 * Interface for supporting list role names for objects. This interface will be mixed with metadata
 * objects to provide listing role operations.
 */
@Evolving
public interface SupportsRoles {

  /**
   * List all the role names associated with this metadata object.
   *
   * @return The role name list associated with this metadata object.
   */
  String[] listBindingRoleNames();
}
