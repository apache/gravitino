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
package org.apache.gravitino.idp;

import java.lang.reflect.Constructor;
import org.apache.gravitino.Config;
import org.apache.gravitino.storage.IdGenerator;

/** Test-only helpers for {@link IdpUserGroupManager}. */
public final class IdpUserGroupManagerTestHelper {

  private IdpUserGroupManagerTestHelper() {}

  /**
   * Creates an isolated manager instance without using the process-wide singleton.
   *
   * @param config The server configuration.
   * @param idGenerator The id generator.
   * @return A new manager instance.
   */
  public static IdpUserGroupManager newManager(Config config, IdGenerator idGenerator)
      throws ReflectiveOperationException {
    Constructor<IdpUserGroupManager> constructor =
        IdpUserGroupManager.class.getDeclaredConstructor(Config.class, IdGenerator.class);
    constructor.setAccessible(true);
    return constructor.newInstance(config, idGenerator);
  }
}
