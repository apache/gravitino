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
package org.apache.gravitino.connector;

import java.security.Principal;
import java.util.Map;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.utils.Executable;

/** The catalog can implement their own ProxyPlugin to execute operations by given user. */
@Evolving
public interface ProxyPlugin {

  /**
   * @param principal The given principal to execute the action
   * @param action A method need to be executed.
   * @param properties The properties which be used when execute the action.
   * @return The return value of action.
   * @throws Throwable The throwable object which the action throws.
   */
  Object doAs(
      Principal principal, Executable<Object, Exception> action, Map<String, String> properties)
      throws Throwable;

  /**
   * @param ops The catalog operation is bind to plugin.
   */
  void bindCatalogOperation(CatalogOperations ops);
}
