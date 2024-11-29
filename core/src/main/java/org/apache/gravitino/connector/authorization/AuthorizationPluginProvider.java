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

/**
 * An Authorization plugin provider is a class that provides a catalog name and plugin name for an
 * authorization plugin. <br>
 */
public interface AuthorizationPluginProvider {
  enum Type {
    Ranger("ranger"),
    Chain("chain");
    private final String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }
  }

  /**
   * The string that represents the authorization that this provider uses. <br>
   * This is overridden by children to provide a nice alias for the authorization.
   *
   * @return The string that represents the authorization that this provider uses.
   */
  String catalogProviderName();

  /**
   * The string that represents the authorization plugin that this provider uses. <br>
   * This is overridden by children to provide a nice alias for the authorization.
   *
   * @return The string that represents the authorization plugin that this provider uses.
   */
  String pluginProviderName();
}
