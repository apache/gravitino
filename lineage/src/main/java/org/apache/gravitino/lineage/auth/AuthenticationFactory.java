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
package org.apache.gravitino.lineage.auth;

import javax.ws.rs.BadRequestException;

public class AuthenticationFactory {
  public static LineageServerAuthenticationStrategy createStrategy(String authType) {
    if (authType == null || authType.trim().isEmpty()) {
      return new NoAuthStrategy();
    }

    switch (authType) {
      case "apiKey":
        return new ApiKeyAuthStrategy();
      case "none":
        return new NoAuthStrategy();
      default:
        throw new BadRequestException("Unsupported authentication " + authType);
    }
  }
}
