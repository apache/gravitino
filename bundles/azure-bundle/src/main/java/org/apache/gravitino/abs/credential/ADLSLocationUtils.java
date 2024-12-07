/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.abs.credential;

import java.net.URI;

public class ADLSLocationUtils {
  public static class ADLSLocationParts {
    private final String container;
    private final String accountName;
    private final String path;

    public ADLSLocationParts(String container, String accountName, String path) {
      this.container = container;
      this.accountName = accountName;
      this.path = path;
    }

    public String getContainer() {
      return container;
    }

    public String getAccountName() {
      return accountName;
    }

    public String getPath() {
      return path;
    }
  }

  public static ADLSLocationParts parseLocation(String location) {
    URI locationUri = URI.create(location);

    String[] authorityParts = locationUri.getAuthority().split("@");

    if (authorityParts.length <= 1) {
      throw new IllegalArgumentException("Invalid location: " + location);
    }

    return new ADLSLocationParts(
        authorityParts[0], authorityParts[1].split("\\.")[0], locationUri.getPath());
  }
}
