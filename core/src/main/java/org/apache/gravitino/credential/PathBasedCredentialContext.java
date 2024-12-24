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

package org.apache.gravitino.credential;

import com.google.common.base.Preconditions;
import java.util.Set;
import javax.validation.constraints.NotNull;

/**
 * LocationContext is generated when user requesting resources associated with storage location like
 * table, fileset, etc.
 */
public class PathBasedCredentialContext implements CredentialContext {

  @NotNull private final Set<String> writePaths;
  @NotNull private final Set<String> readPaths;
  @NotNull private final String userName;

  public PathBasedCredentialContext(
      String userName, Set<String> writePaths, Set<String> readPaths) {
    Preconditions.checkNotNull(userName, "User name should not be null");
    Preconditions.checkNotNull(writePaths, "Write paths should not be null");
    Preconditions.checkNotNull(readPaths, "Read paths should not be null");
    this.userName = userName;
    this.writePaths = writePaths;
    this.readPaths = readPaths;
  }

  @Override
  public String getUserName() {
    return userName;
  }

  public Set<String> getWritePaths() {
    return writePaths;
  }

  public Set<String> getReadPaths() {
    return readPaths;
  }
}
