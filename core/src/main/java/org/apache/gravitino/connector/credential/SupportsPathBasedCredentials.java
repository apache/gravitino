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

package org.apache.gravitino.connector.credential;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** The catalog operation should implement this interface to generate the path based credentials. */
@DeveloperApi
public interface SupportsPathBasedCredentials {

  /**
   * Get {@link PathContext} lists.
   *
   * <p>In most cases there will be only one element in the list. For catalogs which support multi
   * locations like fileset, there may be multiple elements.
   *
   * <p>The name identifier is the identifier of the resource like fileset, table, etc. not include
   * metalake, catalog, schema.
   *
   * @param nameIdentifier, The identifier for fileset, table, etc.
   * @return A list of {@link PathContext}
   */
  List<PathContext> getPathContext(NameIdentifier nameIdentifier);
}
