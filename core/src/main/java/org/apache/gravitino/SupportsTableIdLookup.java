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
package org.apache.gravitino;

import java.io.IOException;
import org.apache.gravitino.exceptions.NoSuchEntityException;

/** Optional store capability for resolving table IDs without loading column snapshots. */
public interface SupportsTableIdLookup {

  /**
   * Resolves a table ID using this store's metadata and consistency rules.
   *
   * @param identifier the full table identifier
   * @return the active table ID
   * @throws NoSuchEntityException if the table or its namespace does not exist
   * @throws IOException if the lookup fails
   */
  long getTableId(NameIdentifier identifier) throws IOException;
}
