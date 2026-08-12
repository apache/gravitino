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

package org.apache.gravitino.catalog;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.file.FilesetCatalog;

/**
 * {@code FilesetDispatcher} interface acts as a specialization of the {@link FilesetCatalog}
 * interface, and extends {@link FilesetFileOps} for file operations.This interface is designed to
 * potentially add custom behaviors or operations related to dispatching or handling fileset-related
 * events or actions that are not covered by the standard {@code FilesetCatalog} operations.
 */
public interface FilesetDispatcher extends FilesetCatalog, FilesetFileOps {

  /**
   * Load fileset properties with secret URNs resolved to plaintext for connector / runtime use.
   *
   * <p>Credential-vending keys are omitted (use credentials API). Legacy hidden plaintext secrets
   * are omitted. Default {@link #loadFileset(NameIdentifier)} omit behavior is unchanged.
   *
   * @param ident the identifier of the fileset
   * @return resolved plaintext properties
   * @throws NoSuchFilesetException If the fileset does not exist.
   */
  Map<String, String> loadFilesetResolvedProperties(NameIdentifier ident)
      throws NoSuchFilesetException;
}
