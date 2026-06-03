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

package org.apache.gravitino.iceberg.common.io;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.iceberg.io.StorageCredential;

/** Registry that connects Iceberg FileIO instances with Gravitino credential suppliers. */
public class GravitinoCredentialFileIORegistry {

  private static final ConcurrentMap<String, CredentialSupplier> SUPPLIERS =
      new ConcurrentHashMap<>();

  private GravitinoCredentialFileIORegistry() {}

  /** Supplies storage credentials for a storage path. */
  public interface CredentialSupplier {

    /**
     * Loads credentials for a storage path.
     *
     * @param path the storage path
     * @return storage credentials for the path
     */
    List<StorageCredential> load(String path);
  }

  /**
   * Registers a credential supplier.
   *
   * @param supplier the supplier to register
   * @return the generated supplier identifier
   */
  public static String register(CredentialSupplier supplier) {
    String id = UUID.randomUUID().toString();
    SUPPLIERS.put(id, supplier);
    return id;
  }

  /**
   * Gets a registered credential supplier.
   *
   * @param id the supplier identifier
   * @return the registered supplier
   */
  public static CredentialSupplier get(String id) {
    return SUPPLIERS.get(id);
  }

  /**
   * Unregisters a credential supplier.
   *
   * @param id the supplier identifier
   */
  public static void unregister(String id) {
    SUPPLIERS.remove(id);
  }
}
