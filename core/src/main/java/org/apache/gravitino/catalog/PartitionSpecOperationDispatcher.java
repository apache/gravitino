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
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.SupportsPartitionSpecs;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.storage.IdGenerator;

/** Dispatches partition-spec reads into the catalog class loader. */
public class PartitionSpecOperationDispatcher extends OperationDispatcher
    implements PartitionSpecDispatcher {

  /**
   * Creates a new PartitionSpecOperationDispatcher.
   *
   * @param catalogManager The CatalogManager instance to be used for partition-spec operations.
   * @param store The EntityStore instance to be used for partition-spec operations.
   * @param idGenerator The IdGenerator instance to be used for partition-spec operations.
   * @param secretManager The SecretManager instance to be used for secret operations.
   */
  public PartitionSpecOperationDispatcher(
      CatalogManager catalogManager,
      EntityStore store,
      IdGenerator idGenerator,
      SecretManager secretManager) {
    super(catalogManager, store, idGenerator, secretManager);
  }

  @Override
  public int defaultPartitionSpecId(NameIdentifier tableIdent) {
    return doWithPartitionSpecs(
        tableIdent, SupportsPartitionSpecs::defaultPartitionSpecId, NoSuchTableException.class);
  }

  @Override
  public Map<Integer, Transform[]> partitionSpecs(NameIdentifier tableIdent) {
    return doWithPartitionSpecs(
        tableIdent, SupportsPartitionSpecs::partitionSpecs, NoSuchTableException.class);
  }
}
