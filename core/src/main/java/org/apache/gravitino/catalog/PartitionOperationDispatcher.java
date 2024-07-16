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

import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.storage.IdGenerator;

public class PartitionOperationDispatcher extends OperationDispatcher
    implements PartitionDispatcher {

  /**
   * Creates a new PartitionOperationDispatcher.
   *
   * @param catalogManager The CatalogManager instance to be used for partition operations.
   * @param store The EntityStore instance to be used for partition operations.
   * @param idGenerator The IdGenerator instance to be used for partition operations.
   */
  public PartitionOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  @Override
  public String[] listPartitionNames(NameIdentifier tableIdent) {
    return doWithTable(
        tableIdent, SupportsPartitions::listPartitionNames, NoSuchTableException.class);
  }

  @Override
  public Partition[] listPartitions(NameIdentifier tableIdent) {
    return doWithTable(tableIdent, SupportsPartitions::listPartitions, NoSuchTableException.class);
  }

  @Override
  public Partition getPartition(NameIdentifier tableIdent, String partitionName)
      throws NoSuchPartitionException {
    return doWithTable(
        tableIdent, p -> p.getPartition(partitionName), NoSuchPartitionException.class);
  }

  @Override
  public Partition addPartition(NameIdentifier tableIdent, Partition partition)
      throws PartitionAlreadyExistsException {
    return doWithTable(
        tableIdent, p -> p.addPartition(partition), PartitionAlreadyExistsException.class);
  }

  @Override
  public boolean dropPartition(NameIdentifier tableIdent, String partitionName) {
    return doWithTable(
        tableIdent, p -> p.dropPartition(partitionName), NoSuchPartitionException.class);
  }

  @Override
  public boolean purgePartition(NameIdentifier tableIdent, String partitionName)
      throws UnsupportedOperationException {
    return doWithTable(
        tableIdent, p -> p.purgePartition(partitionName), NoSuchPartitionException.class);
  }
}
