/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.starrocks.operations;

import static com.google.common.base.Preconditions.checkArgument;

import javax.sql.DataSource;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTablePartitionOperations;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.partitions.Partition;

/** Table partition operations for StarRocks. */
public final class StarRocksTablePartitionOperations extends JdbcTablePartitionOperations {

  @SuppressWarnings("unused")
  private final JdbcExceptionConverter exceptionConverter;

  @SuppressWarnings("unused")
  private final JdbcTypeConverter typeConverter;

  public StarRocksTablePartitionOperations(
      DataSource dataSource,
      JdbcTable loadedTable,
      JdbcExceptionConverter exceptionConverter,
      JdbcTypeConverter typeConverter) {
    super(dataSource, loadedTable);
    checkArgument(exceptionConverter != null, "exceptionConverter is null");
    checkArgument(typeConverter != null, "typeConverter is null");
    this.exceptionConverter = exceptionConverter;
    this.typeConverter = typeConverter;
  }

  @Override
  public String[] listPartitionNames() {
    throw new NotImplementedException("To be implemented in the future");
  }

  @Override
  public Partition[] listPartitions() {
    throw new NotImplementedException("To be implemented in the future");
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    throw new NotImplementedException("To be implemented in the future");
  }

  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    throw new NotImplementedException("To be implemented in the future");
  }

  @Override
  public boolean dropPartition(String partitionName) {
    throw new NotImplementedException("To be implemented in the future");
  }
}
