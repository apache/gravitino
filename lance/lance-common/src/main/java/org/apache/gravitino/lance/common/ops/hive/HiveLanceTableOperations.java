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
package org.apache.gravitino.lance.common.ops.hive;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.lance.common.ops.LanceTableOperations;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.hive2.Hive2Namespace;
import org.lance.namespace.model.AlterTableAlterColumnsRequest;
import org.lance.namespace.model.AlterTableDropColumnsRequest;
import org.lance.namespace.model.CreateTableRequest;
import org.lance.namespace.model.CreateTableResponse;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DeregisterTableResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.DropTableResponse;
import org.lance.namespace.model.RegisterTableRequest;
import org.lance.namespace.model.RegisterTableResponse;
import org.lance.namespace.model.TableExistsRequest;

/**
 * Adapts Gravitino's {@link LanceTableOperations} interface onto the official {@link
 * Hive2Namespace} implementation.
 *
 * <p>{@link Hive2Namespace} implements the metastore-registry operations (describe/declare/drop/
 * deregister/exists). It does not implement {@code createTable}, {@code registerTable}, or column
 * alteration; those delegate to the Lance Namespace default behavior, which reports them as
 * unsupported for this backend. Table creation is performed through {@link #declareTable}.
 */
public class HiveLanceTableOperations implements LanceTableOperations {

  private final Hive2Namespace delegate;

  /**
   * Create table operations delegating to the given {@link Hive2Namespace}.
   *
   * @param delegate the underlying Lance Hive 2 namespace implementation
   */
  public HiveLanceTableOperations(Hive2Namespace delegate) {
    this.delegate = delegate;
  }

  @Override
  public DescribeTableResponse describeTable(
      String tableId,
      String delimiter,
      Optional<Long> version,
      boolean checkDeclared,
      boolean loadDetailedMetadata) {
    DescribeTableRequest request = new DescribeTableRequest();
    request.setId(IdentifierUtil.parse(tableId, delimiter));
    version.ifPresent(request::setVersion);
    request.setCheckDeclared(checkDeclared);
    request.setLoadDetailedMetadata(loadDetailedMetadata);
    return delegate.describeTable(request);
  }

  @Override
  public CreateTableResponse createTable(
      String tableId,
      String mode,
      String delimiter,
      String tableLocation,
      Map<String, String> tableProperties,
      byte[] arrowStreamBody) {
    CreateTableRequest request = new CreateTableRequest();
    request.setId(IdentifierUtil.parse(tableId, delimiter));
    request.setMode(mode);
    request.setProperties(tableProperties);
    return delegate.createTable(request, arrowStreamBody);
  }

  @Override
  public DeclareTableResponse declareTable(
      String tableId, String delimiter, String tableLocation, Map<String, String> tableProperties) {
    DeclareTableRequest request = new DeclareTableRequest();
    request.setId(IdentifierUtil.parse(tableId, delimiter));
    request.setLocation(tableLocation);
    request.setProperties(tableProperties);
    return delegate.declareTable(request);
  }

  @Override
  public RegisterTableResponse registerTable(
      String tableId, String mode, String delimiter, Map<String, String> tableProperties) {
    RegisterTableRequest request = new RegisterTableRequest();
    request.setId(IdentifierUtil.parse(tableId, delimiter));
    request.setMode(mode);
    request.setProperties(tableProperties);
    return delegate.registerTable(request);
  }

  @Override
  public DeregisterTableResponse deregisterTable(String tableId, String delimiter) {
    DeregisterTableRequest request = new DeregisterTableRequest();
    request.setId(IdentifierUtil.parse(tableId, delimiter));
    return delegate.deregisterTable(request);
  }

  @Override
  public boolean tableExists(String tableId, String delimiter) {
    TableExistsRequest request = new TableExistsRequest();
    request.setId(IdentifierUtil.parse(tableId, delimiter));
    try {
      delegate.tableExists(request);
      return true;
    } catch (TableNotFoundException e) {
      return false;
    }
  }

  @Override
  public DropTableResponse dropTable(String tableId, String delimiter) {
    DropTableRequest request = new DropTableRequest();
    request.setId(IdentifierUtil.parse(tableId, delimiter));
    return delegate.dropTable(request);
  }

  @Override
  public Object alterTable(String tableId, String delimiter, Object request) {
    List<String> id = IdentifierUtil.parse(tableId, delimiter);
    if (request instanceof AlterTableDropColumnsRequest) {
      AlterTableDropColumnsRequest dropColumns = (AlterTableDropColumnsRequest) request;
      dropColumns.setId(id);
      return delegate.alterTableDropColumns(dropColumns);
    }
    if (request instanceof AlterTableAlterColumnsRequest) {
      AlterTableAlterColumnsRequest alterColumns = (AlterTableAlterColumnsRequest) request;
      alterColumns.setId(id);
      return delegate.alterTableAlterColumns(alterColumns);
    }
    throw new IllegalArgumentException(
        "Unsupported alter table request type: " + request.getClass().getName());
  }
}
