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

package org.apache.gravitino.catalog.lakehouse;

import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.rel.TableCatalog;

/**
 * Interface for detailed lakehouse catalog operations, combining catalog operations and table
 * catalog. {@link GenericLakehouseCatalog} will try to use this interface to provide detailed
 * lakehouse catalog operations.
 *
 * <pre>
 *    GenericLakehouseCatalog.createTable()
 *       -> LakehouseCatalogOperations.createTable()
 *         -> LanceTableOperations.createTable()
 *         -> IcebergTableOperations.createTable()
 *         -> DeltaTableOperations.createTable()
 *         ...
 * </pre>
 */
public interface LakehouseCatalogOperations extends CatalogOperations, TableCatalog {}
