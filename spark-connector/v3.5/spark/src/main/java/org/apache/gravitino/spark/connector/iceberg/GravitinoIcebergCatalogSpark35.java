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
package org.apache.gravitino.spark.connector.iceberg;

import java.lang.reflect.InvocationTargetException;
import org.apache.iceberg.spark.procedures.SparkProcedures;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.iceberg.catalog.Procedure;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureCatalog;

/**
 * Spark 3.5 specific Gravitino Iceberg catalog implementation. {@link ProcedureCatalog} is declared
 * here rather than in the shared base because on Spark 3.x it is a class Iceberg ships, returning
 * Iceberg's {@link Procedure}, while Spark 4 ships its own with a different return type.
 */
public class GravitinoIcebergCatalogSpark35 extends GravitinoIcebergCatalog
    implements ProcedureCatalog {

  /**
   * Procedures validate that the catalog registered with Spark's catalogManager is the same one
   * passed to the {@code ProcedureBuilder} that invokes loadProcedure(). Pass this catalog rather
   * than the internal Spark catalog to satisfy that check.
   */
  @Override
  public Procedure loadProcedure(Identifier identifier) throws NoSuchProcedureException {
    String[] namespace = identifier.namespace();
    String name = identifier.name();

    try {
      if (isSystemNamespace(namespace)) {
        SparkProcedures.ProcedureBuilder builder = SparkProcedures.newBuilder(name);
        if (builder != null) {
          return builder.withTableCatalog(this).build();
        }
      }
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | ClassNotFoundException e) {
      throw new RuntimeException("Failed to load Iceberg Procedure " + identifier, e);
    }

    throw new NoSuchProcedureException(identifier);
  }
}
