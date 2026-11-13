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
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.rel.Table;
import org.apache.iceberg.spark.procedures.SparkProcedures;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.ProcedureCatalog;
import org.apache.spark.sql.connector.catalog.procedures.UnboundProcedure;

/**
 * Spark 4.1 specific Gravitino Iceberg catalog implementation. {@link ProcedureCatalog} is declared
 * here rather than in the shared base because Spark 4 moved the procedure types out of Iceberg's
 * package into its own and changed {@code loadProcedure} to return {@link UnboundProcedure}.
 */
public class GravitinoIcebergCatalogSpark41 extends GravitinoIcebergCatalog
    implements ProcedureCatalog {

  private static final Pattern BRANCH_SELECTOR = Pattern.compile("branch_(.*)");

  /**
   * Also loads {@code <database>.<table>.branch_<name>} as that branch of the table. Iceberg's
   * Spark 4.1 extensions resolve the {@code branch} read option and the WAP branch by renaming the
   * relation this way, and Spark 4.1 loads the relation again by that name before running the
   * query. A query that names {@code <database>.<table>.branch_<name>} directly reads the branch
   * the same way; the loaded table refuses Spark writes and deletes.
   *
   * @param ident the table identifier
   * @return the loaded table
   * @throws NoSuchTableException if the table does not exist
   */
  @Override
  public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident)
      throws NoSuchTableException {
    Matcher branch = BRANCH_SELECTOR.matcher(ident.name());
    if (ident.namespace().length != 2 || !branch.matches()) {
      return super.loadTable(ident);
    }
    Identifier tableIdent =
        Identifier.of(new String[] {ident.namespace()[0]}, ident.namespace()[1]);
    // A denied table throws here rather than returning AuthorizationTable.deny: when Spark loads
    // the relation again, it would report the placeholder only as a changed table id.
    Table gravitinoTable = loadGravitinoTable(tableIdent);
    SparkTable sparkTable = (SparkTable) loadSparkTable(tableIdent);
    return createSparkTable(
        tableIdent,
        gravitinoTable,
        SparkTable.create(sparkTable.table(), branch.group(1)),
        sparkCatalog,
        propertiesConverter,
        sparkTransformConverter,
        getSparkTypeConverter());
  }

  /**
   * Procedures validate that the catalog registered with Spark's catalogManager is the same one
   * passed to the {@code ProcedureBuilder} that invokes loadProcedure(). Pass this catalog rather
   * than the internal Spark catalog to satisfy that check.
   */
  @Override
  public UnboundProcedure loadProcedure(Identifier identifier) {
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

    // Spark 4 ships no NoSuchProcedureException: it wraps whatever loadProcedure throws in
    // FAILED_TO_LOAD_ROUTINE unless the exception is already a SparkThrowable. Iceberg's own
    // BaseCatalog throws a plain RuntimeException here for the same reason, so match it.
    throw new RuntimeException("Procedure does not exist: " + identifier);
  }

  /** Spark 4.1 added this method to {@code ProcedureCatalog}; Spark 4.0 has no equivalent. */
  @Override
  public Identifier[] listProcedures(String[] namespace) {
    try {
      if (isSystemNamespace(namespace)) {
        return SparkProcedures.names().stream()
            .map(name -> Identifier.of(namespace, name))
            .toArray(Identifier[]::new);
      }
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | ClassNotFoundException e) {
      throw new RuntimeException(
          "Failed to list Iceberg Procedures under " + Arrays.toString(namespace), e);
    }
    return new Identifier[0];
  }
}
