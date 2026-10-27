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
package org.apache.gravitino.iceberg.service.restriction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.gravitino.policy.sql.SqlDefinition;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.functions.IcebergFunction;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.types.Types;

/** Converts validated typed SQL definitions to closed Iceberg read restrictions. */
public final class IcebergReadRestrictionConverter {
  private IcebergReadRestrictionConverter() {}

  /** Trusted request-scoped identity and group lookups used while binding SQL expressions. */
  public interface BindingContext {
    /**
     * @return the non-empty authenticated effective user name
     */
    String sessionUser();

    /**
     * Resolves exact flat membership in the current metalake.
     *
     * @param group exact group name
     * @return membership, or empty when the group or identity could not be resolved
     */
    Optional<Boolean> groupMembership(String group);
  }

  /**
   * Binds SQL policies to a trusted identity and concrete top-level schema fields. SQL three-valued
   * logic is preserved; only TRUE retains a row. Identity masks and constant-true filters are
   * omitted. Unsupported target expressions fail conversion instead of removing a required
   * restriction.
   *
   * @param rowFilter optional validated row-filter definition
   * @param columnMasks validated mask definitions keyed by selected top-level field ID
   * @param schema target Iceberg schema
   * @param context trusted identity and group provider
   * @return closed read restrictions with stable field IDs
   * @throws IllegalArgumentException if binding or exact target translation is unavailable
   */
  public static ReadRestrictions convert(
      Optional<SqlDefinition> rowFilter,
      Map<Integer, SqlDefinition> columnMasks,
      Schema schema,
      BindingContext context) {
    Objects.requireNonNull(rowFilter, "rowFilter cannot be null");
    Objects.requireNonNull(columnMasks, "columnMasks cannot be null");
    Objects.requireNonNull(schema, "schema cannot be null");
    SqlRestrictionBinding.Context snapshot = new SqlRestrictionBinding.Context(context);
    Expression filter = null;
    if (rowFilter.isPresent()) {
      SqlDefinition definition = rowFilter.get();
      SqlRestrictionBinding binding = new SqlRestrictionBinding(definition, schema, snapshot, null);
      Expression converted = new SqlRestrictionPredicates(binding).resolve(definition.body()).yes;
      converted = SqlRestrictionCanonicalizer.canonicalize(converted);
      if (converted.op() != Expression.Operation.TRUE) {
        filter = converted;
      }
    }
    List<Map.Entry<Integer, SqlDefinition>> ordered = new ArrayList<>(columnMasks.entrySet());
    for (Map.Entry<Integer, SqlDefinition> entry : ordered) {
      if (entry.getKey() == null || entry.getValue() == null) {
        throw new IllegalArgumentException("Mask field ID and definition cannot be null");
      }
    }
    ordered.sort(Map.Entry.comparingByKey());
    List<IcebergFunction<?, ?>> projections = new ArrayList<>();
    for (Map.Entry<Integer, SqlDefinition> entry : ordered) {
      Types.NestedField field = SqlRestrictionBinding.field(schema, entry.getKey());
      SqlRestrictionBinding binding =
          new SqlRestrictionBinding(entry.getValue(), schema, snapshot, field);
      IcebergFunction<?, ?> projection =
          new SqlRestrictionMasks(field, new SqlRestrictionPredicates(binding))
              .resolve(entry.getValue().body());
      if (projection != null) {
        projections.add(projection);
      }
    }
    ReadRestrictions restrictions = ReadRestrictions.of(filter, projections);
    restrictions.validate(Collections.singletonMap(schema.schemaId(), schema));
    return restrictions;
  }
}
