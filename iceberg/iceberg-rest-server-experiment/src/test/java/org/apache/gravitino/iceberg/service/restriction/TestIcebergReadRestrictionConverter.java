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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.iceberg.service.restriction.IcebergReadRestrictionConverter.BindingContext;
import org.apache.gravitino.policy.rego.RestrictedRegoExpressionParserFacade;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.ColumnMask;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.RowFilter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundLiteralPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.functions.IcebergFunction;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.rest.restrictions.ReadRestrictionsParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Tests design-contract binding from restricted Rego to Iceberg read restrictions. */
public class TestIcebergReadRestrictionConverter {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "region", Types.StringType.get()),
          Types.NestedField.optional(3, "owner", Types.StringType.get()),
          Types.NestedField.optional(4, "amount", Types.DecimalType.of(7, 2)),
          Types.NestedField.optional(5, "event_date", Types.DateType.get()),
          Types.NestedField.optional(6, "event_time", Types.TimeType.get()),
          Types.NestedField.optional(7, "event_ts", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(8, "score", Types.FloatType.get()),
          Types.NestedField.required(9, "required_text", Types.StringType.get()),
          Types.NestedField.optional(
              10,
              "nested",
              Types.StructType.of(Types.NestedField.optional(11, "value", Types.StringType.get()))),
          Types.NestedField.optional(12, "double_score", Types.DoubleType.get()));

  @Test
  void testBindsContextAndColumnToClosedFieldIdPredicate() {
    RowFilter filter =
        RestrictedRegoExpressionParserFacade.parseRowFilter(
            "filter := col(\"region\") == \"US\" if is_group_member(\"auditors\") "
                + "else := col(\"owner\") == session_user()");

    ReadRestrictions restrictions =
        IcebergReadRestrictionConverter.convert(
            Optional.of(filter),
            Collections.emptyMap(),
            SCHEMA,
            context("alice", Collections.singletonMap("auditors", false)));

    assertNotNull(restrictions.rowFilter());
    assertTrue(Binder.isBound(restrictions.rowFilter()));
    assertEquals(Expression.Operation.EQ, restrictions.rowFilter().op());
    BoundLiteralPredicate<?> predicate = (BoundLiteralPredicate<?>) restrictions.rowFilter();
    assertEquals(3, predicate.ref().fieldId());
    assertEquals("alice", predicate.literal().value().toString());

    String json = ReadRestrictionsParser.toJson(restrictions);
    assertTrue(json.contains("\"id\":3"));
    assertTrue(json.contains("alice"), json);
    assertFalse(json.contains("session_user"));
    assertFalse(json.contains("owner"));
  }

  @Test
  void testOmitsTrueAndPreservesFalse() {
    ReadRestrictions allowAll = convertFilter("filter := true", context("alice"));
    assertNull(allowAll.rowFilter());
    assertTrue(allowAll.isEmpty());

    ReadRestrictions denyAll = convertFilter("filter := false", context("alice"));
    assertNotNull(denyAll.rowFilter());
    assertEquals(Expression.Operation.FALSE, denyAll.rowFilter().op());
  }

  @Test
  void testCanonicalizesLogicalChildrenAndInValues() {
    ReadRestrictions first =
        convertFilter(
            "filter := col(\"id\") in [2, 1, 2] and col(\"region\") == \"US\" "
                + "and col(\"region\") == \"US\"",
            context("alice"));
    ReadRestrictions second =
        convertFilter(
            "filter := col(\"region\") == \"US\" and col(\"id\") in [1, 2]", context("alice"));

    assertEquals(
        ExpressionParser.toJson(first.rowFilter()), ExpressionParser.toJson(second.rowFilter()));
  }

  @Test
  void testBindsEverySupportedLiteralFamily() {
    String source =
        "filter := col(\"id\") == 7"
            + " and col(\"amount\") == 12.30"
            + " and col(\"event_date\") == \"2026-10-12\""
            + " and col(\"event_time\") == \"12:34:56.123456\""
            + " and col(\"event_ts\") == \"2026-10-12T12:34:56.123456\""
            + " and col(\"score\") == 0.5"
            + " and col(\"double_score\") == 0.5";

    ReadRestrictions restrictions = convertFilter(source, context("alice"));

    assertNotNull(restrictions.rowFilter());
    assertTrue(Binder.isBound(restrictions.rowFilter()));
    String json = ReadRestrictionsParser.toJson(restrictions);
    assertNotNull(ReadRestrictionsParser.fromJson(json, SCHEMA).rowFilter());
  }

  @Test
  void testRejectsInexactAndUnsupportedLiterals() {
    assertThrows(
        IllegalArgumentException.class,
        () -> convertFilter("filter := col(\"id\") == 1.5", context("alice")));
    assertThrows(
        IllegalArgumentException.class,
        () -> convertFilter("filter := col(\"amount\") == 1.234", context("alice")));
    assertThrows(
        IllegalArgumentException.class,
        () -> convertFilter("filter := col(\"score\") == 16777217", context("alice")));
    assertThrows(
        IllegalArgumentException.class,
        () -> convertFilter("filter := col(\"score\") <= 0.1", context("alice")));
    assertThrows(
        IllegalArgumentException.class,
        () -> convertFilter("filter := col(\"double_score\") <= 0.1", context("alice")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            convertFilter(
                "filter := col(\"event_time\") == \"12:34:56.1234567\"", context("alice")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            convertFilter(
                "filter := col(\"event_ts\") == \"2026-10-12T12:34:56Z\"", context("alice")));
  }

  @Test
  void testPreservesFloatingPointNegativeZero() {
    BoundLiteralPredicate<?> floatPredicate =
        (BoundLiteralPredicate<?>)
            convertFilter("filter := col(\"score\") == -0", context("alice")).rowFilter();
    BoundLiteralPredicate<?> doublePredicate =
        (BoundLiteralPredicate<?>)
            convertFilter("filter := col(\"double_score\") == -0", context("alice")).rowFilter();

    assertEquals(
        Float.floatToRawIntBits(-0.0F),
        Float.floatToRawIntBits((Float) floatPredicate.literal().value()));
    assertEquals(
        Double.doubleToRawLongBits(-0.0D),
        Double.doubleToRawLongBits((Double) doublePredicate.literal().value()));
  }

  @Test
  void testRejectsUnknownGroupInsteadOfTreatingItAsNonMembership() {
    RowFilter filter =
        RestrictedRegoExpressionParserFacade.parseRowFilter(
            "filter := true if is_group_member(\"missing\") else := false");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergReadRestrictionConverter.convert(
                Optional.of(filter),
                Collections.emptyMap(),
                SCHEMA,
                context("alice", Collections.emptyMap())));
  }

  @Test
  void testBindsUnselectedRowFilterBranchesBeforeFolding() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            convertFilter(
                "filter := true if true else := col(\"missing\") == 1", context("alice")));

    assertThrows(
        IllegalArgumentException.class,
        () -> convertFilter("filter := false and is_group_member(\"missing\")", context("alice")));
  }

  @Test
  void testUsesOneIdentitySnapshotPerConversion() {
    AtomicInteger userLookups = new AtomicInteger();
    AtomicInteger groupLookups = new AtomicInteger();
    BindingContext countingContext =
        new BindingContext() {
          @Override
          public String sessionUser() {
            userLookups.incrementAndGet();
            return "alice";
          }

          @Override
          public Optional<Boolean> groupMembership(String group) {
            groupLookups.incrementAndGet();
            return Optional.of(true);
          }
        };

    convertFilter(
        "filter := col(\"owner\") == session_user() if is_group_member(\"auditors\") "
            + "else := is_group_member(\"auditors\")",
        countingContext);

    assertEquals(1, userLookups.get());
    assertEquals(1, groupLookups.get());
  }

  @Test
  void testSelectsFirstMatchingMaskBranch() {
    ColumnMask mask =
        RestrictedRegoExpressionParserFacade.parseColumnMask(
            "mask := action(\"show-last-4\") if is_group_member(\"auditors\") "
                + "else := action(\"replace-with-null\")");

    ReadRestrictions auditor =
        convertMasks(
            Collections.singletonMap(2, mask),
            context("alice", Collections.singletonMap("auditors", true)));
    ReadRestrictions regular =
        convertMasks(
            Collections.singletonMap(2, mask),
            context("alice", Collections.singletonMap("auditors", false)));

    assertEquals("show-last-4", auditor.columnProjections().get(0).name());
    assertEquals("replace-with-null", regular.columnProjections().get(0).name());
    assertEquals(2, auditor.columnProjections().get(0).fieldId());
  }

  @Test
  void testConvertsEveryMaskAction() {
    Map<String, Integer> actions = new LinkedHashMap<>();
    actions.put("mask-alphanum", 2);
    actions.put("mask-to-fixed-value", 1);
    actions.put("replace-with-null", 2);
    actions.put("show-first-4", 2);
    actions.put("show-last-4", 2);
    actions.put("truncate-to-year", 5);
    actions.put("truncate-to-month", 5);
    actions.put("sha-256-global", 1);
    actions.put("sha-256-query-local", 2);

    for (Map.Entry<String, Integer> entry : actions.entrySet()) {
      ColumnMask mask =
          RestrictedRegoExpressionParserFacade.parseColumnMask(
              "mask := action(\"" + entry.getKey() + "\")");
      ReadRestrictions restrictions =
          convertMasks(Collections.singletonMap(entry.getValue(), mask), context("alice"));
      IcebergFunction<?, ?> function = restrictions.columnProjections().get(0);

      assertEquals(entry.getKey(), function.name());
      assertEquals(entry.getValue().intValue(), function.fieldId());
      String json = ReadRestrictionsParser.toJson(restrictions);
      assertEquals(
          json, ReadRestrictionsParser.toJson(ReadRestrictionsParser.fromJson(json, SCHEMA)));
    }
  }

  @Test
  void testMaskValidationFailsClosed() {
    ColumnMask nullMask =
        RestrictedRegoExpressionParserFacade.parseColumnMask(
            "mask := action(\"replace-with-null\")");
    ColumnMask stringMask =
        RestrictedRegoExpressionParserFacade.parseColumnMask("mask := action(\"show-last-4\")");

    assertThrows(
        IllegalArgumentException.class,
        () -> convertMasks(Collections.singletonMap(9, nullMask), context("alice")));
    assertThrows(
        IllegalArgumentException.class,
        () -> convertMasks(Collections.singletonMap(1, stringMask), context("alice")));
    assertThrows(
        IllegalArgumentException.class,
        () -> convertMasks(Collections.singletonMap(11, stringMask), context("alice")));
  }

  @Test
  void testMaskSkipsUnselectedActionsAndConditions() {
    ColumnMask incompatibleFallback =
        RestrictedRegoExpressionParserFacade.parseColumnMask(
            "mask := action(\"mask-to-fixed-value\") if true " + "else := action(\"show-last-4\")");
    ReadRestrictions selected =
        convertMasks(Collections.singletonMap(1, incompatibleFallback), context("alice"));
    assertEquals("mask-to-fixed-value", selected.columnProjections().get(0).name());

    ColumnMask unresolvedUnselectedCondition =
        RestrictedRegoExpressionParserFacade.parseColumnMask(
            "mask := action(\"show-last-4\") if true "
                + "else := action(\"show-first-4\") if is_group_member(\"missing\") "
                + "else := action(\"replace-with-null\")");
    ReadRestrictions firstBranch =
        convertMasks(Collections.singletonMap(2, unresolvedUnselectedCondition), context("alice"));
    assertEquals("show-last-4", firstBranch.columnProjections().get(0).name());

    ColumnMask incompatibleUnselectedBranch =
        RestrictedRegoExpressionParserFacade.parseColumnMask(
            "mask := action(\"show-last-4\") if false "
                + "else := action(\"mask-to-fixed-value\") if true "
                + "else := action(\"show-first-4\")");
    ReadRestrictions laterBranch =
        convertMasks(Collections.singletonMap(1, incompatibleUnselectedBranch), context("alice"));
    assertEquals("mask-to-fixed-value", laterBranch.columnProjections().get(0).name());
  }

  @Test
  void testSelectedIncompatibleMaskStillFailsClosed() {
    ColumnMask incompatibleSelectedBranch =
        RestrictedRegoExpressionParserFacade.parseColumnMask(
            "mask := action(\"show-last-4\") if true " + "else := action(\"mask-to-fixed-value\")");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            convertMasks(
                Collections.singletonMap(1, incompatibleSelectedBranch), context("alice")));

    ColumnMask incompatibleSelectedFallback =
        RestrictedRegoExpressionParserFacade.parseColumnMask(
            "mask := action(\"mask-to-fixed-value\") if false "
                + "else := action(\"show-last-4\")");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            convertMasks(
                Collections.singletonMap(1, incompatibleSelectedFallback), context("alice")));
  }

  @Test
  void testNullTestsRequireOptionalTopLevelField() {
    ReadRestrictions optional =
        convertFilter("filter := col(\"region\") == null", context("alice"));
    assertEquals(Expression.Operation.IS_NULL, optional.rowFilter().op());

    assertThrows(
        IllegalArgumentException.class,
        () -> convertFilter("filter := col(\"required_text\") == null", context("alice")));
  }

  @Test
  void testRejectsTimestampWithTimezone() {
    Schema schema =
        new Schema(Types.NestedField.optional(20, "zoned", Types.TimestampType.withZone()));
    RowFilter filter =
        RestrictedRegoExpressionParserFacade.parseRowFilter(
            "filter := col(\"zoned\") == \"2026-10-12T12:34:56Z\"");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergReadRestrictionConverter.convert(
                Optional.of(filter), Collections.emptyMap(), schema, context("alice")));
  }

  private static ReadRestrictions convertFilter(String source, BindingContext context) {
    RowFilter filter = RestrictedRegoExpressionParserFacade.parseRowFilter(source);
    return IcebergReadRestrictionConverter.convert(
        Optional.of(filter), Collections.emptyMap(), SCHEMA, context);
  }

  private static ReadRestrictions convertMasks(
      Map<Integer, ColumnMask> masks, BindingContext context) {
    return IcebergReadRestrictionConverter.convert(Optional.empty(), masks, SCHEMA, context);
  }

  private static BindingContext context(String sessionUser) {
    return context(sessionUser, Collections.emptyMap());
  }

  private static BindingContext context(String sessionUser, Map<String, Boolean> groupMemberships) {
    Map<String, Boolean> memberships = new HashMap<>(groupMemberships);
    return new BindingContext() {
      @Override
      public String sessionUser() {
        return sessionUser;
      }

      @Override
      public Optional<Boolean> groupMembership(String group) {
        return Optional.ofNullable(memberships.get(group));
      }
    };
  }
}
