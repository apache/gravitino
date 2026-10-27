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

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.gravitino.iceberg.service.restriction.IcebergReadRestrictionConverter.BindingContext;
import org.apache.gravitino.policy.sql.Literal;
import org.apache.gravitino.policy.sql.LiteralType;
import org.apache.gravitino.policy.sql.RestrictedSqlExpressionParserFacade;
import org.apache.gravitino.policy.sql.SqlDefinition;
import org.apache.gravitino.policy.sql.SqlType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundLiteralPredicate;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.rest.restrictions.ReadRestrictionsParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Executes converted SQL predicates against actual Iceberg bound predicates and field IDs. */
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
          Types.NestedField.optional(12, "double_score", Types.DoubleType.get()),
          Types.NestedField.optional(13, "flag", Types.BooleanType.get()),
          Types.NestedField.optional(14, "zoned", Types.TimestampType.withZone()),
          Types.NestedField.optional(15, "bytes", Types.BinaryType.get()),
          Types.NestedField.optional(16, "counter", Types.LongType.get()),
          Types.NestedField.optional(17, "other_flag", Types.BooleanType.get()));

  @Test
  void testBindsDeclaredParametersAndSubjectToFieldIds() {
    ReadRestrictions result = filter("owner STRING", "owner = CURRENT_USER()");
    BoundLiteralPredicate<?> predicate = (BoundLiteralPredicate<?>) result.rowFilter();
    Assertions.assertEquals(3, predicate.ref().fieldId());
    Assertions.assertEquals("alice", predicate.literal().value().toString());
    Assertions.assertTrue(Binder.isBound(result.rowFilter()));
    String json = ReadRestrictionsParser.toJson(result);
    Assertions.assertFalse(json.contains("CURRENT_USER"));
    Assertions.assertFalse(json.contains("owner"));
    Assertions.assertEquals(json, reboundJson(json));
  }

  @Test
  void testOmitsTrueRetainsFalseAndUnknown() {
    Assertions.assertTrue(filter("", "TRUE").isEmpty());
    Assertions.assertEquals(Expression.Operation.FALSE, filter("", "FALSE").rowFilter().op());
    Assertions.assertEquals(Expression.Operation.FALSE, filter("", "NULL").rowFilter().op());
    Assertions.assertTrue(filter("", "NULL OR TRUE").isEmpty());
    Assertions.assertEquals(Expression.Operation.FALSE, filter("", "NOT NULL").rowFilter().op());
    Assertions.assertTrue(filter("", "NOT (FALSE AND NULL)").isEmpty());
  }

  @Test
  void testSqlComparisonsAndNegationExcludeNulls() {
    for (String body :
        Arrays.asList(
            "region <> 'US'",
            "NOT region = 'US'",
            "NOT NOT region <> 'US'",
            "region < 'ZZ'",
            "region <= 'ZZ'",
            "region > 'AA'",
            "region >= 'AA'")) {
      ReadRestrictions result = filter("region STRING", body);
      Assertions.assertFalse(eval(result, row(2, null)), body);
      Assertions.assertTrue(eval(result, row(2, "CA")), body);
    }
    Assertions.assertTrue(
        eval(filter("region STRING", "region IS NULL OR region <> 'US'"), row(2, null)));
    Assertions.assertFalse(eval(filter("region STRING", "region = NULL"), row(2, "US")));
    Assertions.assertFalse(eval(filter("region STRING", "region <> NULL"), row(2, "US")));
  }

  @Test
  void testBooleanThreeValuedTruthTables() {
    for (Boolean a : new Boolean[] {true, false, null}) {
      for (Boolean b : new Boolean[] {true, false, null}) {
        Map<Integer, Object> row = row(13, a, 17, b);
        String params = "flag BOOLEAN, other_flag BOOLEAN";
        Assertions.assertEquals(
            Boolean.TRUE.equals(a) && Boolean.TRUE.equals(b),
            eval(filter(params, "flag AND other_flag"), row));
        Assertions.assertEquals(
            Boolean.TRUE.equals(a) || Boolean.TRUE.equals(b),
            eval(filter(params, "flag OR other_flag"), row));
        Assertions.assertEquals(
            Boolean.FALSE.equals(a) || Boolean.FALSE.equals(b),
            eval(filter(params, "NOT (flag AND other_flag)"), row));
        Assertions.assertEquals(
            Boolean.FALSE.equals(a) && Boolean.FALSE.equals(b),
            eval(filter(params, "NOT (flag OR other_flag)"), row));
      }
    }
    Assertions.assertFalse(eval(filter("flag BOOLEAN", "flag = TRUE"), row(13, null)));
    Assertions.assertTrue(eval(filter("flag BOOLEAN", "flag <> TRUE"), row(13, false)));
  }

  @Test
  void testInAndNotInWithNullElements() {
    ReadRestrictions in = filter("region STRING", "region IN ('US', NULL)");
    ReadRestrictions notIn = filter("region STRING", "region NOT IN ('US', NULL)");
    Assertions.assertTrue(eval(in, row(2, "US")));
    for (Object value : new Object[] {"CA", null}) {
      Assertions.assertFalse(eval(in, row(2, value)));
      Assertions.assertFalse(eval(notIn, row(2, value)));
    }
    Assertions.assertFalse(eval(notIn, row(2, "US")));
    Assertions.assertFalse(eval(filter("region STRING", "NOT region IN (NULL)"), row(2, "US")));
    Assertions.assertTrue(eval(filter("region STRING", "region NOT IN ('US')"), row(2, "CA")));
    Assertions.assertTrue(filter("", "'CA' IN ('CA', NULL)").isEmpty());
    Assertions.assertEquals(
        Expression.Operation.FALSE, filter("", "'CA' NOT IN ('US', NULL)").rowFilter().op());
  }

  @Test
  void testRangesPrefixAndNaN() {
    Assertions.assertTrue(eval(filter("id INTEGER", "id BETWEEN 1 AND 3"), row(1, 1)));
    Assertions.assertTrue(eval(filter("id INTEGER", "id NOT BETWEEN 1 AND 3"), row(1, 4)));
    Assertions.assertTrue(
        eval(filter("region STRING", "region LIKE 'Acme%'"), row(2, "Acme Corp")));
    Assertions.assertFalse(eval(filter("region STRING", "region NOT LIKE 'Acme%'"), row(2, null)));
    Assertions.assertTrue(eval(filter("score FLOAT", "is_nan(score)"), row(8, Float.NaN)));
    Assertions.assertFalse(eval(filter("score FLOAT", "NOT is_nan(score)"), row(8, null)));
    Assertions.assertTrue(eval(filter("score FLOAT", "is_nan(score) = FALSE"), row(8, 0.5F)));
  }

  @Test
  void testSearchedSimpleAndNestedCase() {
    ReadRestrictions searched =
        filter(
            "region STRING",
            "CASE WHEN region = 'US' THEN TRUE WHEN region IS NULL THEN TRUE ELSE FALSE END");
    Assertions.assertTrue(eval(searched, row(2, null)));
    Assertions.assertTrue(eval(searched, row(2, "US")));
    Assertions.assertFalse(eval(searched, row(2, "CA")));
    ReadRestrictions simple =
        filter("region STRING", "CASE region WHEN NULL THEN TRUE WHEN 'US' THEN TRUE END");
    Assertions.assertFalse(eval(simple, row(2, null)));
    Assertions.assertTrue(eval(simple, row(2, "US")));
    Assertions.assertFalse(eval(simple, row(2, "CA")));
    ReadRestrictions negated =
        filter("region STRING", "NOT CASE WHEN region IS NULL THEN NULL ELSE region = 'US' END");
    Assertions.assertFalse(eval(negated, row(2, null)));
    Assertions.assertTrue(eval(negated, row(2, "CA")));
    Assertions.assertTrue(
        eval(
            filter("region STRING", "CASE region WHEN 'US' THEN 'Y' ELSE 'N' END = 'Y'"),
            row(2, "US")));
    Assertions.assertTrue(
        filter("", "CASE WHEN NULL THEN FALSE ELSE CASE 1 WHEN 1 THEN TRUE END END").isEmpty());
  }

  @Test
  void testContextCaseAndFirstMatchSkipUnselectedLookups() {
    AtomicInteger lookups = new AtomicInteger();
    BindingContext context =
        new BindingContext() {
          @Override
          public String sessionUser() {
            return "alice";
          }

          @Override
          public Optional<Boolean> groupMembership(String group) {
            lookups.incrementAndGet();
            return Optional.empty();
          }
        };
    Assertions.assertTrue(
        convert(
                "",
                "CASE WHEN TRUE THEN TRUE WHEN is_group_member('missing') THEN FALSE END",
                context)
            .isEmpty());
    Assertions.assertEquals(0, lookups.get());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> convert("", "FALSE AND is_group_member('missing')", context));
  }

  @Test
  void testContextAndMaskShareOneSnapshot() {
    AtomicInteger users = new AtomicInteger();
    AtomicInteger groups = new AtomicInteger();
    AtomicInteger attributes = new AtomicInteger();
    BindingContext context =
        new BindingContext() {
          @Override
          public String sessionUser() {
            users.incrementAndGet();
            return "alice";
          }

          @Override
          public Optional<Boolean> groupMembership(String name) {
            groups.incrementAndGet();
            return Optional.of(true);
          }

          @Override
          public Optional<Literal> userAttribute(String key) {
            attributes.incrementAndGet();
            return Optional.of(new Literal(LiteralType.STRING, "US", SqlType.STRING));
          }
        };
    SqlDefinition filter =
        parse(
            "region STRING",
            "region = user_attribute('region') AND is_group_member('auditors') AND region = user_attribute('region')");
    SqlDefinition mask =
        mask("STRING", "CASE WHEN is_group_member('auditors') THEN value ELSE NULL END");
    IcebergReadRestrictionConverter.convert(
        Optional.of(filter), Collections.singletonMap(2, mask), SCHEMA, context);
    Assertions.assertEquals(1, users.get());
    Assertions.assertEquals(1, groups.get());
    Assertions.assertEquals(1, attributes.get());
  }

  @Test
  void testTypedUserAttributesAndDeclaredNulls() {
    Literal absent = new Literal(LiteralType.NULL, "", SqlType.STRING);
    BindingContext context = context(Collections.singletonMap("region", absent));
    Assertions.assertEquals(
        Expression.Operation.FALSE,
        convert("region STRING", "region = user_attribute('region')", context).rowFilter().op());
    Assertions.assertTrue(convert("", "user_attribute('region') IS NULL", context).isEmpty());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> convert("id INTEGER", "id = user_attribute('region')", context));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> convert("", "user_attribute('region') = 1", context));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> filter("", "user_attribute('unknown') IS NULL"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> filter("", "is_group_member('unknown')"));
  }

  @Test
  void testExactDeclarationBindingAndAmbiguity() {
    for (String params :
        Arrays.asList(
            "id LONG",
            "amount DECIMAL(8,2)",
            "event_ts TIMESTAMP WITH TIME ZONE",
            "missing STRING")) {
      Assertions.assertThrows(IllegalArgumentException.class, () -> filter(params, "TRUE"));
    }
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> filter("missing STRING", "CASE WHEN TRUE THEN TRUE ELSE missing = 'x' END"));
    Schema ambiguous =
        new Schema(
            Types.NestedField.optional(1, "region", Types.StringType.get()),
            Types.NestedField.optional(2, "REGION", Types.StringType.get()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergReadRestrictionConverter.convert(
                Optional.of(parse("region STRING", "TRUE")),
                Collections.emptyMap(),
                ambiguous,
                context()));
    ReadRestrictions exact =
        IcebergReadRestrictionConverter.convert(
            Optional.of(parse("\"region\" STRING", "\"region\" = 'US'")),
            Collections.emptyMap(),
            ambiguous,
            context());
    Assertions.assertEquals(1, ((BoundLiteralPredicate<?>) exact.rowFilter()).ref().fieldId());
    ReadRestrictions uppercase =
        IcebergReadRestrictionConverter.convert(
            Optional.of(parse("\"REGION\" STRING", "\"REGION\" = 'US'")),
            Collections.emptyMap(),
            ambiguous,
            context());
    Assertions.assertEquals(2, ((BoundLiteralPredicate<?>) uppercase.rowFilter()).ref().fieldId());
  }

  @Test
  void testEveryLiteralFamilyAndCalendarYear() {
    String params =
        "id INTEGER, counter LONG, amount DECIMAL(7,2), event_date DATE, event_time TIME, event_ts TIMESTAMP, zoned TIMESTAMP WITH TIME ZONE, score FLOAT, double_score DOUBLE";
    String body =
        "id = 7 AND counter = 8 AND amount = 12.30 AND event_date = DATE '2026-10-12' AND event_time = TIME '12:34:56.123456' AND event_ts = TIMESTAMP '2026-10-12 12:34:56.123456' AND zoned = TIMESTAMP WITH TIME ZONE '2026-10-12 08:34:56.123456-04:00' AND score = 0.5 AND double_score = 0.5";
    ReadRestrictions result = filter(params, body);
    String json = ReadRestrictionsParser.toJson(result);
    Assertions.assertEquals(json, reboundJson(json));
    Assertions.assertTrue(
        eval(
            filter("event_date DATE", "year(event_date) = 2026"),
            row(5, Math.toIntExact(LocalDate.of(2026, 10, 12).toEpochDay()))));
    Assertions.assertTrue(
        filter("", "month(DATE '2026-03-12') = 3 AND day(DATE '2026-03-12') = 12").isEmpty());
  }

  @Test
  void testExactNumericBindingAndNegativeZero() {
    for (String body : Arrays.asList("score = 0.1", "score = 16777217", "double_score = 0.1")) {
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> filter("score FLOAT, double_score DOUBLE", body));
    }
    BoundLiteralPredicate<?> zero =
        (BoundLiteralPredicate<?>) filter("score FLOAT", "score = -0.0").rowFilter();
    Assertions.assertEquals(
        Float.floatToRawIntBits(-0.0F), Float.floatToRawIntBits((Float) zero.literal().value()));
    BoundLiteralPredicate<?> doubleZero =
        (BoundLiteralPredicate<?>) filter("double_score DOUBLE", "double_score = -0.0").rowFilter();
    Assertions.assertEquals(
        Double.doubleToRawLongBits(-0.0D),
        Double.doubleToRawLongBits((Double) doubleZero.literal().value()));
  }

  @Test
  void testNullTestsOnRequiredAndOptionalFields() {
    Assertions.assertEquals(
        Expression.Operation.FALSE, filter("id INTEGER", "id IS NULL").rowFilter().op());
    Assertions.assertTrue(filter("id INTEGER", "id IS NOT NULL").isEmpty());
    Assertions.assertTrue(eval(filter("region STRING", "region IS NULL"), row(2, null)));
    Assertions.assertFalse(eval(filter("region STRING", "region IS NOT NULL"), row(2, null)));
  }

  @Test
  void testDeterministicCanonicalization() {
    ReadRestrictions a =
        filter("id INTEGER, region STRING", "id IN (2,1,2) AND region = 'US' AND region = 'US'");
    ReadRestrictions b = filter("region STRING, id INTEGER", "region = 'US' AND id IN (1,2)");
    Assertions.assertEquals(ReadRestrictionsParser.toJson(a), ReadRestrictionsParser.toJson(b));
    Assertions.assertEquals(
        ReadRestrictionsParser.toJson(filter("region STRING", "NOT region = 'US'")),
        ReadRestrictionsParser.toJson(filter("region STRING", "region <> 'US'")));
  }

  @Test
  void testIndependentMaskParameterBindingAndIdentity() {
    SqlDefinition definition =
        RestrictedSqlExpressionParserFacade.parseColumnMask(
            "(xxx STRING) RETURNS STRING -> show_last_4(xxx)");
    Map<Integer, SqlDefinition> masks = new HashMap<>();
    masks.put(3, definition);
    masks.put(2, definition);
    ReadRestrictions result =
        IcebergReadRestrictionConverter.convert(Optional.empty(), masks, SCHEMA, context());
    Assertions.assertEquals(2, result.columnProjections().get(0).fieldId());
    Assertions.assertEquals(3, result.columnProjections().get(1).fieldId());
    Assertions.assertTrue(masks(2, mask("STRING", "value")).isEmpty());
    Assertions.assertThrows(IllegalArgumentException.class, () -> masks(1, definition));
    Assertions.assertThrows(IllegalArgumentException.class, () -> masks(11, definition));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergReadRestrictionConverter.convert(
                Optional.of(mask("BOOLEAN", "value")), Collections.emptyMap(), SCHEMA, context()));
  }

  @Test
  void testAllNineMaskFunctionsAndWireRoundTrip() {
    Map<String, Integer> functions = new LinkedHashMap<>();
    functions.put("mask_alphanum", 2);
    functions.put("mask_to_fixed_value", 1);
    functions.put("replace_with_null", 2);
    functions.put("show_first_4", 2);
    functions.put("show_last_4", 2);
    functions.put("truncate_to_year", 5);
    functions.put("truncate_to_month", 14);
    functions.put("sha256_global", 1);
    functions.put("sha256_query_local", 2);
    for (Map.Entry<String, Integer> entry : functions.entrySet()) {
      String type =
          entry.getValue() == 1
              ? "INTEGER"
              : entry.getValue() == 5
                  ? "DATE"
                  : entry.getValue() == 14 ? "TIMESTAMP WITH TIME ZONE" : "STRING";
      ReadRestrictions result = masks(entry.getValue(), mask(type, entry.getKey() + "(value)"));
      Assertions.assertEquals(
          entry.getValue().intValue(), result.columnProjections().get(0).fieldId());
      String json = ReadRestrictionsParser.toJson(result);
      Assertions.assertEquals(json, reboundJson(json));
    }
  }

  @Test
  void testMaskCaseSelectionAndNullableResults() {
    Assertions.assertEquals(
        "show-last-4",
        masks(
                2,
                mask(
                    "STRING",
                    "CASE WHEN CURRENT_USER() = 'alice' THEN show_last_4(value) ELSE NULL END"))
            .columnProjections()
            .get(0)
            .name());
    Assertions.assertEquals(
        "replace-with-null",
        masks(2, mask("STRING", "CASE CURRENT_USER() WHEN 'bob' THEN value END"))
            .columnProjections()
            .get(0)
            .name());
    Assertions.assertEquals(
        "show-first-4",
        masks(
                2,
                mask(
                    "STRING",
                    "CASE WHEN NULL THEN show_last_4(value) ELSE show_first_4(value) END"))
            .columnProjections()
            .get(0)
            .name());
    Assertions.assertThrows(IllegalArgumentException.class, () -> masks(9, mask("STRING", "NULL")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> masks(9, mask("STRING", "CASE WHEN TRUE THEN value ELSE NULL END")));
    Assertions.assertEquals(
        "show-last-4",
        masks(
                2,
                mask(
                    "STRING",
                    "CASE WHEN TRUE THEN show_last_4(value) WHEN is_group_member('missing') THEN value ELSE NULL END"))
            .columnProjections()
            .get(0)
            .name());
  }

  @Test
  void testFixedLiteralsAndUnsupportedScalarMasks() {
    Assertions.assertEquals(
        "mask-to-fixed-value",
        masks(2, mask("STRING", "'XXXXXXXX'")).columnProjections().get(0).name());
    Assertions.assertEquals(
        "mask-to-fixed-value", masks(1, mask("INTEGER", "0")).columnProjections().get(0).name());
    Assertions.assertEquals(
        "mask-to-fixed-value",
        masks(4, mask("DECIMAL(7,2)", "0.00")).columnProjections().get(0).name());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> masks(2, mask("STRING", "'REDACTED'")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> masks(2, mask("STRING", "show_last_4(mask_alphanum(value))")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            masks(
                2,
                mask("STRING", "CASE WHEN value IS NULL THEN NULL ELSE show_last_4(value) END")));
  }

  @Test
  void testUnsupportedTargetExpressionsFailClosed() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> filter("region STRING, owner STRING", "region = owner"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> filter("event_date DATE", "month(event_date) = 3"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> filter("event_date DATE", "day(event_date) = 15"));
    StringBuilder body = new StringBuilder("CASE ");
    for (int i = 0; i < 130; i++) {
      body.append("WHEN id = ").append(i).append(" THEN TRUE ");
    }
    body.append("END");
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> filter("id INTEGER", body.toString()));
    Assertions.assertTrue(exception.getMessage().contains("complexity"));
  }

  @Test
  void testComparisonCaseExpansionHasBoundedWork() {
    StringBuilder choice = new StringBuilder("CASE ");
    for (int i = 0; i < 80; i++) {
      choice.append("WHEN id = ").append(i).append(" THEN ").append(i).append(' ');
    }
    choice.append("END");
    String body = choice + " = " + choice;
    // The source fits the SQL node limit; translating both CASE operands would expand its work.
    SqlDefinition definition = parse("id INTEGER", body);
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergReadRestrictionConverter.convert(
                    Optional.of(definition), Collections.emptyMap(), SCHEMA, context()));
    Assertions.assertTrue(error.getMessage().contains("work limit"));
  }

  @Test
  void testWideExpressionsAndThousandElementMembership() {
    String list =
        IntStream.range(0, 1000).mapToObj(Integer::toString).collect(Collectors.joining(","));
    Assertions.assertTrue(eval(filter("id INTEGER", "id IN (" + list + ")"), row(1, 999)));
    String body = String.join(" OR ", Collections.nCopies(300, "id = 1"));
    Assertions.assertTrue(eval(filter("id INTEGER", body), row(1, 1)));
  }

  private static String reboundJson(String json) {
    ReadRestrictions parsed = ReadRestrictionsParser.fromJson(json, SCHEMA);
    Expression filter =
        parsed.rowFilter() == null
            ? null
            : Binder.bind(SCHEMA.asStruct(), parsed.rowFilter(), true);
    return ReadRestrictionsParser.toJson(ReadRestrictions.of(filter, parsed.columnProjections()));
  }

  private static SqlDefinition parse(String parameters, String body) {
    return RestrictedSqlExpressionParserFacade.parseRowFilter(
        "(" + parameters + ") RETURNS BOOLEAN -> " + body);
  }

  private static SqlDefinition mask(String type, String body) {
    return RestrictedSqlExpressionParserFacade.parseColumnMask(
        "(value " + type + ") RETURNS " + type + " -> " + body);
  }

  private static ReadRestrictions filter(String parameters, String body) {
    return convert(parameters, body, context());
  }

  private static ReadRestrictions convert(String parameters, String body, BindingContext context) {
    return IcebergReadRestrictionConverter.convert(
        Optional.of(parse(parameters, body)), Collections.emptyMap(), SCHEMA, context);
  }

  private static ReadRestrictions masks(int field, SqlDefinition mask) {
    return IcebergReadRestrictionConverter.convert(
        Optional.empty(), Collections.singletonMap(field, mask), SCHEMA, context());
  }

  private static BindingContext context() {
    return context(Collections.emptyMap());
  }

  private static BindingContext context(Map<String, Literal> attributes) {
    return new BindingContext() {
      @Override
      public String sessionUser() {
        return "alice";
      }

      @Override
      public Optional<Boolean> groupMembership(String group) {
        return Optional.empty();
      }

      @Override
      public Optional<Literal> userAttribute(String key) {
        return Optional.ofNullable(attributes.get(key));
      }
    };
  }

  private static Map<Integer, Object> row(Object... entries) {
    Map<Integer, Object> values = new HashMap<>();
    values.put(1, 1);
    for (int i = 0; i < entries.length; i += 2) {
      values.put((Integer) entries[i], entries[i + 1]);
    }
    return values;
  }

  private static boolean eval(ReadRestrictions restrictions, Map<Integer, Object> values) {
    if (restrictions.rowFilter() == null) {
      return true;
    }
    StructLike row =
        new StructLike() {
          @Override
          public int size() {
            return SCHEMA.columns().size();
          }

          @Override
          public <T> T get(int pos, Class<T> type) {
            return type.cast(values.get(SCHEMA.columns().get(pos).fieldId()));
          }

          @Override
          public <T> void set(int pos, T value) {
            throw new UnsupportedOperationException();
          }
        };
    return ExpressionVisitors.visit(
        restrictions.rowFilter(),
        new ExpressionVisitors.ExpressionVisitor<Boolean>() {
          @Override
          public Boolean alwaysTrue() {
            return true;
          }

          @Override
          public Boolean alwaysFalse() {
            return false;
          }

          @Override
          public Boolean and(Boolean a, Boolean b) {
            return a && b;
          }

          @Override
          public Boolean or(Boolean a, Boolean b) {
            return a || b;
          }

          @Override
          public Boolean not(Boolean a) {
            return !a;
          }

          @Override
          public <T> Boolean predicate(BoundPredicate<T> predicate) {
            return predicate.test(row);
          }
        });
  }
}
