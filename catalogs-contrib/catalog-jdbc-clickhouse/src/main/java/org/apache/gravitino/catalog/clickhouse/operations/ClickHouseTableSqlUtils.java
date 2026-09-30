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
package org.apache.gravitino.catalog.clickhouse.operations;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;

final class ClickHouseTableSqlUtils {

  private static final Pattern TO_DATE_PATTERN =
      Pattern.compile("toDate\\((.+)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern TO_YEAR_PATTERN =
      Pattern.compile("toYear\\((.+)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern TO_MONTH_PATTERN =
      Pattern.compile("toYYYYMM\\((.+)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern TO_START_OF_WEEK_PATTERN =
      Pattern.compile("toStartOfWeek[(](.+)[)]", Pattern.CASE_INSENSITIVE);
  private static final Pattern TO_START_OF_MONTH_PATTERN =
      Pattern.compile("toStartOfMonth[(](.+)[)]", Pattern.CASE_INSENSITIVE);
  private static final Pattern FUNCTION_WRAPPER_PATTERN =
      Pattern.compile("^\\s*([A-Za-z0-9_]+)\\((.*)\\)\\s*$");
  private static final Pattern PROJECTION_SETTING_NAME_PATTERN =
      Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
  private static final Pattern NUMERIC_SETTING_LITERAL_PATTERN =
      Pattern.compile("^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?$");
  private static final Pattern IDENTIFIER_SETTING_LITERAL_PATTERN =
      Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
  private static final Set<String> PROJECTION_PROPERTY_FIELDS =
      Set.of("name", "type", "query", "settings");
  private static final ObjectMapper PROJECTION_JSON_MAPPER =
      new ObjectMapper()
          .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  private ClickHouseTableSqlUtils() {}

  record ProjectionDefinition(
      String name, String type, String query, Map<String, String> settings) {}

  static String serializeProjectionDefinitions(List<ProjectionDefinition> definitions) {
    ArrayNode root = PROJECTION_JSON_MAPPER.createArrayNode();
    Set<String> projectionNames = new HashSet<>();
    definitions.stream()
        .sorted(Comparator.comparing(ProjectionDefinition::name))
        .forEach(
            definition -> {
              validateProjectionDefinition(definition);
              Preconditions.checkArgument(
                  projectionNames.add(definition.name()), "Duplicate ClickHouse projection name");
              ObjectNode projection = root.addObject();
              projection.put("name", definition.name());
              projection.put("type", definition.type());
              projection.put("query", definition.query().trim());
              ObjectNode settings = projection.putObject("settings");
              new TreeMap<>(definition.settings()).forEach(settings::put);
            });
    try {
      return PROJECTION_JSON_MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to serialize ClickHouse projection metadata", e);
    }
  }

  static List<ProjectionDefinition> parseProjectionDefinitions(@Nullable String json) {
    if (StringUtils.isBlank(json)) {
      return Collections.emptyList();
    }

    JsonNode root;
    try {
      root = PROJECTION_JSON_MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid ClickHouse projection property JSON", e);
    }
    Preconditions.checkArgument(
        root != null && root.isArray(), "Projection property must be a JSON array");

    List<ProjectionDefinition> definitions = new ArrayList<>();
    Set<String> projectionNames = new HashSet<>();
    for (JsonNode projection : root) {
      Preconditions.checkArgument(
          projection.isObject(), "Each projection definition must be a JSON object");
      Iterator<String> fields = projection.fieldNames();
      while (fields.hasNext()) {
        String field = fields.next();
        Preconditions.checkArgument(
            PROJECTION_PROPERTY_FIELDS.contains(field),
            "Unsupported ClickHouse projection property field: %s",
            field);
      }

      String name = requiredText(projection, "name");
      String type = requiredText(projection, "type");
      String query = requiredText(projection, "query");
      Map<String, String> settings = parseProjectionSettings(projection.get("settings"));
      ProjectionDefinition definition = new ProjectionDefinition(name, type, query, settings);
      validateProjectionDefinition(definition);
      Preconditions.checkArgument(
          projectionNames.add(name), "Duplicate ClickHouse projection name");
      definitions.add(definition);
    }
    definitions.sort(Comparator.comparing(ProjectionDefinition::name));
    return Collections.unmodifiableList(definitions);
  }

  static String formatProjectionClauses(List<ProjectionDefinition> definitions) {
    StringBuilder sqlBuilder = new StringBuilder();
    for (ProjectionDefinition definition : definitions) {
      validateProjectionDefinition(definition);
      sqlBuilder
          .append(",\n PROJECTION ")
          .append(quoteProjectionIdentifier(definition.name()))
          .append(" (\n  ")
          .append(definition.query().trim())
          .append("\n )");
      if (!definition.settings().isEmpty()) {
        String settings =
            new TreeMap<>(definition.settings())
                .entrySet().stream()
                    .map(entry -> entry.getKey() + " = " + entry.getValue())
                    .collect(java.util.stream.Collectors.joining(", "));
        sqlBuilder.append(" WITH SETTINGS (").append(settings).append(")");
      }
    }
    return sqlBuilder.toString();
  }

  private static String requiredText(JsonNode object, String field) {
    JsonNode value = object.get(field);
    Preconditions.checkArgument(
        value != null && value.isTextual() && StringUtils.isNotBlank(value.asText()),
        "ClickHouse projection property must include non-empty text field '%s'",
        field);
    return value.asText();
  }

  static Map<String, String> parseProjectionSettings(String json) {
    JsonNode settingsNode;
    try {
      settingsNode = PROJECTION_JSON_MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid ClickHouse projection settings JSON", e);
    }
    return parseProjectionSettings(settingsNode);
  }

  private static Map<String, String> parseProjectionSettings(@Nullable JsonNode settingsNode) {
    if (settingsNode == null) {
      return Collections.emptyMap();
    }
    Preconditions.checkArgument(
        settingsNode.isObject(), "Projection settings must be a JSON object");

    Map<String, String> settings = new TreeMap<>();
    Iterator<Map.Entry<String, JsonNode>> fields = settingsNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      JsonNode value = field.getValue();
      Preconditions.checkArgument(
          PROJECTION_SETTING_NAME_PATTERN.matcher(field.getKey()).matches(),
          "Invalid ClickHouse projection setting name");
      Preconditions.checkArgument(
          value.isTextual() && isSafeSettingLiteral(value.asText()),
          "Unsupported ClickHouse projection setting value");
      settings.put(field.getKey(), value.asText().trim());
    }
    return Collections.unmodifiableMap(settings);
  }

  private static void validateProjectionDefinition(ProjectionDefinition definition) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(definition.name()), "ClickHouse projection name is required");
    Preconditions.checkArgument(
        definition.name().chars().noneMatch(Character::isISOControl),
        "ClickHouse projection name contains a control character");
    Preconditions.checkArgument(
        "Normal".equals(definition.type()) || "Aggregate".equals(definition.type()),
        "Unsupported ClickHouse projection type: %s",
        definition.type());
    Preconditions.checkArgument(
        StringUtils.isNotBlank(definition.query()) && isSafeProjectionQuery(definition.query()),
        "Invalid ClickHouse projection query");
    Preconditions.checkArgument(
        definition.settings() != null, "Projection settings cannot be null");
    definition
        .settings()
        .forEach(
            (name, value) -> {
              Preconditions.checkArgument(
                  PROJECTION_SETTING_NAME_PATTERN.matcher(name).matches(),
                  "Invalid ClickHouse projection setting name");
              Preconditions.checkArgument(
                  isSafeSettingLiteral(value), "Unsupported ClickHouse projection setting value");
            });
  }

  private static boolean isSafeSettingLiteral(String value) {
    String literal = StringUtils.trimToEmpty(value);
    return NUMERIC_SETTING_LITERAL_PATTERN.matcher(literal).matches()
        || "true".equalsIgnoreCase(literal)
        || "false".equalsIgnoreCase(literal)
        || IDENTIFIER_SETTING_LITERAL_PATTERN.matcher(literal).matches()
        || isValidQuotedSettingLiteral(literal);
  }

  private static boolean isValidQuotedSettingLiteral(String literal) {
    if (literal.length() < 2
        || literal.charAt(0) != '\''
        || literal.charAt(literal.length() - 1) != '\'') {
      return false;
    }
    for (int i = 1; i < literal.length() - 1; i++) {
      char current = literal.charAt(i);
      if (current == '\\') {
        if (i + 1 >= literal.length() - 1) {
          return false;
        }
        i++;
      } else if (current == '\'') {
        if (i + 1 >= literal.length() - 1 || literal.charAt(i + 1) != '\'') {
          return false;
        }
        i++;
      }
    }
    return true;
  }

  private static boolean isSafeProjectionQuery(String query) {
    int depth = 0;
    char quote = 0;
    boolean identifierQuote = false;
    StringBuilder quotedIdentifier = new StringBuilder();
    boolean hasSelect = false;
    StringBuilder token = new StringBuilder();
    for (int i = 0; i < query.length(); i++) {
      char current = query.charAt(i);
      if (quote != 0) {
        if (current == '\\' && i + 1 < query.length()) {
          char escaped = query.charAt(++i);
          if (identifierQuote) {
            quotedIdentifier.append(escaped);
          }
        } else if (current == quote) {
          if (i + 1 < query.length() && query.charAt(i + 1) == quote) {
            if (identifierQuote) {
              quotedIdentifier.append(current);
            }
            i++;
          } else {
            if (identifierQuote && "_part_offset".equalsIgnoreCase(quotedIdentifier.toString())) {
              return false;
            }
            quote = 0;
            identifierQuote = false;
            quotedIdentifier.setLength(0);
          }
        } else if (identifierQuote) {
          quotedIdentifier.append(current);
        }
        continue;
      }

      if (Character.isLetterOrDigit(current) || current == '_') {
        token.append(Character.toLowerCase(current));
        continue;
      }
      if (!token.isEmpty()) {
        String value = token.toString();
        if ("_part_offset".equals(value) || (depth == 0 && "where".equals(value))) {
          return false;
        }
        hasSelect |= "select".equals(value);
        token.setLength(0);
      }

      if (current == '\'' || current == '"' || current == '`') {
        quote = current;
        identifierQuote = current != '\'';
        quotedIdentifier.setLength(0);
      } else if (current == ';' || current == '#') {
        return false;
      } else if (current == '-' && i + 1 < query.length() && query.charAt(i + 1) == '-') {
        return false;
      } else if (current == '/'
          && i + 1 < query.length()
          && (query.charAt(i + 1) == '*' || query.charAt(i + 1) == '/')) {
        return false;
      } else if (current == '(') {
        depth++;
      } else if (current == ')' && --depth < 0) {
        return false;
      }
    }
    if (!token.isEmpty()) {
      String value = token.toString();
      if ("_part_offset".equals(value) || (depth == 0 && "where".equals(value))) {
        return false;
      }
      hasSelect |= "select".equals(value);
    }
    return quote == 0 && depth == 0 && hasSelect;
  }

  private static String quoteProjectionIdentifier(String identifier) {
    Preconditions.checkArgument(StringUtils.isNotBlank(identifier), "Projection name is required");
    return "`" + identifier.replace("\\", "\\\\").replace("`", "\\`") + "`";
  }

  static Transform[] parsePartitioning(@Nullable String partitionKey) {
    if (StringUtils.isBlank(partitionKey)) {
      return Transforms.EMPTY_TRANSFORM;
    }

    String trimmedKey = normalizePartitionKey(partitionKey);
    if (StringUtils.isBlank(trimmedKey)) {
      return Transforms.EMPTY_TRANSFORM;
    }

    String[] parts = trimmedKey.split(",");
    List<Transform> transforms = new ArrayList<>();
    for (String part : parts) {
      String expression = StringUtils.trim(part);
      if (StringUtils.isBlank(expression)) {
        continue;
      }
      Transform transform = parsePartitionExpression(expression);
      if (transform == null) {
        // A single unsupported native expression means the whole partition key cannot be
        // represented as structured transforms.
        return Transforms.EMPTY_TRANSFORM;
      }
      transforms.add(transform);
    }

    return transforms.toArray(new Transform[0]);
  }

  static String toPartitionExpression(Transform transform) {
    Preconditions.checkArgument(transform != null, "Partition transform cannot be null");
    String name = transform.name().toLowerCase(Locale.ROOT);
    return switch (name) {
      case Transforms.NAME_OF_IDENTITY -> quoteIdentifier(partitionFieldName(transform));
      case Transforms.NAME_OF_YEAR -> "toYear(%s)"
          .formatted(quoteIdentifier(partitionFieldName(transform)));
      case Transforms.NAME_OF_MONTH -> "toYYYYMM(%s)"
          .formatted(quoteIdentifier(partitionFieldName(transform)));
      case Transforms.NAME_OF_DAY -> "toDate(%s)"
          .formatted(quoteIdentifier(partitionFieldName(transform)));
      case "tostartofweek" -> "toStartOfWeek(%s)"
          .formatted(quoteIdentifier(partitionFieldName(transform)));
      case "tostartofmonth" -> "toStartOfMonth(%s)"
          .formatted(quoteIdentifier(partitionFieldName(transform)));
      default -> throw new IllegalArgumentException(
          "Unsupported partition transform: " + transform.name());
    };
  }

  static List<String> extractShardingKeyColumns(String shardingKey) {
    String normalized = normalizeShardingKeyExpression(shardingKey);
    if (StringUtils.isBlank(normalized)) {
      return Collections.emptyList();
    }

    String[] parts = normalized.split(",");
    List<String> columns = new ArrayList<>();
    for (String part : parts) {
      String column = normalizeIdentifier(part);
      Preconditions.checkArgument(
          isSimpleIdentifier(column), "Sharding key contains unsupported expression: %s", part);
      columns.add(column);
    }
    return ImmutableList.copyOf(columns);
  }

  static String formatShardingKey(String shardingKey) {
    String trimmed = StringUtils.trim(shardingKey);
    if (StringUtils.isBlank(trimmed)) {
      return trimmed;
    }

    String normalized = normalizeIdentifier(trimmed);
    if (isSimpleIdentifier(normalized)) {
      return quoteIdentifier(normalized);
    }
    return trimmed;
  }

  static String[][] parseIndexFields(String expression) {
    if (StringUtils.isBlank(expression)) {
      return new String[0][];
    }

    String normalized = normalizeIndexExpression(expression);
    if (StringUtils.isBlank(normalized)) {
      return new String[0][];
    }

    String[] parts = normalized.split(",");
    List<String[]> fields = new ArrayList<>();
    for (String part : parts) {
      String col = normalizeIdentifier(part);
      Preconditions.checkArgument(
          isSimpleIdentifier(col), "Unsupported index expression: " + expression);
      fields.add(new String[] {col});
    }

    return fields.toArray(new String[0][]);
  }

  static String normalizeIndexExpression(String expression) {
    String current = expression.trim();

    if (StringUtils.startsWithIgnoreCase(current, "tuple(") && StringUtils.endsWith(current, ")")) {
      current = current.substring("tuple(".length(), current.length() - 1).trim();
    }

    return current;
  }

  static String normalizeIdentifier(String identifier) {
    String col = StringUtils.trim(identifier);
    if (StringUtils.startsWith(col, "`") && StringUtils.endsWith(col, "`") && col.length() >= 2) {
      return col.substring(1, col.length() - 1);
    }
    return col;
  }

  static boolean isSimpleIdentifier(String identifier) {
    return StringUtils.isNotBlank(identifier)
        && !StringUtils.containsAny(identifier, "(", ")", " ", "%", "+", "-", "*", "/");
  }

  private static boolean isStrictIdentifier(String identifier) {
    return StringUtils.isNotBlank(identifier) && identifier.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
  }

  private static String normalizeShardingKeyExpression(String expression) {
    String current = expression.trim();

    boolean stripped = true;
    while (stripped) {
      stripped = false;
      Matcher matcher = FUNCTION_WRAPPER_PATTERN.matcher(current);
      if (matcher.matches()) {
        current = matcher.group(2).trim();
        stripped = true;
      }
    }

    return normalizeIndexExpression(current);
  }

  @Nullable
  private static Transform parsePartitionExpression(String expression) {
    String trimmedExpression = StringUtils.trim(expression);

    Matcher toYearMatcher = TO_YEAR_PATTERN.matcher(trimmedExpression);
    if (toYearMatcher.matches()) {
      String identifier = extractPartitionIdentifier(toYearMatcher.group(1));
      return identifier == null ? null : Transforms.year(identifier);
    }

    Matcher toYYYYMMMatcher = TO_MONTH_PATTERN.matcher(trimmedExpression);
    if (toYYYYMMMatcher.matches()) {
      String identifier = extractPartitionIdentifier(toYYYYMMMatcher.group(1));
      return identifier == null ? null : Transforms.month(identifier);
    }

    Matcher toDateMatcher = TO_DATE_PATTERN.matcher(trimmedExpression);
    if (toDateMatcher.matches()) {
      String identifier = extractPartitionIdentifier(toDateMatcher.group(1));
      return identifier == null ? null : Transforms.day(identifier);
    }

    Matcher toStartOfWeekMatcher = TO_START_OF_WEEK_PATTERN.matcher(trimmedExpression);
    if (toStartOfWeekMatcher.matches()) {
      String identifier = extractPartitionIdentifier(toStartOfWeekMatcher.group(1));
      return identifier == null
          ? null
          : Transforms.apply("toStartOfWeek", new Expression[] {NamedReference.field(identifier)});
    }

    Matcher toStartOfMonthMatcher = TO_START_OF_MONTH_PATTERN.matcher(trimmedExpression);
    if (toStartOfMonthMatcher.matches()) {
      String identifier = extractPartitionIdentifier(toStartOfMonthMatcher.group(1));
      return identifier == null
          ? null
          : Transforms.apply("toStartOfMonth", new Expression[] {NamedReference.field(identifier)});
    }

    String identifier = extractPartitionIdentifier(trimmedExpression);
    return identifier == null ? null : Transforms.identity(identifier);
  }

  /**
   * Extracts a partition column name from an expression. A backtick-quoted identifier (which may
   * contain special characters such as {@code -}) is always treated as a column name. Otherwise the
   * expression must match the strict column-name pattern. Returns {@code null} for arbitrary
   * expressions such as {@code f(x)} that cannot be represented as a single column reference.
   */
  @Nullable
  private static String extractPartitionIdentifier(String expression) {
    String trimmed = StringUtils.trim(expression);
    if (StringUtils.startsWith(trimmed, "`")
        && StringUtils.endsWith(trimmed, "`")
        && trimmed.length() >= 2) {
      String inner = trimmed.substring(1, trimmed.length() - 1);
      return StringUtils.isNotBlank(inner) ? inner : null;
    }
    return isStrictIdentifier(trimmed) ? trimmed : null;
  }

  private static String normalizePartitionKey(String partitionKey) {
    String trimmedKey = partitionKey.trim();
    if (StringUtils.equalsIgnoreCase(trimmedKey, "tuple()")) {
      return "";
    }
    if (StringUtils.startsWithIgnoreCase(trimmedKey, "tuple(")
        && StringUtils.endsWith(trimmedKey, ")")) {
      return trimmedKey.substring("tuple(".length(), trimmedKey.length() - 1).trim();
    }
    if (StringUtils.startsWith(trimmedKey, "(") && StringUtils.endsWith(trimmedKey, ")")) {
      return trimmedKey.substring(1, trimmedKey.length() - 1).trim();
    }
    return trimmedKey;
  }

  private static String quoteIdentifier(String identifier) {
    return String.format("`%s`", identifier);
  }

  private static String partitionFieldName(Transform transform) {
    Preconditions.checkArgument(
        transform.arguments().length == 1
            && transform.arguments()[0] instanceof NamedReference
            && ((NamedReference) transform.arguments()[0]).fieldName().length == 1,
        "ClickHouse partition transform only supports a single column reference");

    return ((NamedReference) transform.arguments()[0]).fieldName()[0];
  }
}
