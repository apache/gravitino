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
package org.apache.gravitino.catalog.hive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.ExceptionMessages;

/**
 * Encodes and decodes Trino/Presto's native "Presto View" HMS view format, so that views created by
 * Gravitino's Trino dialect are readable by a native Trino Hive connector (and vice versa).
 *
 * <p>Trino recognizes a Hive Metastore VIRTUAL_VIEW table as a Trino view when it carries the
 * {@code presto_view=true} table parameter and its {@code comment} table parameter equals {@code
 * "Presto View"}; the view body is stored in {@code viewOriginalText} as {@code "/* Presto View: "
 * + base64(json) + " * /"}. The JSON payload mirrors Trino's {@code ConnectorViewDefinition} wire
 * format (see {@code io.trino.plugin.hive.ViewReaderUtil} /{@code
 * io.trino.spi.connector.ConnectorViewDefinition} in the Trino source tree): {@code Optional}
 * fields are serialized as the raw value or JSON {@code null}, never omitted, since Trino's decoder
 * requires every field to be present.
 */
final class TrinoNativeViewCodec {

  static final String PRESTO_VIEW_FLAG = "presto_view";
  static final String PRESTO_VIEW_COMMENT = "Presto View";

  private static final String VIEW_PREFIX = "/* Presto View: ";
  private static final String VIEW_SUFFIX = " */";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  // Trino's default time/timestamp precision (milliseconds) when none is specified.
  private static final int DEFAULT_PRECISION = 3;

  private TrinoNativeViewCodec() {}

  /** A single view output column, mirroring Trino's {@code ConnectorViewDefinition.ViewColumn}. */
  static final class ViewColumn {
    final String name;
    final String type;
    @Nullable final String comment;

    ViewColumn(String name, String type, @Nullable String comment) {
      this.name = name;
      this.type = type;
      this.comment = comment;
    }
  }

  /** Mirrors the fields of Trino's {@code ConnectorViewDefinition}. */
  static final class ViewDefinition {
    final String originalSql;
    @Nullable final String catalog;
    @Nullable final String schema;
    final List<ViewColumn> columns;
    @Nullable final String comment;
    @Nullable final String owner;
    final boolean runAsInvoker;
    // Trino's SQL path setting for the view; Gravitino's view model has no equivalent, so this is
    // only populated on decode() (to let callers detect and reject a non-empty path) and is always
    // written back empty by encode().
    final List<String> path;

    ViewDefinition(
        String originalSql,
        @Nullable String catalog,
        @Nullable String schema,
        List<ViewColumn> columns,
        @Nullable String comment,
        @Nullable String owner,
        boolean runAsInvoker,
        List<String> path) {
      this.originalSql = originalSql;
      this.catalog = catalog;
      this.schema = schema;
      this.columns = columns;
      this.comment = comment;
      this.owner = owner;
      this.runAsInvoker = runAsInvoker;
      this.path = path;
    }
  }

  /**
   * Encodes a view definition into Trino's native {@code viewOriginalText} format.
   *
   * @param definition the view definition to encode
   * @return the encoded {@code "/* Presto View: ... * /"} string
   */
  static String encode(ViewDefinition definition) {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("originalSql", definition.originalSql);
    root.put("catalog", definition.catalog);
    root.put("schema", definition.schema);

    ArrayNode columns = root.putArray("columns");
    for (ViewColumn column : definition.columns) {
      ObjectNode columnNode = columns.addObject();
      columnNode.put("name", column.name);
      columnNode.put("type", column.type);
      columnNode.put("comment", column.comment);
    }

    root.put("comment", definition.comment);
    root.put("owner", definition.owner);
    root.put("runAsInvoker", definition.runAsInvoker);
    root.putArray("path");

    byte[] bytes;
    try {
      bytes = MAPPER.writeValueAsBytes(root);
    } catch (JsonProcessingException e) {
      throw ExceptionMessages.wrap("Failed to encode Trino native view definition", e);
    }
    return VIEW_PREFIX + Base64.getEncoder().encodeToString(bytes) + VIEW_SUFFIX;
  }

  /**
   * Decodes a Trino native {@code viewOriginalText} value.
   *
   * @param viewOriginalText the raw {@code viewOriginalText} HMS field value
   * @return the decoded view definition
   */
  static ViewDefinition decode(String viewOriginalText) {
    if (viewOriginalText == null
        || !viewOriginalText.startsWith(VIEW_PREFIX)
        || !viewOriginalText.endsWith(VIEW_SUFFIX)) {
      throw new IllegalArgumentException(
          "Not a valid Trino native view: viewOriginalText is missing the Presto View prefix/suffix");
    }
    String encoded =
        viewOriginalText.substring(
            VIEW_PREFIX.length(), viewOriginalText.length() - VIEW_SUFFIX.length());
    byte[] bytes = Base64.getDecoder().decode(encoded);

    JsonNode root;
    try {
      root = MAPPER.readTree(bytes);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to decode Trino native view definition", e);
    }

    JsonNode columnsNode = root.path("columns");
    if (!columnsNode.isArray() || columnsNode.isEmpty()) {
      throw new IllegalArgumentException(
          "Not a valid Trino native view: 'columns' field is missing, not an array, or empty");
    }
    List<ViewColumn> columns = new ArrayList<>();
    for (JsonNode columnNode : columnsNode) {
      String name = textOrNull(columnNode, "name");
      String type = textOrNull(columnNode, "type");
      if (name == null || type == null) {
        throw new IllegalArgumentException(
            "Not a valid Trino native view: a column is missing 'name' or 'type'");
      }
      columns.add(new ViewColumn(name, type, textOrNull(columnNode, "comment")));
    }

    String originalSql = textOrNull(root, "originalSql");
    if (originalSql == null) {
      throw new IllegalArgumentException(
          "Not a valid Trino native view: 'originalSql' field is missing or null");
    }

    List<String> path = new ArrayList<>();
    for (JsonNode pathNode : root.path("path")) {
      path.add(pathNode.asText());
    }

    return new ViewDefinition(
        originalSql,
        textOrNull(root, "catalog"),
        textOrNull(root, "schema"),
        columns,
        textOrNull(root, "comment"),
        textOrNull(root, "owner"),
        root.path("runAsInvoker").asBoolean(true),
        path);
  }

  @Nullable
  private static String textOrNull(JsonNode node, String field) {
    JsonNode value = node.get(field);
    return value == null || value.isNull() ? null : value.asText();
  }

  /**
   * Converts a Gravitino type to its Trino type signature string (e.g. {@code varchar(10)}, {@code
   * row(a integer,b varchar)}), for use in the encoded view's column list.
   *
   * @param type the Gravitino type
   * @return the Trino type signature string
   */
  static String toTrinoTypeString(Type type) {
    switch (type.name()) {
      case BOOLEAN:
        return "boolean";
      case BYTE:
        // Trino has no unsigned integer types; widen to the next size up to preserve range,
        // matching GeneralDataTypeTransformer's Gravitino-to-Trino type mapping.
        return ((Types.ByteType) type).signed() ? "tinyint" : "smallint";
      case SHORT:
        return ((Types.ShortType) type).signed() ? "smallint" : "integer";
      case INTEGER:
        return ((Types.IntegerType) type).signed() ? "integer" : "bigint";
      case LONG:
        return ((Types.LongType) type).signed() ? "bigint" : "decimal(20,0)";
      case FLOAT:
        return "real";
      case DOUBLE:
        return "double";
      case STRING:
        return "varchar";
      case VARCHAR:
        return "varchar(" + ((Types.VarCharType) type).length() + ")";
      case FIXEDCHAR:
        return "char(" + ((Types.FixedCharType) type).length() + ")";
      case DATE:
        return "date";
      case TIME:
        Types.TimeType timeType = (Types.TimeType) type;
        int timePrecision = timeType.hasPrecisionSet() ? timeType.precision() : DEFAULT_PRECISION;
        return "time(" + timePrecision + ")";
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) type;
        int timestampPrecision =
            timestampType.hasPrecisionSet() ? timestampType.precision() : DEFAULT_PRECISION;
        return timestampType.hasTimeZone()
            ? "timestamp(" + timestampPrecision + ") with time zone"
            : "timestamp(" + timestampPrecision + ")";
      case UUID:
        return "uuid";
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) type;
        return "decimal(" + decimalType.precision() + "," + decimalType.scale() + ")";
      case BINARY:
        return "varbinary";
      case LIST:
        return "array(" + toTrinoTypeString(((Types.ListType) type).elementType()) + ")";
      case MAP:
        Types.MapType mapType = (Types.MapType) type;
        return "map("
            + toTrinoTypeString(mapType.keyType())
            + ","
            + toTrinoTypeString(mapType.valueType())
            + ")";
      case STRUCT:
        Types.StructType structType = (Types.StructType) type;
        StringBuilder row = new StringBuilder("row(");
        Types.StructType.Field[] fields = structType.fields();
        for (int i = 0; i < fields.length; i++) {
          if (i > 0) {
            row.append(',');
          }
          row.append(rowFieldName(fields[i].name()))
              .append(' ')
              .append(toTrinoTypeString(fields[i].type()));
        }
        return row.append(')').toString();
      default:
        throw new UnsupportedOperationException("Unsupported conversion to Trino type: " + type);
    }
  }

  /**
   * Quotes a row field name, matching Trino's own {@code NamedTypeSignature}, which always quotes
   * named row fields unconditionally; a plain-looking name can still be a reserved keyword (e.g.
   * {@code select}), which Trino cannot parse unquoted.
   */
  private static String rowFieldName(String name) {
    return "\"" + name.replace("\"", "\"\"") + "\"";
  }

  /**
   * Converts a Trino type signature string (as produced by {@link #toTrinoTypeString}) back into a
   * Gravitino type. Used to restore a Trino dialect view's real column list from its encoded
   * payload, since the underlying HMS table only carries a single dummy column (matching Trino's
   * own native behavior; see {@code io.trino.plugin.hive.HiveMetadata#createView}).
   *
   * @param typeString the Trino type signature string
   * @return the Gravitino type
   */
  static Type fromTrinoTypeString(String typeString) {
    String s = typeString.trim();
    String lower = s.toLowerCase(Locale.ROOT);
    switch (lower) {
      case "boolean":
        return Types.BooleanType.get();
      case "tinyint":
        return Types.ByteType.get();
      case "smallint":
        return Types.ShortType.get();
      case "integer":
        return Types.IntegerType.get();
      case "bigint":
        return Types.LongType.get();
      case "real":
        return Types.FloatType.get();
      case "double":
        return Types.DoubleType.get();
      case "varchar":
        return Types.StringType.get();
      case "date":
        return Types.DateType.get();
      case "varbinary":
        return Types.BinaryType.get();
      case "uuid":
        return Types.UUIDType.get();
      default:
        break;
    }
    if (lower.startsWith("varchar(")) {
      return Types.VarCharType.of(Integer.parseInt(innerContent(s)));
    }
    if (lower.startsWith("char(")) {
      return Types.FixedCharType.of(Integer.parseInt(innerContent(s)));
    }
    if (lower.startsWith("time(")) {
      if (lower.endsWith("with time zone")) {
        throw new UnsupportedOperationException(
            "Unsupported Trino type: TIME WITH TIME ZONE has no Gravitino equivalent: "
                + typeString);
      }
      return Types.TimeType.of(parsePrecision(s));
    }
    if (lower.startsWith("timestamp(")) {
      return lower.endsWith("with time zone")
          ? Types.TimestampType.withTimeZone(parsePrecision(s))
          : Types.TimestampType.withoutTimeZone(parsePrecision(s));
    }
    if (lower.startsWith("decimal(")) {
      String[] parts = splitTopLevel(innerContent(s));
      return Types.DecimalType.of(
          Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim()));
    }
    if (lower.startsWith("array(")) {
      return Types.ListType.nullable(fromTrinoTypeString(innerContent(s)));
    }
    if (lower.startsWith("map(")) {
      String[] parts = splitTopLevel(innerContent(s));
      return Types.MapType.valueNullable(
          fromTrinoTypeString(parts[0]), fromTrinoTypeString(parts[1]));
    }
    if (lower.startsWith("row(")) {
      String[] parts = splitTopLevel(innerContent(s));
      Types.StructType.Field[] fields = new Types.StructType.Field[parts.length];
      for (int i = 0; i < parts.length; i++) {
        fields[i] = parseRowField(parts[i].trim(), typeString);
      }
      return Types.StructType.of(fields);
    }
    throw new UnsupportedOperationException("Unsupported Trino type: " + typeString);
  }

  private static String innerContent(String s) {
    return s.substring(s.indexOf('(') + 1, s.length() - 1);
  }

  /**
   * Extracts the integer precision from a type string of the form {@code name(N)[ trailing text]},
   * e.g. {@code timestamp(3) with time zone}; unlike {@link #innerContent}, this does not assume
   * the string ends with the closing parenthesis.
   */
  private static int parsePrecision(String s) {
    int openIdx = s.indexOf('(');
    int closeIdx = s.indexOf(')', openIdx);
    return Integer.parseInt(s.substring(openIdx + 1, closeIdx).trim());
  }

  /**
   * Parses a single {@code row(...)} field of the form {@code name type} or {@code "quoted name"
   * type} into a Gravitino struct field. Gravitino's struct type has no concept of an anonymous
   * field, so a Trino row field without a name (e.g. from {@code ROW(1, 'a')}) is rejected rather
   * than guessed at.
   */
  private static Types.StructType.Field parseRowField(String field, String typeString) {
    String fieldName;
    String fieldType;
    if (field.startsWith("\"")) {
      int closeQuote = field.indexOf('"', 1);
      while (closeQuote != -1
          && closeQuote + 1 < field.length()
          && field.charAt(closeQuote + 1) == '"') {
        closeQuote = field.indexOf('"', closeQuote + 2);
      }
      if (closeQuote == -1) {
        throw new UnsupportedOperationException(
            "Unsupported Trino type: malformed quoted row field name: " + typeString);
      }
      fieldName = field.substring(1, closeQuote).replace("\"\"", "\"");
      fieldType = field.substring(closeQuote + 1).trim();
    } else {
      int spaceIdx = field.indexOf(' ');
      if (spaceIdx <= 0) {
        throw new UnsupportedOperationException(
            "Unsupported Trino type: anonymous row fields are not supported: " + typeString);
      }
      fieldName = field.substring(0, spaceIdx);
      fieldType = field.substring(spaceIdx + 1);
    }
    return Types.StructType.Field.nullableField(fieldName, fromTrinoTypeString(fieldType));
  }

  /**
   * Splits a comma-separated argument list, ignoring commas nested inside parentheses or inside a
   * double-quoted row field name (which may itself contain commas, e.g. {@code row("a,b" integer)}
   * ; a doubled {@code ""} inside the quotes is an escaped literal quote, not a terminator).
   */
  private static String[] splitTopLevel(String s) {
    List<String> parts = new ArrayList<>();
    int depth = 0;
    boolean inQuotes = false;
    int start = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '"') {
        if (inQuotes && i + 1 < s.length() && s.charAt(i + 1) == '"') {
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (inQuotes) {
        // Ignore parentheses/commas inside a quoted field name.
      } else if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (c == ',' && depth == 0) {
        parts.add(s.substring(start, i));
        start = i + 1;
      }
    }
    parts.add(s.substring(start));
    return parts.toArray(new String[0]);
  }
}
