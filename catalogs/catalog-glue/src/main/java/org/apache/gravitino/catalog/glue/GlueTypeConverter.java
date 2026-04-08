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
package org.apache.gravitino.catalog.glue;

import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/**
 * Converts between AWS Glue/Hive type strings and Gravitino {@link Type} objects.
 *
 * <p>Glue uses Hive-style DDL type strings (e.g. {@code "int"}, {@code "array<string>"},
 * {@code "struct<id:int,name:string>"}). This class handles the full set of primitive and complex
 * types in both directions.
 */
public class GlueTypeConverter {

    private GlueTypeConverter() {}

    // -------------------------------------------------------------------------
    // Glue → Gravitino
    // -------------------------------------------------------------------------

    /**
     * Converts a Glue/Hive type string to a Gravitino {@link Type}.
     *
     * @param glueType the Glue type string, e.g. {@code "bigint"}, {@code "array<string>"}
     * @return the corresponding Gravitino type
     * @throws IllegalArgumentException if the type string is unrecognized
     */
    public static Type fromGlue(String glueType) {
        if (glueType == null || glueType.isBlank()) {
            throw new IllegalArgumentException("Glue type string must not be null or blank");
        }
        return parseType(glueType.trim().toLowerCase());
    }

    private static Type parseType(String raw) {
        // --- Primitives ---
        switch (raw) {
            case "string":
                return Types.StringType.get();
            case "boolean":
                return Types.BooleanType.get();
            case "tinyint":
                return Types.ByteType.get();
            case "smallint":
                return Types.ShortType.get();
            case "int":
            case "integer":
                return Types.IntegerType.get();
            case "bigint":
                return Types.LongType.get();
            case "float":
                return Types.FloatType.get();
            case "double":
            case "double precision":
                return Types.DoubleType.get();
            case "binary":
                return Types.BinaryType.get();
            case "date":
                return Types.DateType.get();
            case "timestamp":
                return Types.TimestampType.withoutTimeZone();
            case "timestamptz":
            case "timestamp with time zone":
                return Types.TimestampType.withTimeZone();
            default:
                break;
        }

        // --- varchar(n) ---
        if (raw.startsWith("varchar(") && raw.endsWith(")")) {
            int length = parseOneIntArg("varchar", raw);
            return Types.VarCharType.of(length);
        }

        // --- char(n) ---
        if (raw.startsWith("char(") && raw.endsWith(")")) {
            int length = parseOneIntArg("char", raw);
            return Types.FixedCharType.of(length);
        }

        // --- decimal(p, s) or decimal(p) ---
        if (raw.startsWith("decimal(") && raw.endsWith(")")) {
            String inner = raw.substring("decimal(".length(), raw.length() - 1);
            String[] parts = inner.split(",");
            int precision = Integer.parseInt(parts[0].trim());
            int scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
            return Types.DecimalType.of(precision, scale);
        }

        // --- array<elementType> ---
        if (raw.startsWith("array<") && raw.endsWith(">")) {
            String inner = raw.substring("array<".length(), raw.length() - 1);
            Type elementType = parseType(inner);
            return Types.ListType.of(elementType, true);
        }

        // --- map<keyType, valueType> ---
        if (raw.startsWith("map<") && raw.endsWith(">")) {
            String inner = raw.substring("map<".length(), raw.length() - 1);
            int splitIdx = findTopLevelComma(inner);
            if (splitIdx < 0) {
                throw new IllegalArgumentException("Invalid Glue map type: " + raw);
            }
            Type keyType = parseType(inner.substring(0, splitIdx).trim());
            Type valueType = parseType(inner.substring(splitIdx + 1).trim());
            return Types.MapType.of(keyType, valueType, true);
        }

        // --- struct<name:type, name:type, ...> ---
        if (raw.startsWith("struct<") && raw.endsWith(">")) {
            String inner = raw.substring("struct<".length(), raw.length() - 1);
            return parseStructType(inner);
        }

        throw new IllegalArgumentException("Unsupported Glue type: " + raw);
    }

    /** Parses {@code struct<field1:type1,field2:type2,...>} inner content. */
    private static Types.StructType parseStructType(String inner) {
        // Split on top-level commas to get individual fields
        java.util.List<String> fieldTokens = splitTopLevel(inner);
        Types.StructType.Field[] fields = new Types.StructType.Field[fieldTokens.size()];

        for (int i = 0; i < fieldTokens.size(); i++) {
            String token = fieldTokens.get(i).trim();
            int colonIdx = token.indexOf(':');
            if (colonIdx < 0) {
                throw new IllegalArgumentException("Invalid struct field definition: " + token);
            }
            String fieldName = token.substring(0, colonIdx).trim();
            String fieldTypeStr = token.substring(colonIdx + 1).trim();
            Type fieldType = parseType(fieldTypeStr);
            fields[i] = Types.StructType.Field.nullableField(fieldName, fieldType);
        }
        return Types.StructType.of(fields);
    }

    /** Finds the index of the first top-level comma (not inside angle brackets). */
    private static int findTopLevelComma(String s) {
        int depth = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '<') depth++;
            else if (c == '>') depth--;
            else if (c == ',' && depth == 0) return i;
        }
        return -1;
    }

    /** Splits a string on top-level commas, respecting nested angle brackets. */
    private static java.util.List<String> splitTopLevel(String s) {
        java.util.List<String> result = new java.util.ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '<') depth++;
            else if (c == '>') depth--;
            else if (c == ',' && depth == 0) {
                result.add(s.substring(start, i));
                start = i + 1;
            }
        }
        result.add(s.substring(start));
        return result;
    }

    /** Parses the single integer argument from e.g. {@code "varchar(255)"}. */
    private static int parseOneIntArg(String typeName, String raw) {
        String inner = raw.substring(typeName.length() + 1, raw.length() - 1).trim();
        return Integer.parseInt(inner);
    }

    // -------------------------------------------------------------------------
    // Gravitino → Glue
    // -------------------------------------------------------------------------

    /**
     * Converts a Gravitino {@link Type} to a Glue/Hive type string.
     *
     * @param type the Gravitino type
     * @return the corresponding Glue type string
     * @throws IllegalArgumentException if the type has no Glue equivalent
     */
    public static String toGlue(Type type) {
        if (type instanceof Types.StringType) return "string";
        if (type instanceof Types.BooleanType) return "boolean";
        if (type instanceof Types.ByteType) return "tinyint";
        if (type instanceof Types.ShortType) return "smallint";
        if (type instanceof Types.IntegerType) return "int";
        if (type instanceof Types.LongType) return "bigint";
        if (type instanceof Types.FloatType) return "float";
        if (type instanceof Types.DoubleType) return "double";
        if (type instanceof Types.BinaryType) return "binary";
        if (type instanceof Types.DateType) return "date";
        if (type instanceof Types.TimestampType) {
            return ((Types.TimestampType) type).hasTimeZone() ? "timestamptz" : "timestamp";
        }
        if (type instanceof Types.VarCharType) {
            return "varchar(" + ((Types.VarCharType) type).length() + ")";
        }
        if (type instanceof Types.FixedCharType) {
            return "char(" + ((Types.FixedCharType) type).length() + ")";
        }
        if (type instanceof Types.DecimalType) {
            Types.DecimalType dt = (Types.DecimalType) type;
            return "decimal(" + dt.precision() + "," + dt.scale() + ")";
        }
        if (type instanceof Types.ListType) {
            Types.ListType lt = (Types.ListType) type;
            return "array<" + toGlue(lt.elementType()) + ">";
        }
        if (type instanceof Types.MapType) {
            Types.MapType mt = (Types.MapType) type;
            return "map<" + toGlue(mt.keyType()) + "," + toGlue(mt.valueType()) + ">";
        }
        if (type instanceof Types.StructType) {
            Types.StructType st = (Types.StructType) type;
            StringBuilder sb = new StringBuilder("struct<");
            Types.StructType.Field[] fields = st.fields();
            for (int i = 0; i < fields.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(fields[i].name()).append(":").append(toGlue(fields[i].type()));
            }
            sb.append(">");
            return sb.toString();
        }

        throw new IllegalArgumentException(
                "Unsupported Gravitino type for Glue conversion: " + type.getClass().getSimpleName());
    }
}
