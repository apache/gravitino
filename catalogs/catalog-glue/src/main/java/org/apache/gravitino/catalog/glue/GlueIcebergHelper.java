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

import static org.apache.gravitino.catalog.glue.GlueConstants.CURRENT_SCHEMA_ID_PARAM;
import static org.apache.gravitino.catalog.glue.GlueConstants.ICEBERG_FIELD_ID;
import static org.apache.gravitino.catalog.glue.GlueConstants.ICEBERG_FIELD_OPTIONAL;
import static org.apache.gravitino.catalog.glue.GlueConstants.TABLE_TYPE_PARAM;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.IcebergSchema;
import software.amazon.awssdk.services.glue.model.IcebergStructField;
import software.amazon.awssdk.services.glue.model.IcebergStructTypeEnum;
import software.amazon.awssdk.services.glue.model.IcebergTableUpdate;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

/**
 * Utility methods for building Iceberg-specific Glue API structures used when creating and
 * modifying Iceberg-format tables via the AWS Glue Data Catalog.
 */
final class GlueIcebergHelper {

  private static final Logger LOG = LoggerFactory.getLogger(GlueIcebergHelper.class);

  private GlueIcebergHelper() {}

  /**
   * Returns true if the Glue table is an Iceberg-format table.
   *
   * <p>Checks for {@code table_type=ICEBERG} in {@code Table.parameters()}.
   */
  static boolean isIcebergTable(Table glueTable) {
    if (!glueTable.hasParameters()) return false;
    return GlueConstants.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(
        glueTable.parameters().get(TABLE_TYPE_PARAM));
  }

  /**
   * Builds a list of {@link IcebergTableUpdate} objects from Gravitino {@link TableChange} entries.
   *
   * <p>Schema changes (add/rename/delete/update column) are consolidated into a single {@code
   * add-schema} update carrying the complete new schema. Property changes are emitted as a separate
   * {@code set-properties} update.
   *
   * @param rawGlueTable the current Glue table (used to read existing column field IDs)
   * @param changes the changes to apply
   * @return list of {@link IcebergTableUpdate} to apply; empty if {@code changes} contains no
   *     applicable entries, otherwise one entry for schema changes and/or one for property changes
   */
  static List<IcebergTableUpdate> buildIcebergTableUpdates(
      Table rawGlueTable, TableChange... changes) {

    List<TableChange> schemaChanges = new ArrayList<>();
    Map<String, String> setProperties = new HashMap<>();

    for (TableChange change : changes) {
      if (change instanceof TableChange.ColumnChange) {
        schemaChanges.add(change);
      } else if (change instanceof TableChange.SetProperty) {
        TableChange.SetProperty sp = (TableChange.SetProperty) change;
        setProperties.put(sp.getProperty(), sp.getValue());
      } else if (change instanceof TableChange.RemoveProperty) {
        throw new UnsupportedOperationException(
            "Removing properties from Iceberg tables is not supported via the Glue SDK. "
                + "Use an Iceberg-native client (Spark/Athena) to modify table properties.");
      } else {
        throw new IllegalArgumentException(
            "Unsupported table change for Iceberg table: " + change.getClass().getSimpleName());
      }
    }

    List<IcebergTableUpdate> updates = new ArrayList<>();

    if (!schemaChanges.isEmpty()) {
      IcebergSchema newSchema = buildUpdatedSchema(rawGlueTable, schemaChanges);
      updates.add(IcebergTableUpdate.builder().schema(newSchema).build());
    }

    if (!setProperties.isEmpty()) {
      updates.add(IcebergTableUpdate.builder().properties(setProperties).build());
    }

    return updates;
  }

  /**
   * Reads the current Iceberg schema from a Glue table's StorageDescriptor columns (preserving
   * {@code iceberg.field.id} from column parameters), applies the given column changes, and returns
   * the resulting {@link IcebergSchema} with an incremented schema ID.
   */
  static IcebergSchema buildUpdatedSchema(Table rawGlueTable, List<TableChange> schemaChanges) {
    List<IcebergStructField> fields = currentFields(rawGlueTable);
    int maxId = fields.stream().mapToInt(IcebergStructField::id).max().orElse(0);

    for (TableChange change : schemaChanges) {
      if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn add = (TableChange.AddColumn) change;
        Preconditions.checkArgument(
            add.fieldName().length == 1, "Nested column additions are not supported");
        fields.add(
            IcebergStructField.builder()
                .id(++maxId)
                .name(add.fieldName()[0])
                .type(gravitinoTypeToDoc(add.getDataType()))
                .required(!add.isNullable())
                .doc(add.getComment())
                .build());

      } else if (change instanceof TableChange.DeleteColumn) {
        String name = ((TableChange.DeleteColumn) change).fieldName()[0];
        fields.removeIf(f -> f.name().equals(name));

      } else if (change instanceof TableChange.RenameColumn) {
        TableChange.RenameColumn rename = (TableChange.RenameColumn) change;
        updateField(fields, rename.fieldName()[0], b -> b.name(rename.getNewName()));

      } else if (change instanceof TableChange.UpdateColumnType) {
        TableChange.UpdateColumnType upd = (TableChange.UpdateColumnType) change;
        Document newTypeDoc = gravitinoTypeToDoc(upd.getNewDataType());
        updateField(fields, upd.fieldName()[0], b -> b.type(newTypeDoc));

      } else if (change instanceof TableChange.UpdateColumnComment) {
        TableChange.UpdateColumnComment upd = (TableChange.UpdateColumnComment) change;
        updateField(fields, upd.fieldName()[0], b -> b.doc(upd.getNewComment()));

      } else if (change instanceof TableChange.UpdateColumnNullability) {
        TableChange.UpdateColumnNullability upd = (TableChange.UpdateColumnNullability) change;
        updateField(fields, upd.fieldName()[0], b -> b.required(!upd.nullable()));

      } else {
        throw new IllegalArgumentException(
            "Unsupported column change: " + change.getClass().getSimpleName());
      }
    }

    int currentSchemaId = parseSchemaId(rawGlueTable);
    return IcebergSchema.builder()
        .schemaId(currentSchemaId + 1)
        .type(IcebergStructTypeEnum.STRUCT)
        .fields(fields)
        .build();
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /**
   * Finds the first field with the given {@code name} in {@code fields} and replaces it in-place
   * with the result of applying {@code updater} to its builder. Throws if the field is not found.
   */
  private static void updateField(
      List<IcebergStructField> fields,
      String name,
      UnaryOperator<IcebergStructField.Builder> updater) {
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).name().equals(name)) {
        fields.set(i, updater.apply(fields.get(i).toBuilder()).build());
        return;
      }
    }
    throw new IllegalArgumentException("Column '" + name + "' not found in Iceberg table schema");
  }

  /** Reads current Iceberg fields from the Glue table's StorageDescriptor columns. */
  private static List<IcebergStructField> currentFields(Table rawGlueTable) {
    List<IcebergStructField> fields = new ArrayList<>();
    StorageDescriptor sd = rawGlueTable.storageDescriptor();
    if (sd == null || !sd.hasColumns()) return fields;

    int fallbackId = 0;
    for (Column col : sd.columns()) {
      int fieldId = parseFieldId(col, ++fallbackId);
      boolean required = parseRequired(col);
      fields.add(
          IcebergStructField.builder()
              .id(fieldId)
              .name(col.name())
              .type(hiveTypeToDoc(col.type()))
              .required(required)
              .doc(col.comment())
              .build());
    }
    return fields;
  }

  /** Parses {@code iceberg.field.id} from column parameters; falls back to {@code fallbackId}. */
  private static int parseFieldId(Column col, int fallbackId) {
    if (col.hasParameters()) {
      String raw = col.parameters().get(ICEBERG_FIELD_ID);
      if (raw != null) {
        try {
          return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
          LOG.warn(
              "Column '{}' has non-numeric iceberg.field.id '{}'; falling back to sequence ID {}",
              col.name(),
              raw,
              fallbackId);
          return fallbackId;
        }
      }
    }
    return fallbackId;
  }

  /** Parses {@code iceberg.field.optional}; defaults to {@code false} (i.e., nullable). */
  private static boolean parseRequired(Column col) {
    if (col.hasParameters()) {
      String optional = col.parameters().get(ICEBERG_FIELD_OPTIONAL);
      if (optional != null) {
        return !Boolean.parseBoolean(optional);
      }
    }
    return false;
  }

  /**
   * Reads the current schema ID from {@code Table.parameters()["current-schema-id"]}; returns 0 if
   * absent.
   */
  private static int parseSchemaId(Table rawGlueTable) {
    if (!rawGlueTable.hasParameters()) return 0;
    String raw = rawGlueTable.parameters().get(CURRENT_SCHEMA_ID_PARAM);
    if (raw == null) return 0;
    try {
      return Integer.parseInt(raw);
    } catch (NumberFormatException e) {
      LOG.warn("Table has non-numeric current-schema-id '{}'; treating as 0", raw);
      return 0;
    }
  }

  /**
   * Converts a Glue/Hive column type string to an Iceberg type {@link Document}.
   *
   * <p>Iceberg REST spec type names are used for primitives (e.g. {@code "long"} for Hive {@code
   * bigint}). Complex types (array, map, struct) are represented as JSON maps.
   */
  static Document hiveTypeToDoc(String hiveType) {
    if (hiveType == null) return Document.fromString("string");
    String t = hiveType.toLowerCase(Locale.ROOT).trim();

    switch (t) {
      case "string":
        return Document.fromString("string");
      case "bigint":
        return Document.fromString("long");
      case "int":
      case "integer":
      case "smallint":
      case "tinyint":
        return Document.fromString("int");
      case "float":
        return Document.fromString("float");
      case "double":
      case "double precision":
        return Document.fromString("double");
      case "boolean":
        return Document.fromString("boolean");
      case "binary":
        return Document.fromString("binary");
      case "date":
        return Document.fromString("date");
      case "timestamp":
        return Document.fromString("timestamp");
      default:
        break;
    }

    if (t.startsWith("decimal(")) {
      try {
        String inner =
            t.substring("decimal(".length(), t.endsWith(")") ? t.length() - 1 : t.length());
        String[] parts = inner.split(",");
        int precision = Integer.parseInt(parts[0].trim());
        int scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
        return Document.fromMap(
            Map.of(
                "type", Document.fromString("decimal"),
                "precision", Document.fromNumber(precision),
                "scale", Document.fromNumber(scale)));
      } catch (NumberFormatException e) {
        LOG.warn("Malformed Hive decimal type '{}', falling back to Iceberg 'string'", hiveType);
        return Document.fromString("string");
      }
    }
    if (t.startsWith("varchar(") || t.startsWith("char(")) {
      return Document.fromString("string");
    }
    LOG.warn("Unknown Hive type '{}', falling back to Iceberg 'string'", hiveType);
    return Document.fromString("string");
  }

  /**
   * Converts a Gravitino {@link Type} to an Iceberg type {@link Document}.
   *
   * <p>Supports primitive types. Complex types (list, map, struct) require element/key/value field
   * IDs that can only be assigned at schema-build time; pass-through as nested Document maps.
   */
  static Document gravitinoTypeToDoc(Type type) {
    if (type instanceof Types.StringType
        || type instanceof Types.VarCharType
        || type instanceof Types.FixedCharType) {
      return Document.fromString("string");
    }
    if (type instanceof Types.LongType) return Document.fromString("long");
    if (type instanceof Types.IntegerType
        || type instanceof Types.ShortType
        || type instanceof Types.ByteType) {
      return Document.fromString("int");
    }
    if (type instanceof Types.FloatType) return Document.fromString("float");
    if (type instanceof Types.DoubleType) return Document.fromString("double");
    if (type instanceof Types.BooleanType) return Document.fromString("boolean");
    if (type instanceof Types.BinaryType || type instanceof Types.FixedType) {
      return Document.fromString("binary");
    }
    if (type instanceof Types.DateType) return Document.fromString("date");
    if (type instanceof Types.TimeType) return Document.fromString("time");
    if (type instanceof Types.TimestampType) {
      return Document.fromString(
          ((Types.TimestampType) type).hasTimeZone() ? "timestamptz" : "timestamp");
    }
    if (type instanceof Types.UUIDType) return Document.fromString("uuid");
    if (type instanceof Types.DecimalType) {
      Types.DecimalType dt = (Types.DecimalType) type;
      return Document.fromMap(
          Map.of(
              "type", Document.fromString("decimal"),
              "precision", Document.fromNumber(dt.precision()),
              "scale", Document.fromNumber(dt.scale())));
    }
    throw new UnsupportedOperationException(
        "Iceberg Glue catalog does not support type: " + type.simpleString());
  }
}
