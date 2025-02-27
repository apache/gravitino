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
package org.apache.gravitino.cli.outputs;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Schema;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;

/** Plain format to print a pretty string to standard out. */
public abstract class PlainFormat<T> extends BaseOutputFormat<T> {

  /**
   * Routes the object to its appropriate formatter and outputs the formatted result. Creates a new
   * formatter instance for the given object type and delegates the formatting.
   *
   * @param entity The object to format
   * @param context The command context
   * @throws IllegalArgumentException if the object type is not supported
   */
  public static void output(Object entity, CommandContext context) {
    if (entity instanceof Metalake) {
      new MetalakePlainFormat(context).output((Metalake) entity);
    } else if (entity instanceof Metalake[]) {
      new MetalakeListPlainFormat(context).output((Metalake[]) entity);
    } else if (entity instanceof Catalog) {
      new CatalogPlainFormat(context).output((Catalog) entity);
    } else if (entity instanceof Catalog[]) {
      new CatalogListPlainFormat(context).output((Catalog[]) entity);
    } else if (entity instanceof Schema) {
      new SchemaPlainFormat(context).output((Schema) entity);
    } else if (entity instanceof Schema[]) {
      new SchemaListPlainFormat(context).output((Schema[]) entity);
    } else if (entity instanceof Table) {
      new TablePlainFormat(context).output((Table) entity);
    } else if (entity instanceof Table[]) {
      new TableListPlainFormat(context).output((Table[]) entity);
    } else if (entity instanceof Audit) {
      new AuditPlainFormat(context).output((Audit) entity);
    } else if (entity instanceof Column[]) {
      new ColumnListPlainFormat(context).output((Column[]) entity);
    } else {
      throw new IllegalArgumentException("Unsupported object type");
    }
  }

  /**
   * Creates a new {@link PlainFormat} with the specified output properties.
   *
   * @param context The command context.
   */
  public PlainFormat(CommandContext context) {
    super(context);
  }

  /**
   * Formats a single {@link Metalake} instance as a comma-separated string. Output format: name,
   * comment
   */
  static final class MetalakePlainFormat extends PlainFormat<Metalake> {

    public MetalakePlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Metalake metalake) {
      return COMMA_JOINER.join(metalake.name(), metalake.comment());
    }
  }

  /**
   * Formats an array of Metalakes, outputting one name per line. Returns null if the array is empty
   * or null.
   */
  static final class MetalakeListPlainFormat extends PlainFormat<Metalake[]> {

    public MetalakeListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Metalake[] metalakes) {
      List<String> metalakeNames =
          Arrays.stream(metalakes).map(Metalake::name).collect(Collectors.toList());
      return NEWLINE_JOINER.join(metalakeNames);
    }
  }

  /**
   * Formats a single {@link Catalog} instance as a comma-separated string. Output format: name,
   * type, provider, comment
   */
  static final class CatalogPlainFormat extends PlainFormat<Catalog> {
    public CatalogPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Catalog catalog) {
      return COMMA_JOINER.join(
          catalog.name(), catalog.type(), catalog.provider(), catalog.comment());
    }
  }

  /**
   * Formats an array of Catalogs, outputting one name per line. Returns null if the array is empty
   * or null.
   */
  static final class CatalogListPlainFormat extends PlainFormat<Catalog[]> {
    public CatalogListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Catalog[] catalogs) {

      List<String> catalogNames =
          Arrays.stream(catalogs).map(Catalog::name).collect(Collectors.toList());
      return NEWLINE_JOINER.join(catalogNames);
    }
  }

  /**
   * Formats a single {@link Schema} instance as a comma-separated string. Output format: name,
   * comment
   */
  static final class SchemaPlainFormat extends PlainFormat<Schema> {
    public SchemaPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Schema schema) {
      return COMMA_JOINER.join(schema.name(), schema.comment());
    }
  }

  /**
   * Formats an array of Schemas, outputting one name per line. Returns null if the array is empty
   * or null.
   */
  static final class SchemaListPlainFormat extends PlainFormat<Schema[]> {
    public SchemaListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Schema[] schemas) {
      List<String> schemaNames =
          Arrays.stream(schemas).map(Schema::name).collect(Collectors.toList());
      return NEWLINE_JOINER.join(schemaNames);
    }
  }

  /**
   * Formats a single Table instance with detailed column information. Output format: table_name,
   * table_comment
   */
  static final class TablePlainFormat extends PlainFormat<Table> {
    public TablePlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Table table) {
      String comment = table.comment() == null ? "N/A" : table.comment();
      return COMMA_JOINER.join(new String[] {table.name(), comment});
    }
  }

  /**
   * Formats an array of Tables, outputting one name per line. Returns null if the array is empty or
   * null.
   */
  static final class TableListPlainFormat extends PlainFormat<Table[]> {
    public TableListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Table[] tables) {
      List<String> tableNames = Arrays.stream(tables).map(Table::name).collect(Collectors.toList());
      return NEWLINE_JOINER.join(tableNames);
    }
  }

  /**
   * Formats an instance of {@link Audit} , outputting the audit information. Output format:
   * creator, create_time, modified, modified_time
   */
  static final class AuditPlainFormat extends PlainFormat<Audit> {
    public AuditPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Audit audit) {
      return COMMA_JOINER.join(
          audit.creator(),
          audit.createTime() == null ? "N/A" : audit.createTime(),
          audit.lastModifier() == null ? "N/A" : audit.lastModifier(),
          audit.lastModifiedTime() == null ? "N/A" : audit.lastModifiedTime());
    }
  }

  /**
   * Formats an array of {@link org.apache.gravitino.rel.Column} into a six-column table display.
   * Lists all column names, types, default values, auto-increment, nullable, and comments in a
   * plain format.
   */
  static final class ColumnListPlainFormat extends PlainFormat<Column[]> {

    /**
     * Creates a new {@link ColumnListPlainFormat} with the specified output properties.
     *
     * @param context The command context.
     */
    public ColumnListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Column[] columns) {
      String header =
          COMMA_JOINER.join(
              "name", "datatype", "default_value", "comment", "nullable", "auto_increment");
      StringBuilder data = new StringBuilder();
      for (int i = 0; i < columns.length; i++) {
        String name = columns[i].name();
        String dataType = columns[i].dataType().simpleString();
        String defaultValue = LineUtil.getDefaultValue(columns[i]);
        String comment = LineUtil.getComment(columns[i]);
        String nullable = columns[i].nullable() ? "true" : "false";
        String autoIncrement = LineUtil.getAutoIncrement(columns[i]);

        data.append(
            COMMA_JOINER.join(name, dataType, defaultValue, comment, nullable, autoIncrement));
        data.append(System.lineSeparator());
      }
      return NEWLINE_JOINER.join(header, data.toString());
    }
  }
}
