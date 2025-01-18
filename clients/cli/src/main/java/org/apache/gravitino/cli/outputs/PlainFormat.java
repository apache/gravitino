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
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Schema;
import org.apache.gravitino.rel.Table;

/**
 * Formats entity into plain text representation for command-line output. Supports formatting of
 * single objects and arrays of Metalake, Catalog, Schema, and Table objects. Each supported type
 * has its own specialized formatter as an inner class.
 */
public abstract class PlainFormat<T> extends BaseOutputFormat<T> {

  /**
   * Routes the object to its appropriate formatter and outputs the formatted result. Creates a new
   * formatter instance for the given object type and delegates the formatting.
   *
   * @param object The object to format
   * @param property Configuration properties for output formatting
   * @throws IllegalArgumentException if the object type is not supported
   */
  public static void output(Object object, OutputProperty property) {
    if (object instanceof Metalake) {
      new MetalakePlainFormat(property).output((Metalake) object);
    } else if (object instanceof Metalake[]) {
      new MetalakesPlainFormat(property).output((Metalake[]) object);
    } else if (object instanceof Catalog) {
      new CatalogPlainFormat(property).output((Catalog) object);
    } else if (object instanceof Catalog[]) {
      new CatalogsPlainFormat(property).output((Catalog[]) object);
    } else if (object instanceof Schema) {
      new SchemaPlainFormat(property).output((Schema) object);
    } else if (object instanceof Schema[]) {
      new SchemasPlainFormat(property).output((Schema[]) object);
    } else if (object instanceof Table) {
      new TablePlainFormat(property).output((Table) object);
    } else if (object instanceof Table[]) {
      new TablesPlainFormat(property).output((Table[]) object);
    } else {
      throw new IllegalArgumentException("Unsupported object type");
    }
  }

  /**
   * Creates a new {@link PlainFormat} with the specified output properties.
   *
   * @param property Configuration for controlling output behavior
   */
  public PlainFormat(OutputProperty property) {
    super(property.isQuiet(), property.getLimit(), property.isSort());
  }

  /**
   * Formats a single {@link Metalake} instance as a comma-separated string. Output format: name,
   * comment
   */
  static final class MetalakePlainFormat extends PlainFormat<Metalake> {

    public MetalakePlainFormat(OutputProperty property) {
      super(property);
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
  static final class MetalakesPlainFormat extends PlainFormat<Metalake[]> {

    public MetalakesPlainFormat(OutputProperty property) {
      super(property);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Metalake[] metalakes) {
      if (metalakes == null || metalakes.length == 0) {
        return null;
      } else {
        List<String> metalakeNames =
            Arrays.stream(metalakes).map(Metalake::name).collect(Collectors.toList());
        return NEWLINE_JOINER.join(metalakeNames);
      }
    }
  }

  /**
   * Formats a single {@link Catalog} instance as a comma-separated string. Output format: name,
   * type, provider, comment
   */
  static final class CatalogPlainFormat extends PlainFormat<Catalog> {
    public CatalogPlainFormat(OutputProperty property) {
      super(property);
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
  static final class CatalogsPlainFormat extends PlainFormat<Catalog[]> {
    public CatalogsPlainFormat(OutputProperty property) {
      super(property);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Catalog[] catalogs) {
      if (catalogs == null || catalogs.length == 0) {
        output("No catalogs exists.", System.err);
        return null;
      } else {
        List<String> catalogNames =
            Arrays.stream(catalogs).map(Catalog::name).collect(Collectors.toList());
        return NEWLINE_JOINER.join(catalogNames);
      }
    }
  }

  /**
   * Formats a single {@link Schema} instance as a comma-separated string. Output format: name,
   * comment
   */
  static final class SchemaPlainFormat extends PlainFormat<Schema> {
    public SchemaPlainFormat(OutputProperty property) {
      super(property);
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
  static final class SchemasPlainFormat extends PlainFormat<Schema[]> {
    public SchemasPlainFormat(OutputProperty property) {
      super(property);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Schema[] schemas) {
      if (schemas == null || schemas.length == 0) {
        return null;
      } else {
        List<String> schemaNames =
            Arrays.stream(schemas).map(Schema::name).collect(Collectors.toList());
        return NEWLINE_JOINER.join(schemaNames);
      }
    }
  }

  /**
   * Formats a single Table instance with detailed column information. Output format: table_name
   * column1_name, column1_type, column1_comment column2_name, column2_type, column2_comment ...
   * table_comment
   */
  static final class TablePlainFormat extends PlainFormat<Table> {
    public TablePlainFormat(OutputProperty property) {
      super(property);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Table table) {
      StringBuilder output = new StringBuilder(table.name() + System.lineSeparator());
      List<String> columnOutput =
          Arrays.stream(table.columns())
              .map(
                  column ->
                      COMMA_JOINER.join(
                          column.name(), column.dataType().simpleString(), column.comment()))
              .collect(Collectors.toList());
      output.append(NEWLINE_JOINER.join(columnOutput));
      output.append(System.lineSeparator());
      output.append(table.comment());
      return output.toString();
    }
  }

  /**
   * Formats an array of Tables, outputting one name per line. Returns null if the array is empty or
   * null.
   */
  static final class TablesPlainFormat extends PlainFormat<Table[]> {
    public TablesPlainFormat(OutputProperty property) {
      super(property);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Table[] tables) {
      if (tables == null || tables.length == 0) {
        return null;
      } else {
        List<String> tableNames =
            Arrays.stream(tables).map(Table::name).collect(Collectors.toList());
        return NEWLINE_JOINER.join(tableNames);
      }
    }
  }
}
