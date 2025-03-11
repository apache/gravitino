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

import static org.apache.gravitino.cli.outputs.OutputConstant.DATA_LINE_COLUMN_SEPARATOR_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.DATA_LINE_LEFT_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.DATA_LINE_RIGHT_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.DATA_ROW_BORDER_COLUMN_SEPARATOR_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.DATA_ROW_BORDER_LEFT_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.DATA_ROW_BORDER_MIDDLE_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.DATA_ROW_BORDER_RIGHT_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.HEADER_BOTTOM_BORDER_COLUMN_SEPARATOR_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.HEADER_BOTTOM_BORDER_LEFT_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.HEADER_BOTTOM_BORDER_MIDDLE_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.HEADER_BOTTOM_BORDER_RIGHT_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.TABLE_BOTTOM_BORDER_COLUMN_SEPARATOR_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.TABLE_BOTTOM_BORDER_LEFT_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.TABLE_BOTTOM_BORDER_MIDDLE_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.TABLE_BOTTOM_BORDER_RIGHT_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.TABLE_UPPER_BORDER_COLUMN_SEPARATOR_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.TABLE_UPPER_BORDER_LEFT_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.TABLE_UPPER_BORDER_MIDDLE_IDX;
import static org.apache.gravitino.cli.outputs.OutputConstant.TABLE_UPPER_BORDER_RIGHT_IDX;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Schema;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.rel.Table;

/**
 * Abstract base class for formatting entity information into ASCII-art tables. Provides
 * comprehensive table rendering with features including: - Header and footer rows - Column
 * alignments and padding - Border styles and row separators - Content overflow handling - Row
 * numbers - Data limiting and sorting
 */
public abstract class TableFormat<T> extends BaseOutputFormat<T> {
  public static final int PADDING = 1;

  /**
   * Routes the entity object to its appropriate table formatter. Creates a new formatter instance
   * based on the object's type.
   *
   * @param entity The object to format.
   * @param context the command context.
   * @throws IllegalArgumentException if the object type is not supported
   */
  public static void output(Object entity, CommandContext context) {
    if (entity instanceof Metalake) {
      new MetalakeTableFormat(context).output((Metalake) entity);
    } else if (entity instanceof Metalake[]) {
      new MetalakeListTableFormat(context).output((Metalake[]) entity);
    } else if (entity instanceof Catalog) {
      new CatalogTableFormat(context).output((Catalog) entity);
    } else if (entity instanceof Catalog[]) {
      new CatalogListTableFormat(context).output((Catalog[]) entity);
    } else if (entity instanceof Schema) {
      new SchemaTableFormat(context).output((Schema) entity);
    } else if (entity instanceof Schema[]) {
      new SchemaListTableFormat(context).output((Schema[]) entity);
    } else if (entity instanceof Table) {
      new TableDetailsTableFormat(context).output((Table) entity);
    } else if (entity instanceof Table[]) {
      new TableListTableFormat(context).output((Table[]) entity);
    } else if (entity instanceof Audit) {
      new AuditTableFormat(context).output((Audit) entity);
    } else if (entity instanceof org.apache.gravitino.rel.Column[]) {
      new ColumnListTableFormat(context).output((org.apache.gravitino.rel.Column[]) entity);
    } else {
      throw new IllegalArgumentException("Unsupported object type");
    }
  }

  /**
   * Creates a new {@link TableFormat} with the specified properties.
   *
   * @param context the command context.
   */
  public TableFormat(CommandContext context) {
    super(context);
    // TODO: add other options for TableFormat
  }

  /**
   * Get the formatted output string for the given columns.
   *
   * @param columns the columns to print.
   * @return the table formatted output string.
   */
  public String getTableFormat(Column... columns) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    String[] headers =
        Arrays.stream(columns)
            .map(Column::getHeader)
            .filter(Objects::nonNull)
            .toArray(String[]::new);

    List<Character> borders = OutputConstant.BASIC_ASCII;

    if (headers.length != columns.length) {
      throw new IllegalArgumentException("Headers must be provided for all columns");
    }

    try (OutputStreamWriter osw = new OutputStreamWriter(baos, StandardCharsets.UTF_8)) {
      writeUpperBorder(osw, borders, System.lineSeparator(), columns);
      writeHeader(osw, borders, System.lineSeparator(), columns);
      writeHeaderBorder(osw, borders, System.lineSeparator(), columns);
      writeData(osw, borders, columns, System.lineSeparator());
      writeBottomBorder(osw, borders, System.lineSeparator(), columns);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new String(baos.toByteArray(), StandardCharsets.UTF_8);
  }

  /**
   * Writes the top border of the table using specified border characters.
   *
   * @param writer the writer for output
   * @param borders the collection of border characters for rendering
   * @param lineSeparator the system-specific line separator
   * @param columns the array of columns defining the table structure
   * @throws IOException if an error occurs while writing to the output
   */
  private static void writeUpperBorder(
      OutputStreamWriter writer, List<Character> borders, String lineSeparator, Column[] columns)
      throws IOException {
    writeHorizontalLine(
        writer,
        borders.get(TABLE_UPPER_BORDER_LEFT_IDX),
        borders.get(TABLE_UPPER_BORDER_MIDDLE_IDX),
        borders.get(TABLE_UPPER_BORDER_COLUMN_SEPARATOR_IDX),
        borders.get(TABLE_UPPER_BORDER_RIGHT_IDX),
        lineSeparator,
        columns);
  }

  /**
   * Writes the bottom border that separates the header from the table content.
   *
   * @param writer the writer for output
   * @param borders the collection of border characters for rendering
   * @param lineSeparator the system-specific line separator
   * @param columns the array of columns defining the table structure
   * @throws IOException if an error occurs while writing to the output
   */
  private static void writeHeaderBorder(
      OutputStreamWriter writer, List<Character> borders, String lineSeparator, Column[] columns)
      throws IOException {
    writeHorizontalLine(
        writer,
        borders.get(HEADER_BOTTOM_BORDER_LEFT_IDX),
        borders.get(HEADER_BOTTOM_BORDER_MIDDLE_IDX),
        borders.get(HEADER_BOTTOM_BORDER_COLUMN_SEPARATOR_IDX),
        borders.get(HEADER_BOTTOM_BORDER_RIGHT_IDX),
        lineSeparator,
        columns);
  }

  /**
   * Writes the separator line between data rows.
   *
   * @param writer the writer for output
   * @param borders the collection of border characters for rendering
   * @param lineSeparator the system-specific line separator
   * @param columns the array of columns defining the table structure
   * @throws IOException if an error occurs while writing to the output
   */
  private static void writeRowSeparator(
      OutputStreamWriter writer, List<Character> borders, String lineSeparator, Column[] columns)
      throws IOException {
    writeHorizontalLine(
        writer,
        borders.get(DATA_ROW_BORDER_LEFT_IDX),
        borders.get(DATA_ROW_BORDER_MIDDLE_IDX),
        borders.get(DATA_ROW_BORDER_COLUMN_SEPARATOR_IDX),
        borders.get(DATA_ROW_BORDER_RIGHT_IDX),
        lineSeparator,
        columns);
  }

  /**
   * Writes the bottom border that closes the table.
   *
   * @param writer the writer for output
   * @param borders the collection of border characters for rendering
   * @param lineSeparator the system-specific line separator
   * @param columns the array of columns defining the table structure
   * @throws IOException if an error occurs while writing to the output
   */
  private static void writeBottomBorder(
      OutputStreamWriter writer, List<Character> borders, String lineSeparator, Column[] columns)
      throws IOException {
    writeHorizontalLine(
        writer,
        borders.get(TABLE_BOTTOM_BORDER_LEFT_IDX),
        borders.get(TABLE_BOTTOM_BORDER_MIDDLE_IDX),
        borders.get(TABLE_BOTTOM_BORDER_COLUMN_SEPARATOR_IDX),
        borders.get(TABLE_BOTTOM_BORDER_RIGHT_IDX),
        lineSeparator,
        columns);
  }

  /**
   * Writes the data rows of the table.
   *
   * <p>For each row of data:
   *
   * <ul>
   *   <li>Writes the data line with appropriate borders and alignment
   *   <li>If not the last row and row boundaries are enabled in the style, writes a separator line
   *       between rows
   * </ul>
   *
   * @param writer the writer for output
   * @param borders the collection of border characters for rendering
   * @param columns the array of columns containing the data to write
   * @param lineSeparator the system-specific line separator
   * @throws IOException if an error occurs while writing to the output
   */
  private void writeData(
      OutputStreamWriter writer, List<Character> borders, Column[] columns, String lineSeparator)
      throws IOException {
    int dataSize = columns[0].getCellCount();
    Column.HorizontalAlign[] dataAligns =
        Arrays.stream(columns).map(Column::getDataAlign).toArray(Column.HorizontalAlign[]::new);

    for (int i = 0; i < dataSize; i++) {
      String[] data = getData(columns, i);
      writeRow(
          writer,
          borders.get(DATA_LINE_LEFT_IDX),
          borders.get(DATA_LINE_COLUMN_SEPARATOR_IDX),
          borders.get(DATA_LINE_RIGHT_IDX),
          data,
          columns,
          dataAligns,
          lineSeparator);
    }
  }

  /**
   * Writes a horizontal line in the table using specified border characters. The line consists of
   * repeated middle characters for each column width, separated by column separators and bounded by
   * left/right borders.
   *
   * @param osw The output stream writer for writing the line.
   * @param left The character used for the left border.
   * @param middle The character to repeat for creating the line.
   * @param columnSeparator The character used between columns.
   * @param right The character used for the right border.
   * @param lineSeparator The line separator to append.
   * @param columns Array of columns containing width information.
   * @throws IOException If an error occurs while writing to the output stream.
   */
  private static void writeHorizontalLine(
      OutputStreamWriter osw,
      Character left,
      Character middle,
      Character columnSeparator,
      Character right,
      String lineSeparator,
      Column[] columns)
      throws IOException {

    Integer[] colWidths =
        Arrays.stream(columns).map(s -> s.getMaxWidth() + 2 * PADDING).toArray(Integer[]::new);

    if (left != null) {
      osw.write(left);
    }

    for (int col = 0; col < colWidths.length; col++) {
      writeRepeated(osw, middle, colWidths[col]);
      if (columnSeparator != null && col != colWidths.length - 1) {
        osw.write(columnSeparator);
      }
    }

    if (right != null) {
      osw.write(right);
    }

    if (lineSeparator != null) {
      osw.write(System.lineSeparator());
    }
  }

  /**
   * Renders the header row of a formatted table, applying specified alignments and borders. This
   * method processes the column definitions to extract headers and their alignment, then delegates
   * the actual writing to writeDataLine.
   *
   * @param osw The output writer for writing the formatted header
   * @param borders A list containing border characters in the following order: [4]: left border
   *     character [5]: middle border character [6]: right border character
   * @param lineSeparator Platform-specific line separator (e.g., \n on Unix, \r\n on Windows)
   * @param columns Array of Column objects defining the structure of each table column, including
   *     header text and alignment preferences
   * @throws IOException If any error occurs during writing to the output stream
   */
  private static void writeHeader(
      OutputStreamWriter osw, List<Character> borders, String lineSeparator, Column[] columns)
      throws IOException {
    Column.HorizontalAlign[] dataAligns =
        Arrays.stream(columns).map(Column::getHeaderAlign).toArray(Column.HorizontalAlign[]::new);

    String[] headers =
        Arrays.stream(columns)
            .map(Column::getHeader)
            .filter(Objects::nonNull)
            .toArray(String[]::new);

    writeRow(
        osw,
        borders.get(4),
        borders.get(5),
        borders.get(6),
        headers,
        columns,
        dataAligns,
        lineSeparator);
  }

  /**
   * Write the data to the output stream.
   *
   * @param osw the output stream writer.
   * @param left the left border character.
   * @param columnSeparator the column separator character.
   * @param right the right border character.
   * @param data the data to write.
   * @param columns the columns to write.
   * @param lineSeparator the line separator.
   */
  private static void writeRow(
      OutputStreamWriter osw,
      Character left,
      Character columnSeparator,
      Character right,
      String[] data,
      Column[] columns,
      Column.HorizontalAlign[] dataAligns,
      String lineSeparator)
      throws IOException {

    int maxWidth;
    Column.HorizontalAlign dataAlign;

    if (left != null) {
      osw.write(left);
    }

    for (int i = 0; i < data.length; i++) {
      maxWidth = columns[i].getMaxWidth();
      dataAlign = dataAligns[i];
      writeJustified(osw, data[i], dataAlign, maxWidth, PADDING);
      if (i < data.length - 1) {
        osw.write(columnSeparator);
      }
    }

    if (right != null) {
      osw.write(right);
    }

    osw.write(lineSeparator);
  }

  /**
   * Retrieves data from all columns for a specific row index. Creates an array of cell values by
   * extracting the data at the given row index from each column.
   *
   * @param columns Array of columns to extract data from.
   * @param rowIndex Zero-based index of the row to retrieve.
   * @return Array of cell values for the specified row.
   * @throws IndexOutOfBoundsException if rowIndex is invalid for any column.
   */
  private static String[] getData(Column[] columns, int rowIndex) {
    return Arrays.stream(columns).map(c -> c.getCell(rowIndex)).toArray(String[]::new);
  }

  /**
   * Justifies the given string according to the specified alignment and maximum length then writes
   * it to the output stream.
   *
   * @param osw the output stream writer.
   * @param str the string to justify.
   * @param align the horizontal alignment.
   * @param maxLength the maximum length.
   * @param minPadding the minimum padding.
   * @throws IOException if an I/O error occurs.
   */
  private static void writeJustified(
      OutputStreamWriter osw,
      String str,
      Column.HorizontalAlign align,
      int maxLength,
      int minPadding)
      throws IOException {

    osw.write(LineUtil.getSpaces(minPadding));
    if (str.length() < maxLength) {
      int leftPadding =
          align == Column.HorizontalAlign.LEFT
              ? 0
              : align == Column.HorizontalAlign.CENTER
                  ? (maxLength - LineUtil.getDisplayWidth(str)) / 2
                  : maxLength - LineUtil.getDisplayWidth(str);

      writeRepeated(osw, ' ', leftPadding);
      osw.write(str);
      writeRepeated(osw, ' ', maxLength - LineUtil.getDisplayWidth(str) - leftPadding);
    } else {
      osw.write(str);
    }
    osw.write(LineUtil.getSpaces(minPadding));
  }

  /**
   * Writes a character repeatedly to the output stream a specified number of times. Used for
   * creating horizontal lines and padding in the table.
   *
   * @param osw Output stream to write to.
   * @param c Character to repeat.
   * @param num Number of times to repeat the character (must be non-negative).
   * @throws IOException If an I/O error occurs during writing.
   * @throws IllegalArgumentException if num is negative.
   */
  private static void writeRepeated(OutputStreamWriter osw, char c, int num) throws IOException {
    for (int i = 0; i < num; i++) {
      osw.append(c);
    }
  }

  /**
   * Formats a metalake into a table string representation. Creates a two-column table with headers
   * "METALAKE" and "COMMENT", containing the metalake's name and comment respectively.
   */
  static final class MetalakeTableFormat extends TableFormat<Metalake> {

    /**
     * Creates a new {@link TableFormat} with the specified properties.
     *
     * @param context the command context.
     */
    public MetalakeTableFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Metalake metalake) {
      Column columnName = new Column(context, "metalake");
      Column columnComment = new Column(context, "comment");

      columnName.addCell(metalake.name());
      columnComment.addCell(metalake.comment());

      return getTableFormat(columnName, columnComment);
    }
  }

  /**
   * Formats an array of Metalakes into a single-column table display. Lists all metalake names in a
   * vertical format.
   */
  static final class MetalakeListTableFormat extends TableFormat<Metalake[]> {

    public MetalakeListTableFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Metalake[] metalakes) {

      Column columnName = new Column(context, "metalake");
      Arrays.stream(metalakes).forEach(metalake -> columnName.addCell(metalake.name()));

      return getTableFormat(columnName);
    }
  }

  /**
   * Formats a single Catalog instance into a four-column table display. Displays catalog details
   * including name, type, provider, and comment information.
   */
  static final class CatalogTableFormat extends TableFormat<Catalog> {

    public CatalogTableFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Catalog catalog) {
      Column columnName = new Column(context, "catalog");
      Column columnType = new Column(context, "type");
      Column columnProvider = new Column(context, "provider");
      Column columnComment = new Column(context, "comment");

      columnName.addCell(catalog.name());
      columnType.addCell(catalog.type().name());
      columnProvider.addCell(catalog.provider());
      columnComment.addCell(catalog.comment());

      return getTableFormat(columnName, columnType, columnProvider, columnComment);
    }
  }

  /**
   * Formats an array of Catalogs into a single-column table display. Lists all catalog names in a
   * vertical format.
   */
  static final class CatalogListTableFormat extends TableFormat<Catalog[]> {

    public CatalogListTableFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Catalog[] catalogs) {
      Column columnName = new Column(context, "catalog");
      Arrays.stream(catalogs).forEach(metalake -> columnName.addCell(metalake.name()));

      return getTableFormat(columnName);
    }
  }

  /**
   * Formats a single {@link Schema} instance into a two-column table display. Displays catalog
   * details including name and comment information.
   */
  static final class SchemaTableFormat extends TableFormat<Schema> {
    public SchemaTableFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Schema schema) {
      Column columnName = new Column(context, "schema");
      Column columnComment = new Column(context, "comment");

      columnName.addCell(schema.name());
      columnComment.addCell(schema.comment());

      return getTableFormat(columnName, columnComment);
    }
  }

  /**
   * Formats an array of Schemas into a single-column table display. Lists all schema names in a
   * vertical format.
   */
  static final class SchemaListTableFormat extends TableFormat<Schema[]> {
    public SchemaListTableFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Schema[] schemas) {
      Column column = new Column(context, "schema");
      Arrays.stream(schemas).forEach(schema -> column.addCell(schema.name()));

      return getTableFormat(column);
    }
  }

  /**
   * Formats a single {@link Table} instance into a two-column table display. Displays table details
   * including name and comment information.
   */
  static final class TableDetailsTableFormat extends TableFormat<Table> {
    public TableDetailsTableFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Table table) {
      Column columnName = new Column(context, "name");
      Column columnType = new Column(context, "type");
      Column columnDefaultValue = new Column(context, "default");
      Column columnAutoIncrement = new Column(context, "AutoIncrement");
      Column columnNullable = new Column(context, "nullable");
      Column columnComment = new Column(context, "comment");

      org.apache.gravitino.rel.Column[] columns = table.columns();
      for (org.apache.gravitino.rel.Column column : columns) {
        columnName.addCell(column.name());
        columnType.addCell(column.dataType().simpleString());
        columnDefaultValue.addCell(LineUtil.getDefaultValue(column));
        columnAutoIncrement.addCell(LineUtil.getAutoIncrement(column));
        columnNullable.addCell(column.nullable());
        columnComment.addCell(
            column.comment() == null || column.comment().isEmpty() ? "N/A" : column.comment());
      }

      return getTableFormat(
          columnName,
          columnType,
          columnDefaultValue,
          columnAutoIncrement,
          columnNullable,
          columnComment);
    }
  }

  /**
   * Formats an array of {@link Table} into a single-column table display. Lists all table names in
   * a vertical format.
   */
  static final class TableListTableFormat extends TableFormat<Table[]> {
    public TableListTableFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Table[] tables) {
      Column column = new Column(context, "table");
      Arrays.stream(tables).forEach(table -> column.addCell(table.name()));

      return getTableFormat(column);
    }
  }

  /**
   * Formats a single {@link Audit} instance into a four-column table display. Displays audit
   * details, including creator, create time, modified, and modify time.
   */
  static final class AuditTableFormat extends TableFormat<Audit> {
    public AuditTableFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Audit audit) {
      Column columnCreator = new Column(context, "creator");
      Column columnCreateTime = new Column(context, "creation at");
      Column columnModified = new Column(context, "modifier");
      Column columnModifyTime = new Column(context, "modified at");

      columnCreator.addCell(audit.creator());
      columnCreateTime.addCell(audit.createTime() == null ? "N/A" : audit.createTime().toString());
      columnModified.addCell(audit.lastModifier() == null ? "N/A" : audit.lastModifier());
      columnModifyTime.addCell(
          audit.lastModifiedTime() == null ? "N/A" : audit.lastModifiedTime().toString());

      return getTableFormat(columnCreator, columnCreateTime, columnModified, columnModifyTime);
    }
  }

  /**
   * Formats an array of {@link org.apache.gravitino.rel.Column} into a six-column table display.
   * Lists all column names, types, default values, auto-increment, nullable, and comments in a
   * vertical format.
   */
  static final class ColumnListTableFormat extends TableFormat<org.apache.gravitino.rel.Column[]> {

    /**
     * Creates a new {@link TableFormat} with the specified properties.
     *
     * @param context the command context.
     */
    public ColumnListTableFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(org.apache.gravitino.rel.Column[] columns) {
      Column columnName = new Column(context, "name");
      Column columnType = new Column(context, "type");
      Column columnDefaultVal = new Column(context, "default");
      Column columnAutoIncrement = new Column(context, "AutoIncrement");
      Column columnNullable = new Column(context, "nullable");
      Column columnComment = new Column(context, "comment");

      for (org.apache.gravitino.rel.Column column : columns) {
        columnName.addCell(column.name());
        columnType.addCell(column.dataType().simpleString());
        columnDefaultVal.addCell(LineUtil.getDefaultValue(column));
        columnAutoIncrement.addCell(LineUtil.getAutoIncrement(column));
        columnNullable.addCell(column.nullable());
        columnComment.addCell(LineUtil.getComment(column));
      }

      return getTableFormat(
          columnName,
          columnType,
          columnDefaultVal,
          columnAutoIncrement,
          columnNullable,
          columnComment);
    }
  }
}
