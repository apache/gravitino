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

import static org.apache.gravitino.cli.outputs.Constant.DATA_LINE_COLUMN_SEPARATOR_IDX;
import static org.apache.gravitino.cli.outputs.Constant.DATA_LINE_LEFT_IDX;
import static org.apache.gravitino.cli.outputs.Constant.DATA_LINE_RIGHT_IDX;
import static org.apache.gravitino.cli.outputs.Constant.DATA_ROW_BORDER_COLUMN_SEPARATOR_IDX;
import static org.apache.gravitino.cli.outputs.Constant.DATA_ROW_BORDER_LEFT_IDX;
import static org.apache.gravitino.cli.outputs.Constant.DATA_ROW_BORDER_MIDDLE_IDX;
import static org.apache.gravitino.cli.outputs.Constant.DATA_ROW_BORDER_RIGHT_IDX;
import static org.apache.gravitino.cli.outputs.Constant.HEADER_BOTTOM_BORDER_COLUMN_SEPARATOR_IDX;
import static org.apache.gravitino.cli.outputs.Constant.HEADER_BOTTOM_BORDER_LEFT_IDX;
import static org.apache.gravitino.cli.outputs.Constant.HEADER_BOTTOM_BORDER_MIDDLE_IDX;
import static org.apache.gravitino.cli.outputs.Constant.HEADER_BOTTOM_BORDER_RIGHT_IDX;
import static org.apache.gravitino.cli.outputs.Constant.TABLE_BOTTOM_BORDER_COLUMN_SEPARATOR_IDX;
import static org.apache.gravitino.cli.outputs.Constant.TABLE_BOTTOM_BORDER_LEFT_IDX;
import static org.apache.gravitino.cli.outputs.Constant.TABLE_BOTTOM_BORDER_MIDDLE_IDX;
import static org.apache.gravitino.cli.outputs.Constant.TABLE_BOTTOM_BORDER_RIGHT_IDX;
import static org.apache.gravitino.cli.outputs.Constant.TABLE_UPPER_BORDER_COLUMN_SEPARATOR_IDX;
import static org.apache.gravitino.cli.outputs.Constant.TABLE_UPPER_BORDER_LEFT_IDX;
import static org.apache.gravitino.cli.outputs.Constant.TABLE_UPPER_BORDER_MIDDLE_IDX;
import static org.apache.gravitino.cli.outputs.Constant.TABLE_UPPER_BORDER_RIGHT_IDX;

import com.google.common.base.Preconditions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.cli.CommandContext;

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
    checkColumns(columns);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    String[] headers =
        Arrays.stream(columns)
            .map(Column::getHeader)
            .filter(Objects::nonNull)
            .toArray(String[]::new);

    List<Character> borders = Constant.BASIC_ASCII;
    checkHeaders(headers, columns);

    if (headers.length != columns.length) {
      throw new IllegalArgumentException("Headers must be provided for all columns");
    }

    if (limit != -1) {
      columns = getLimitedColumns(columns);
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

  private void checkColumns(Column... columns) {
    Preconditions.checkArgument(columns.length > 0, "At least one column must be provided");
    int cellCount = columns[0].getCellCount();
    for (Column column : columns) {
      Preconditions.checkArgument(
          column.getCellCount() == cellCount, "All columns must have the same cell count");
    }
  }

  private void checkHeaders(String[] headers, Column[] columns) {
    Preconditions.checkArgument(
        headers.length == columns.length, "Headers must be provided for all columns");
    for (String header : headers) {
      Preconditions.checkArgument(header != null, "Headers must not be null");
    }
  }

  /**
   * Limits the number of rows in the table columns based on a predefined limit. If the current cell
   * count is below the limit, returns the original columns unchanged. Otherwise, creates new
   * columns with data truncated to the limit.
   *
   * @param columns The array of columns to potentially limit
   * @return A new array of columns with limited rows, or the original array if no limiting is
   *     needed
   * @throws IllegalArgumentException If the columns array is null or empty
   */
  private Column[] getLimitedColumns(Column[] columns) {
    if (columns[0].getCellCount() < limit) {
      return columns;
    }

    Column[] limitedColumns = new Column[columns.length];
    for (int i = 0; i < columns.length; i++) {
      limitedColumns[i] = columns[i].getLimitedColumn(limit);
    }

    return limitedColumns;
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
      if (metalakes.length == 0) {
        output("No metalakes exist.", System.err);
        return null;
      } else {
        Column columnName = new Column(context, "metalake");
        Arrays.stream(metalakes).forEach(metalake -> columnName.addCell(metalake.name()));

        return getTableFormat(columnName);
      }
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
      if (catalogs.length == 0) {
        output("No metalakes exist.", System.err);
        return null;
      } else {
        Column columnName = new Column(context, "catalog");
        Arrays.stream(catalogs).forEach(metalake -> columnName.addCell(metalake.name()));

        return getTableFormat(columnName);
      }
    }
  }
}
