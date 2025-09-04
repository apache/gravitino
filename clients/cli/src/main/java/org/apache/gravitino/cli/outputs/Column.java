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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.cli.CommandContext;

/**
 * Represents a column in a formatted table output. Manages column properties including header,
 * alignment, and content cells. Handles width calculations.
 */
public class Column {
  /** The character used to indicate that a cell has been truncated. */
  public static final char ELLIPSIS = '…';

  private final String header;
  private final HorizontalAlign headerAlign;
  private final HorizontalAlign dataAlign;
  private final CommandContext context;

  private int maxWidth;
  private List<String> cellContents;

  /**
   * Creates a new {@code Column} instance with the specified header and default alignment.
   *
   * @param context the command context.
   * @param header the header of the column.
   */
  public Column(CommandContext context, String header) {
    this(context, header, HorizontalAlign.CENTER, HorizontalAlign.LEFT);
  }

  /**
   * Creates a new {@code Column} instance with the specified header and alignment.
   *
   * @param context the command context.
   * @param header the header of the column.
   * @param headerAlign the alignment of the header.
   * @param dataAlign the alignment of the data in the column.
   */
  public Column(
      CommandContext context,
      String header,
      HorizontalAlign headerAlign,
      HorizontalAlign dataAlign) {
    this.context = context;
    this.header = LineUtil.capitalize(header);
    this.headerAlign = headerAlign;
    this.dataAlign = dataAlign;

    this.cellContents = Lists.newArrayList();
    this.maxWidth = LineUtil.getDisplayWidth(header);
  }

  /**
   * Specifies the horizontal text alignment within table elements such as cells and headers. This
   * enum provides options for standard left-to-right text positioning.
   */
  public enum HorizontalAlign {
    /** Aligns text to the left. */
    LEFT,
    /** Aligns text to the center. */
    CENTER,
    /** Aligns text to the right. */
    RIGHT
  }

  /**
   * Returns the header of the column.
   *
   * @return the header of the column.
   */
  public String getHeader() {
    return header;
  }

  /**
   * Returns the alignment of the header.
   *
   * @return the alignment of the header.
   */
  public HorizontalAlign getHeaderAlign() {
    return headerAlign;
  }

  /**
   * Returns the alignment of the data in the column.
   *
   * @return the alignment of the data in the column.
   */
  public HorizontalAlign getDataAlign() {
    return dataAlign;
  }

  /**
   * Returns the maximum width of the column.
   *
   * @return the maximum width of the column.
   */
  public int getMaxWidth() {
    return maxWidth;
  }

  /**
   * Returns the command context.
   *
   * @return the {@link CommandContext} instance.
   */
  public CommandContext getContext() {
    return context;
  }

  /**
   * Returns a copy of this column.
   *
   * @return a copy of this column.
   */
  public Column copy() {
    return new Column(context, header, headerAlign, dataAlign);
  }

  /**
   * Adds a cell to the column and updates the maximum width of the column.
   *
   * @param cell the cell to add to the column.
   * @return this column instance, for chaining.
   */
  public Column addCell(String cell) {
    if (cell == null) {
      cell = "null";
    }

    maxWidth = Math.max(maxWidth, LineUtil.getDisplayWidth(cell));
    cellContents.add(cell);
    return this;
  }

  /**
   * Adds a cell to the column and updates the maximum width of the column.
   *
   * @param cell the cell to add to the column.
   * @return this column instance, for chaining.
   */
  public Column addCell(Object cell) {
    return addCell(cell == null ? "null" : cell.toString());
  }

  /**
   * Adds a cell to the column and updates the maximum width of the column.
   *
   * @param cell the cell to add to the column.
   * @return this column instance, for chaining.
   */
  public Column addCell(char cell) {
    return addCell(String.valueOf(cell));
  }

  /**
   * Adds a cell to the column and updates the maximum width of the column.
   *
   * @param cell the cell to add to the column.
   * @return this column instance, for chaining.
   */
  public Column addCell(int cell) {
    return addCell(String.valueOf(cell));
  }

  /**
   * Adds a cell to the column and updates the maximum width of the column.
   *
   * @param cell the cell to add to the column.
   * @return this column instance, for chaining.
   */
  public Column addCell(double cell) {
    return addCell(String.valueOf(cell));
  }

  /**
   * Adds a cell to the column and updates the maximum width of the column.
   *
   * @param cell the cell to add to the column.
   * @return this column instance, for chaining.
   */
  public Column addCell(boolean cell) {
    return addCell(String.valueOf(cell));
  }

  /**
   * Returns a limited version of this column, with a maximum of {@code limit} cells.
   *
   * @param limit the maximum number of cells to include in the limited column.
   * @return a limited version of this column, with a maximum of {@code limit} cells.
   */
  public Column getLimitedColumn(int limit) {
    if (cellContents.size() <= limit) {
      return this;
    }

    Column newColumn = copy();
    newColumn.cellContents = cellContents.subList(0, Math.min(limit, cellContents.size()));
    newColumn.reCalculateMaxWidth();
    newColumn.addCell(ELLIPSIS);

    return newColumn;
  }

  /**
   * Returns the cell at the specified index.
   *
   * @param index the index of the cell to return.
   * @return the cell at the specified index.
   */
  public String getCell(int index) {
    return cellContents.get(index);
  }

  /**
   * Returns the number of cells in the column.
   *
   * @return the number of cells in the column.
   */
  public int getCellCount() {
    return cellContents.size();
  }

  private void reCalculateMaxWidth() {
    for (String cell : cellContents) {
      maxWidth = Math.max(maxWidth, LineUtil.getDisplayWidth(cell));
    }
  }
}
