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
import java.util.Locale;
import org.apache.gravitino.cli.utils.LineUtil;

/**
 * Represents a column in a formatted table output. Manages column properties including header,
 * footer, alignment, visibility, and content cells. Handles width calculations and content overflow
 * behavior.
 */
public class Column {
  private final String header;
  private final String footer;
  private final HorizontalAlign headerAlign;
  private final HorizontalAlign dataAlign;
  private final HorizontalAlign footerAlign;
  private OverflowBehaviour overflowBehaviour;
  private int maxWidth;
  private boolean visible;
  private final OutputProperty property;
  private List<String> cellContents;

  /**
   * Creates a {@link Column} instance with specified header, footer and output properties.
   * Initializes the column with default values and empty cell list.
   *
   * @param header Column header text (will be converted to uppercase).
   * @param footer Column footer text.
   * @param property a {@link OutputProperty} instance containing Output formatting properties.
   */
  public Column(String header, String footer, OutputProperty property) {
    this.property = property;
    this.header = header == null ? "" : header.toUpperCase(Locale.ENGLISH);
    this.footer = footer;
    this.headerAlign = property.getHeaderAlign();
    this.dataAlign = property.getDataAlign();
    this.footerAlign = property.getFooterAlign();
    this.overflowBehaviour = property.getOverflowBehaviour();
    this.visible = true;
    this.maxWidth = LineUtil.getDisplayWidth(header);
    this.cellContents = Lists.newArrayList();
  }

  /**
   * Creates a copy of this column with the same properties but empty cells.
   *
   * @return New {@link Column} instance with copied properties.
   */
  public Column copy() {
    Column newColumn = new Column(header, footer, property);
    newColumn.setOverflowBehaviour(overflowBehaviour);
    newColumn.setVisible(visible);

    return newColumn;
  }

  /**
   * Adds a new cell to the column and updates the maximum width if necessary. Null values are
   * converted to "null" string.
   *
   * @param cell Cell content to add.
   * @return This column instance for method chaining.
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
   * Creates a new {@link Column} with limited number of cells and an ellipsis indicator.
   *
   * @param limit Maximum number of cells to include
   * @return New Column instance with limited cells
   */
  public Column getLimitedColumn(int limit) {
    if (cellContents.size() <= limit) {
      return this;
    }

    Column newColumn = copy();
    newColumn.cellContents = cellContents.subList(0, Math.min(limit, cellContents.size()));
    newColumn.reCalculateMaxWidth();
    newColumn.addCell(String.valueOf(OutputConstant.ELLIPSIS));

    return newColumn;
  }

  private void reCalculateMaxWidth() {
    for (String cell : cellContents) {
      maxWidth = Math.max(maxWidth, LineUtil.getDisplayWidth(cell));
    }
  }

  public String getCell(int index) {
    return cellContents.get(index);
  }

  public int getCellCount() {
    return cellContents.size();
  }

  public String getHeader() {
    return header;
  }

  public String getFooter() {
    return footer;
  }

  public HorizontalAlign getHeaderAlign() {
    return headerAlign;
  }

  public HorizontalAlign getDataAlign() {
    return dataAlign;
  }

  public HorizontalAlign getFooterAlign() {
    return footerAlign;
  }

  public int getMaxWidth() {
    return maxWidth;
  }

  public void setMaxWidth(int maxWidth) {
    this.maxWidth = maxWidth;
  }

  public void setOverflowBehaviour(OverflowBehaviour overflowBehaviour) {
    this.overflowBehaviour = overflowBehaviour;
  }

  public void setVisible(boolean visible) {
    this.visible = visible;
  }
}
