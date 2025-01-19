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

import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.GravitinoOptions;

/**
 * Configuration properties for controlling output formatting and behavior. This class encapsulates
 * all settings that affect how data is formatted and displayed, including alignment, styling, and
 * overflow handling.
 */
public class OutputProperty {
  public static final String OUTPUT_FORMAT_TABLE = "table";
  public static final String OUTPUT_FORMAT_PLAIN = "plain";

  /** Default configuration with common settings */
  private static final OutputProperty DEFAULT_OUTPUT_PROPERTY =
      new OutputProperty(
          false,
          false,
          -1,
          BorderStyle.BASIC2,
          HorizontalAlign.CENTER,
          HorizontalAlign.LEFT,
          HorizontalAlign.CENTER,
          OUTPUT_FORMAT_PLAIN,
          OverflowBehaviour.CLIP_RIGHT,
          true);

  private boolean sort;
  private boolean quiet;
  private int limit;
  private BorderStyle borderStyle;
  private HorizontalAlign headerAlign;
  private HorizontalAlign dataAlign;
  private final HorizontalAlign footerAlign;
  private String outputFormat;
  private final OverflowBehaviour overflowBehaviour;
  private boolean rowNumbersEnabled;

  /**
   * Creates a new {@link OutputProperty} with specified configurations.
   *
   * @param sort Whether to sort the output.
   * @param quiet Whether to suppress output.
   * @param limit Maximum number of rows (-1 for unlimited).
   * @param borderStyle Border style for tables.
   * @param headerAlign Header text alignment.
   * @param dataAlign Data cell alignment.
   * @param footerAlign Footer text alignment.
   * @param outputFormat Output format type.
   * @param overflowBehaviour Overflow handling strategy.
   * @param rowNumbersEnabled Whether to show row numbers.
   */
  public OutputProperty(
      boolean sort,
      boolean quiet,
      int limit,
      BorderStyle borderStyle,
      HorizontalAlign headerAlign,
      HorizontalAlign dataAlign,
      HorizontalAlign footerAlign,
      String outputFormat,
      OverflowBehaviour overflowBehaviour,
      boolean rowNumbersEnabled) {
    this.sort = sort;
    this.quiet = quiet;
    this.limit = limit;
    this.borderStyle = borderStyle;
    this.headerAlign = headerAlign;
    this.dataAlign = dataAlign;
    this.footerAlign = footerAlign;
    this.outputFormat = outputFormat;
    this.overflowBehaviour = overflowBehaviour;
    this.rowNumbersEnabled = rowNumbersEnabled;
  }

  /**
   * Returns a new instance with default output properties.
   *
   * @return Default configuration instance.
   */
  public static OutputProperty defaultOutputProperty() {
    return DEFAULT_OUTPUT_PROPERTY.copy();
  }

  /**
   * Creates a new {@link OutputProperty} instance from command line arguments.
   *
   * @param line Command line.
   * @return Configured {@code OutputProperty} instance.
   */
  public static OutputProperty fromLine(CommandLine line) {
    OutputProperty outputProperty = defaultOutputProperty();
    String outputFormat = line.getOptionValue(GravitinoOptions.OUTPUT);
    if (outputFormat != null) {
      outputProperty.setOutputFormat(outputFormat);
    }

    if (line.hasOption(GravitinoOptions.QUIET)) {
      outputProperty.setQuiet(true);
    }
    // TODO: implement other options.
    return outputProperty;
  }

  public boolean isSort() {
    return sort;
  }

  public boolean isQuiet() {
    return quiet;
  }

  public int getLimit() {
    return limit;
  }

  public BorderStyle getStyle() {
    return borderStyle;
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

  public String getOutputFormat() {
    return outputFormat;
  }

  public OverflowBehaviour getOverflowBehaviour() {
    return overflowBehaviour;
  }

  public boolean isRowNumbersEnabled() {
    return rowNumbersEnabled;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public void setSort(boolean sort) {
    this.sort = sort;
  }

  public void setQuiet(boolean quiet) {
    this.quiet = quiet;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setStyle(BorderStyle borderStyle) {
    this.borderStyle = borderStyle;
  }

  public void setRowNumbersEnabled(boolean rowNumbersEnabled) {
    this.rowNumbersEnabled = rowNumbersEnabled;
  }

  public void setHeaderAlign(HorizontalAlign headerAlign) {
    this.headerAlign = headerAlign;
  }

  public void setDataAlign(HorizontalAlign dataAlign) {
    this.dataAlign = dataAlign;
  }

  /**
   * Creates a new instance with current property values.
   *
   * @return New {@code OutputProperty} instance
   */
  public OutputProperty copy() {
    return new OutputProperty(
        sort,
        quiet,
        limit,
        borderStyle,
        headerAlign,
        dataAlign,
        footerAlign,
        outputFormat,
        overflowBehaviour,
        rowNumbersEnabled);
  }
}
