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

import com.google.common.collect.ImmutableList;

public class OutputConstant {
  public static final ImmutableList<Character> BASIC_ASCII =
      ImmutableList.of(
          '+', '-', '+', '+', '|', '|', '|', '+', '-', '+', '+', '|', '|', '|', '+', '-', '+', '+',
          '+', '-', '+', '+', '|', '|', '|', '+', '-', '+', '+');

  // ===== Table Upper Border Indices =====
  public static final int TABLE_UPPER_BORDER_LEFT_IDX = 0;
  public static final int TABLE_UPPER_BORDER_MIDDLE_IDX = 1;
  public static final int TABLE_UPPER_BORDER_COLUMN_SEPARATOR_IDX = 2;
  public static final int TABLE_UPPER_BORDER_RIGHT_IDX = 3;

  // ===== Data Line Indices =====
  public static final int DATA_LINE_LEFT_IDX = 4;
  public static final int DATA_LINE_COLUMN_SEPARATOR_IDX = 5;
  public static final int DATA_LINE_RIGHT_IDX = 6;

  // ===== Data Row Border Indices =====
  public static final int DATA_ROW_BORDER_LEFT_IDX = 14;
  public static final int DATA_ROW_BORDER_MIDDLE_IDX = 15;
  public static final int DATA_ROW_BORDER_COLUMN_SEPARATOR_IDX = 16;
  public static final int DATA_ROW_BORDER_RIGHT_IDX = 17;

  // ===== Table Bottom Border Indices =====
  public static final int TABLE_BOTTOM_BORDER_LEFT_IDX = 25;
  public static final int TABLE_BOTTOM_BORDER_MIDDLE_IDX = 26;
  public static final int TABLE_BOTTOM_BORDER_COLUMN_SEPARATOR_IDX = 27;
  public static final int TABLE_BOTTOM_BORDER_RIGHT_IDX = 28;

  // ===== Header Bottom Border Indices =====
  public static final int HEADER_BOTTOM_BORDER_LEFT_IDX = 18;
  public static final int HEADER_BOTTOM_BORDER_MIDDLE_IDX = 19;
  public static final int HEADER_BOTTOM_BORDER_COLUMN_SEPARATOR_IDX = 20;
  public static final int HEADER_BOTTOM_BORDER_RIGHT_IDX = 21;

  private OutputConstant() {
    // private constructor to prevent instantiation
  }
}
