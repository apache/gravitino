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

/**
 * Specifies the horizontal text alignment within table elements such as cells, headers, and
 * footers. This enum provides options for standard left-to-right text positioning.
 */
public enum HorizontalAlign {
  /** Text is aligned to the left side */
  LEFT,

  /** Text is centered horizontally */
  CENTER,

  /** Text is aligned to the right side */
  RIGHT
}
