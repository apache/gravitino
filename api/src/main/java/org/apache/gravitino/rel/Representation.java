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
package org.apache.gravitino.rel;

import org.apache.gravitino.annotation.Unstable;

/**
 * A representation of a view's underlying definition. A view can carry multiple representations
 * targeting different engines or dialects. Currently only the {@link #TYPE_SQL SQL} representation
 * type is supported.
 */
@Unstable
public interface Representation {

  /** The representation type for SQL-based view definitions. */
  String TYPE_SQL = "sql";

  /**
   * Returns the representation type. The only supported value today is {@link #TYPE_SQL}.
   *
   * @return The representation type identifier.
   */
  String type();
}
