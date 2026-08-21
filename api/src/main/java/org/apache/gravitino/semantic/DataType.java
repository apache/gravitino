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
package org.apache.gravitino.semantic;

import org.apache.gravitino.annotation.Evolving;

/**
 * Logical data types for Semantic Model fields and metrics, derived from the Apache Ossie logical
 * type vocabulary. These types are independent of physical relational column types and are not
 * inferred from source schemas.
 */
@Evolving
public enum DataType {
  /** A string value. */
  STRING,

  /** An integer value. */
  INTEGER,

  /** An exact base-10 decimal value with unspecified precision and scale. */
  DECIMAL,

  /** An approximate floating-point value. */
  FLOAT,

  /** A boolean value. */
  BOOLEAN,

  /** A date without a time of day. */
  DATE,

  /** A time of day without a date. */
  TIME,

  /** A date and time without a timezone or offset. */
  DATE_TIME,

  /** A date and time that identifies an instant using timezone or offset context. */
  DATE_TIME_TZ,

  /** A known logical type outside the portable type vocabulary. */
  OPAQUE
}
