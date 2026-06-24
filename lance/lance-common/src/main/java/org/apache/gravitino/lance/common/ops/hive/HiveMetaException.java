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
package org.apache.gravitino.lance.common.ops.hive;

import com.google.errorprone.annotations.FormatMethod;
import org.apache.hadoop.hive.metastore.api.MetaException;

/** Runtime wrapper for Hive Metastore exceptions. Adapted from Apache Iceberg for Hive 2.x. */
class HiveMetaException extends RuntimeException {

  /**
   * Wrap a Hive Metastore exception.
   *
   * @param cause the underlying metastore exception
   */
  HiveMetaException(MetaException cause) {
    super(cause);
  }

  /**
   * Wrap a Hive Metastore exception with a formatted message.
   *
   * @param cause the underlying metastore exception
   * @param message the message format string
   * @param args the format arguments
   */
  @FormatMethod
  HiveMetaException(MetaException cause, String message, Object... args) {
    super(String.format(message, args), cause);
  }

  /**
   * Wrap an arbitrary throwable with a formatted message.
   *
   * @param throwable the underlying cause
   * @param message the message format string
   * @param args the format arguments
   */
  @FormatMethod
  HiveMetaException(Throwable throwable, String message, Object... args) {
    super(String.format(message, args), throwable);
  }
}
