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

package org.apache.gravitino.exceptions;

/**
 * Thrown when a table row exists but the server cannot open or modify its dataset location.
 *
 * <p>The table metadata is still present. Callers that only need to know whether the table exists
 * can treat this as existence. Callers that need the dataset must not continue.
 */
public class UnsupportedTableStorageException extends UnsupportedOperationException {

  /**
   * Constructs the exception with the caller-facing reason.
   *
   * @param message the reason the dataset location cannot be used
   */
  public UnsupportedTableStorageException(String message) {
    super(message);
  }
}
