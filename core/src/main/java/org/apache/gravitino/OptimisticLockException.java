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
package org.apache.gravitino;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;

/**
 * Thrown when a concurrent entity modification is detected via a {@code current_version} mismatch
 * in the relational entity store. The {@link org.apache.gravitino.lock.TreeLockUtils} retry loop
 * catches this exception and re-executes the operation up to a fixed number of times.
 *
 * <p>This exception is an internal implementation detail and should not cross the public API
 * boundary.
 */
public class OptimisticLockException extends GravitinoRuntimeException {

  /**
   * Constructs an OptimisticLockException.
   *
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public OptimisticLockException(@FormatString String message, Object... args) {
    super(message, args);
  }
}
