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

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/** A visible Glossary Term cannot accept a new Article binding in its current status. */
public class UnapprovedTermException extends GravitinoRuntimeException {
  /**
   * Creates the conflict for a Term whose read permission has been checked.
   *
   * @param term the visible Term name
   */
  public UnapprovedTermException(String term) {
    super("Term %s must be APPROVED before it can be bound to an Article", term);
  }

  /**
   * Creates an exception with the reported detail message.
   *
   * @param message the detail message
   * @param args arguments for the message
   */
  @FormatMethod
  public UnapprovedTermException(@FormatString String message, Object... args) {
    super(message, args);
  }
}
