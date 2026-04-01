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

package org.apache.gravitino.listener.api.event.function;

import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.listener.api.event.OperationType;

/** Represents an event triggered before altering a function. */
@DeveloperApi
public class AlterFunctionPreEvent extends FunctionPreEvent {
  private final FunctionChange[] functionChanges;

  public AlterFunctionPreEvent(
      String user, NameIdentifier identifier, FunctionChange[] functionChanges) {
    super(user, identifier);
    this.functionChanges = Arrays.copyOf(functionChanges, functionChanges.length);
  }

  /**
   * Retrieves the specific changes that were made to the function during the alteration process.
   *
   * @return An array of {@link FunctionChange} objects detailing each modification applied to the
   *     function.
   */
  public FunctionChange[] functionChanges() {
    return Arrays.copyOf(functionChanges, functionChanges.length);
  }

  @Override
  public OperationType operationType() {
    return OperationType.ALTER_FUNCTION;
  }
}
