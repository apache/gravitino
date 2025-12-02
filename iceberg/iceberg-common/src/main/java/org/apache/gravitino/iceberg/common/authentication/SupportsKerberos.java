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

package org.apache.gravitino.iceberg.common.authentication;

import java.util.Map;

/**
 * An interface to indicate that the implementing class supports Kerberos authentication and can do
 * operations with Kerberos.
 */
public interface SupportsKerberos {

  /**
   * Perform operations with Kerberos authentication.
   *
   * @param properties the catalog properties
   * @param executable the operations to be performed
   * @return the result of the operations
   * @param <R> the return type of the operations
   * @throws Throwable if any error occurs during the operations
   */
  <R> R doKerberosOperations(Map<String, String> properties, Executable<R> executable)
      throws Throwable;

  @FunctionalInterface
  interface Executable<R> {
    R execute() throws Throwable;
  }
}
