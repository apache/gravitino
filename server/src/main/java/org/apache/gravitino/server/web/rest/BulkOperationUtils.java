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
package org.apache.gravitino.server.web.rest;

import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.dto.responses.BulkOperationFailureDTO;
import org.apache.gravitino.dto.responses.BulkOperationResponse;

class BulkOperationUtils {

  private BulkOperationUtils() {}

  static BulkOperationResponse bulkOperation(String[] names, BulkNameOperator operator) {
    return bulkOperation(names, operator, name -> name);
  }

  static <T> BulkOperationResponse bulkOperation(
      T[] items, BulkOperator<T> operator, BulkNameExtractor<T> nameExtractor) {
    List<String> succeeded = new ArrayList<>();
    List<BulkOperationFailureDTO> failed = new ArrayList<>();

    for (T item : items) {
      String name = nameExtractor.name(item);
      try {
        succeeded.add(operator.apply(item));
      } catch (Exception e) {
        failed.add(new BulkOperationFailureDTO(name, failureReason(e)));
      }
    }

    return new BulkOperationResponse(
        succeeded.toArray(new String[0]), failed.toArray(new BulkOperationFailureDTO[0]));
  }

  private static String failureReason(Exception e) {
    String message = e.getMessage();
    if (message == null || message.isBlank()) {
      return e.getClass().getSimpleName();
    }
    return e.getClass().getSimpleName() + ": " + message;
  }

  @FunctionalInterface
  interface BulkNameOperator extends BulkOperator<String> {}

  @FunctionalInterface
  interface BulkOperator<T> {
    String apply(T item) throws Exception;
  }

  @FunctionalInterface
  interface BulkNameExtractor<T> {
    String name(T item);
  }
}
