/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.storage.relational.service;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.Field;

/**
 * This class is the collection wrapper of SupportsSkippingFields handler. The class will contain
 * all the handlers can proceed the data. We can choose different handlers according to the skipping
 * fields to acquire better performance.
 *
 * @param <T> The value type which the handler will return.
 */
class SupportsSkippingFieldsHandlers<T> {
  private final List<SupportsSkippingFields<T>> methods = Lists.newArrayList();

  // We should put the low-cost handler into the front of the list.
  void addHandler(SupportsSkippingFields<T> supportsSkippingFields) {
    methods.add(supportsSkippingFields);
  }

  T execute(Set<Field> reuiredFields) {
    for (SupportsSkippingFields<T> method : methods) {
      if (reuiredFields.containsAll(method.requiredFields())) {
        return method.execute();
      }
    }

    throw new IllegalArgumentException("Don't support skip fields");
  }
}
