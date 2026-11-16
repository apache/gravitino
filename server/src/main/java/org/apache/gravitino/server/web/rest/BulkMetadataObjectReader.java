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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.bulk.BulkItemResult;
import org.apache.gravitino.bulk.BulkManager;
import org.apache.gravitino.dto.responses.BulkError;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;

/** Reads permitted objects in bulk, isolating object-level failures. */
final class BulkMetadataObjectReader {
  private BulkMetadataObjectReader() {}

  static <T, R> List<BulkItemResult<R>> read(
      String metalake,
      MetadataObjectDTO[] requested,
      Function<List<MetadataObject>, Map<MetadataObject, T>> load,
      BiFunction<Integer, T, R> convert) {
    List<MetadataObject> objects =
        Arrays.stream(requested)
            .map(object -> MetadataObjects.parse(object.fullName(), object.type()))
            .collect(Collectors.toList());
    Set<MetadataObject> permitted =
        Arrays.stream(MetadataAuthzHelper.filterMetadataObject(metalake, requested))
            .map(object -> MetadataObjects.parse(object.fullName(), object.type()))
            .collect(Collectors.toSet());
    List<MetadataObject> readable =
        objects.stream().filter(permitted::contains).collect(Collectors.toList());
    Map<MetadataObject, T> values = Collections.emptyMap();
    @Nullable Exception batchError = null;
    if (!readable.isEmpty()) {
      try {
        values = load.apply(readable);
      } catch (Exception e) {
        batchError = e;
      }
    }

    List<BulkItemResult<R>> results = new ArrayList<>();
    for (int i = 0; i < objects.size(); i++) {
      MetadataObject object = objects.get(i);
      try {
        if (!permitted.contains(object)) {
          throw new ForbiddenException("Not authorized to access the requested metadata object");
        }
        // A failed singleton already identifies the object; retry only a failed batch.
        if (batchError != null && readable.size() == 1) {
          throw batchError;
        }
        T value =
            batchError == null
                ? values.get(object)
                : load.apply(Collections.singletonList(object)).get(object);
        results.add(BulkItemResult.success(i, object.fullName(), convert.apply(i, value)));
      } catch (Exception e) {
        results.add(BulkItemResult.failure(i, object.fullName(), e));
      }
    }
    return results;
  }

  static BulkError[] errors(List<? extends BulkItemResult<?>> results, BulkManager manager) {
    return results.stream()
        .filter(result -> !result.succeeded())
        .map(manager::toBulkError)
        .map(
            error ->
                new BulkError(
                    error.getIndex(),
                    error.getName(),
                    error.getCode(),
                    error.getType(),
                    StringUtils.defaultIfBlank(error.getMessage(), error.getType())))
        .toArray(BulkError[]::new);
  }
}
