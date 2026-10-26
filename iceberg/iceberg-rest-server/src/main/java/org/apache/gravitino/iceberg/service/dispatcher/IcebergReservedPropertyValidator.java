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
package org.apache.gravitino.iceberg.service.dispatcher;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.StringIdentifier;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

/**
 * Rejects Iceberg REST requests that set a property Gravitino reserves for itself.
 *
 * <p>Gravitino reads {@link StringIdentifier#ID_KEY} to match an external object to its stored
 * entity. A client that sets it could copy another object's id and get attached to that entity, with its
 * owner and grants, so the Gravitino REST API never accepts it from users and neither does this
 * server.
 */
final class IcebergReservedPropertyValidator {

  static void validateProperties(@Nullable Map<String, String> properties) {
    if (properties != null && properties.containsKey(StringIdentifier.ID_KEY)) {
      throw new IllegalArgumentException(
          String.format(
              "Property %s is reserved by Gravitino and cannot be set", StringIdentifier.ID_KEY));
    }
  }

  static void validateUpdate(UpdateTableRequest request) {
    request.updates().stream()
        .filter(MetadataUpdate.SetProperties.class::isInstance)
        .map(MetadataUpdate.SetProperties.class::cast)
        .map(MetadataUpdate.SetProperties::updated)
        .forEach(IcebergReservedPropertyValidator::validateProperties);
  }

  private IcebergReservedPropertyValidator() {}
}
