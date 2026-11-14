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
package org.apache.gravitino.authorization.kafka;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.BaseAuthorization;

/**
 * Authorization provider implementation that synchronizes Apache Gravitino RBAC privileges and
 * ownership directly to Apache Kafka ACLs via {@code AdminClient}.
 */
public class KafkaAuthorization extends BaseAuthorization<KafkaAuthorization> {

  /** Provider short name registered in SPI for {@code authorization-provider = "kafka"}. */
  public static final String SHORT_NAME = "kafka";

  /** Creates a new {@link KafkaAuthorization} provider instance. */
  public KafkaAuthorization() {}

  @Override
  public String shortName() {
    return SHORT_NAME;
  }

  @Override
  public AuthorizationPlugin newPlugin(
      String metalake, String catalogProvider, Map<String, String> config) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalake), "Metalake name must not be blank");
    Preconditions.checkArgument(
        SHORT_NAME.equalsIgnoreCase(catalogProvider),
        "Unsupported catalog provider %s for Kafka authorization plugin (expected kafka)",
        catalogProvider);
    KafkaAuthorizationProperties properties = new KafkaAuthorizationProperties(config);
    properties.validate();
    return new KafkaAuthorizationPlugin(metalake, properties);
  }

  @Override
  public void close() throws IOException {
    // No provider-level resources to close; each catalog plugin manages its own AdminClient.
  }
}
