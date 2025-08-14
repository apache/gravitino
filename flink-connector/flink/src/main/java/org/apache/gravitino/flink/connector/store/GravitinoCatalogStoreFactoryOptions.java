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

package org.apache.gravitino.flink.connector.store;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class GravitinoCatalogStoreFactoryOptions {

  private GravitinoCatalogStoreFactoryOptions() {}

  public static final String GRAVITINO = "gravitino";

  public static final ConfigOption<String> GRAVITINO_URI =
      ConfigOptions.key("gravitino.uri")
          .stringType()
          .noDefaultValue()
          .withDescription("The uri of Gravitino server");
  public static final ConfigOption<String> GRAVITINO_METALAKE =
      ConfigOptions.key("gravitino.metalake")
          .stringType()
          .noDefaultValue()
          .withDescription("The name of Gravitino metalake");

  public static final ConfigOption<Map<String, String>> GRAVITINO_CLIENT_CONFIG =
      ConfigOptions.key("gravitino.client")
          .mapType()
          .defaultValue(ImmutableMap.of())
          .withDescription("The config of Gravitino client");
}
