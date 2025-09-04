/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.credential.config;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.storage.GCSProperties;

public class GCSCredentialConfig extends Config {

  public static final ConfigEntry<String> GCS_CREDENTIAL_FILE_PATH =
      new ConfigBuilder(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE)
          .doc("The path of GCS credential file")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public GCSCredentialConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  @Nullable
  public String gcsCredentialFilePath() {
    return this.get(GCS_CREDENTIAL_FILE_PATH);
  }
}
