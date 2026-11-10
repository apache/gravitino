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

package org.apache.gravitino.maintenance.optimizer.updater.calculator.local;

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import org.apache.commons.lang3.StringUtils;

/** Importer for inline JSON-lines statistics payloads. */
public class PayloadStatisticsImporter extends AbstractStatisticsImporter {

  private final String payload;

  public PayloadStatisticsImporter(String payload, String defaultCatalogName) {
    super(defaultCatalogName);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(payload), "Statistics payload must be provided");
    this.payload = payload;
  }

  @Override
  protected BufferedReader openReader() throws IOException {
    return new BufferedReader(new StringReader(payload));
  }
}
