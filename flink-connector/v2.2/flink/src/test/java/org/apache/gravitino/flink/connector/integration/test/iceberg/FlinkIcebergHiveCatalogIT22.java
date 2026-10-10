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

package org.apache.gravitino.flink.connector.integration.test.iceberg;

import org.junit.jupiter.api.condition.DisabledIf;

/** Runs Iceberg Hive backend integration tests with Flink 2.2 in deploy mode. */
// The Flink runtime uses Iceberg 1.12 while the server uses Iceberg 1.11. Embedded mode shares
// their incompatible classes in one JVM; each concrete class declares this non-inherited condition.
@DisabledIf("org.apache.gravitino.integration.test.util.ITUtils#isEmbedded")
public class FlinkIcebergHiveCatalogIT22 extends FlinkIcebergHiveCatalogIT {}
