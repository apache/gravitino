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

package org.apache.gravitino.catalog.postgresql.integration.test;

import org.apache.gravitino.integration.test.container.PGImageName;
import org.apache.gravitino.integration.test.container.PostgreSQLFixture;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.Tag;

@Tag("gravitino-docker-test")
@PostgreSQLFixture(
    version = PGImageName.VERSION_16,
    database = TestDatabaseName.PG_CATALOG_POSTGRESQL_IT)
public class CatalogPostgreSqlVersion16IT extends CatalogPostgreSqlIT {}
