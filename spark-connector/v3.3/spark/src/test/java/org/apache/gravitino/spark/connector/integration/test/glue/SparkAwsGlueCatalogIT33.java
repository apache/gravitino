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
package org.apache.gravitino.spark.connector.integration.test.glue;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Integration test for Spark 3.3 Glue catalog connector against a real AWS Glue endpoint.
 *
 * <p>This test is <b>skipped by default</b> and only runs when {@code AWS_ACCESS_KEY_ID} is set.
 * Required environment variables:
 *
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID}
 *   <li>{@code AWS_SECRET_ACCESS_KEY}
 *   <li>{@code AWS_DEFAULT_REGION}
 *   <li>{@code AWS_S3_TEST_BUCKET}
 * </ul>
 */
@EnabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")
public class SparkAwsGlueCatalogIT33 extends SparkGlueCatalogIT {}
