/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Declares that a test class needs a {@link TrinoCluster}: a Trino coordinator, its workers and the
 * Hive, MySQL and PostgreSQL it queries. The cluster calls the class's Gravitino server, so it
 * starts lazily: ask for it as a parameter of a {@code @BeforeAll} or test method, which runs after
 * {@code BaseIT} has started the server.
 *
 * <pre>{@code
 * @TrinoFixture
 * class CatalogsPageTest extends BaseWebIT {
 *   @BeforeAll
 *   void before(TrinoCluster trino) {
 *     hiveMetastoreUri = trino.serviceUrls().get("hive_uri");
 *   }
 * }
 * }</pre>
 */
@Documented
@Inherited
@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(ContainerExtension.class)
@ContainerFixture(TrinoSpec.Factory.class)
public @interface TrinoFixture {

  /**
   * Returns the Trino version to run.
   *
   * @return the version; 478 by default
   */
  int version() default TrinoCluster.DEFAULT_TRINO_VERSION;

  /**
   * Returns the number of Trino workers besides the coordinator.
   *
   * @return the worker count; none by default, so the coordinator runs queries itself
   */
  int workers() default 0;

  /**
   * Returns the Hive version next to Trino.
   *
   * @return the Hive version; Hive2 by default
   */
  HiveVersion hive() default HiveVersion.HIVE2;

  /**
   * Returns how long the cluster lives.
   *
   * @return the scope; the whole run by default, shared with other classes
   */
  Scope scope() default Scope.RUN;

  /**
   * Returns when the cluster starts.
   *
   * @return lazily by default, since the Gravitino server must run first
   */
  Startup startup() default Startup.LAZY;

  /**
   * Returns whether the container keeps its durability work. A durable container is private to the
   * class or test; a cluster has no durability work to skip, so only the scope differs.
   *
   * @return disposable by default: data on {@code tmpfs} and no syncing, where the service allows
   */
  Durability durability() default Durability.DISPOSABLE;
}
