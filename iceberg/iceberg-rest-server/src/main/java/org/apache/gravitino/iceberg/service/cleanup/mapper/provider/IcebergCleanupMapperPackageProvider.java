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

package org.apache.gravitino.iceberg.service.cleanup.mapper.provider;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.gravitino.iceberg.service.cleanup.mapper.IcebergCleanupJobMapper;
import org.apache.gravitino.storage.relational.mapper.provider.MapperPackageProvider;

/**
 * Lists the Iceberg async-cleanup mappers so the cleanup job store can register them into the
 * Gravitino entity store's shared MyBatis configuration and reuse the shared relational backend
 * rather than opening its own JDBC connections.
 *
 * <p>This implements {@link MapperPackageProvider} but is invoked directly (not via {@link
 * java.util.ServiceLoader}): in deploy mode the iceberg-rest-server runs in an isolated
 * auxiliary-service class loader that core's service lookup cannot see into, so {@link
 * org.apache.gravitino.iceberg.service.cleanup.IcebergCleanupJobStore} consults this provider when
 * it registers the mappers.
 */
public class IcebergCleanupMapperPackageProvider implements MapperPackageProvider {

  @Override
  public List<Class<?>> getMapperClasses() {
    return ImmutableList.of(IcebergCleanupJobMapper.class);
  }
}
