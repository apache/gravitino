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
package org.apache.gravitino.hive.client.hive2;

import java.util.Properties;
import org.apache.gravitino.hive.client.HiveClientClassLoader;
import org.apache.gravitino.hive.client.HiveShim;

/**
 * Hive 2.x metastore shim. Hive 2.x has no multi-catalog support and no column constraints, so this
 * class adds nothing on top of {@link HiveShim}'s baseline behavior.
 */
public class HiveShimV2 extends HiveShim {

  public HiveShimV2(Properties properties) {
    super(HiveClientClassLoader.HiveVersion.HIVE2, properties);
  }
}
