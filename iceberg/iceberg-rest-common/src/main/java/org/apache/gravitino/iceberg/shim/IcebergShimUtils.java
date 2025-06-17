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

package org.apache.gravitino.iceberg.shim;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.utils.ClassUtils;

public class IcebergShimUtils {
  private static boolean useModernIceberg;

  private static final String MODERN_CONFIG_PROVIDER =
      "org.apache.gravitino.iceberg.shim.IcebergModernConfigProvider";

  static {
    try {
      Class.forName(MODERN_CONFIG_PROVIDER, false, Thread.currentThread().getContextClassLoader());
      useModernIceberg = true;
    } catch (ClassNotFoundException e) {
      useModernIceberg = false;
    }
  }

  private static final String ICEBERG_REST_SPEC_PACKAGE =
      "org.apache.gravitino.iceberg.service.rest";

  private static final String MODERN_ICEBERG_REST_SPEC_PACKAGE =
      "org.apache.gravitino.iceberg.modern.service.rest";

  public List<String> getRESTPackages() {
    if (useModernIceberg()) {
      return Lists.newArrayList(ICEBERG_REST_SPEC_PACKAGE, MODERN_ICEBERG_REST_SPEC_PACKAGE);
    }
    return Lists.newArrayList(ICEBERG_REST_SPEC_PACKAGE);
  };

  public IcebergRESTConfigProvider getIcebergRESTConfigProvider() {
    if (useModernIceberg()) {
      return ClassUtils.loadClass(
          MODERN_CONFIG_PROVIDER, Thread.currentThread().getContextClassLoader());
    }

    return new IcebergLegacyConfigProvider();
  }

  @VisibleForTesting
  public boolean useModernIceberg() {
    return useModernIceberg;
  }
}
