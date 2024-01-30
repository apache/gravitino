/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.util;

import com.datastrato.gravitino.client.KerberosTokenProvider;

public class KerberosProviderHelper {

  private KerberosProviderHelper() {}

  private static KerberosTokenProvider provider;

  public static void setProvider(KerberosTokenProvider newProvider) {
    provider = newProvider;
  }

  public static KerberosTokenProvider getProvider() {
    return provider;
  }
}
