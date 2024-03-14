/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import java.util.Map;

public interface User extends Auditable {

  String name();

  Map<String, String> properties();
}
