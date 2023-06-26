package com.datastrato.graviton;

import java.util.Map;

public interface Lakehouse extends Auditable {

  String name();

  String comment();

  Map<String, String> properties();
}
