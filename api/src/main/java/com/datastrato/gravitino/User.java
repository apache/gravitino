package com.datastrato.gravitino;

import java.util.Map;

public interface User extends Auditable {

    String name();

    Map<String, String> properties();
}
