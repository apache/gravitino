package org.apache.gravitino.connector.authorization;

import java.util.List;

public interface AuthorizationSecurableObject {
    String resource();

    List<String> privileges();
}
