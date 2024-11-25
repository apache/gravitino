package org.apache.gravitino.connector.authorization;

import org.apache.gravitino.authorization.SecurableObject;

import java.util.List;

public interface SupportsConverter {
    List<AuthorizationSecurableObject> convertToAuthorizationObject(ExtendedSecurableObject object);
}
