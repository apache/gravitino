package org.apache.gravitino.authorization;

import java.util.Map;
import java.util.stream.Collectors;

public abstract class AuthorizationProperties {
  protected Map<String, String> properties;

  public AuthorizationProperties(Map<String, String> properties) {
    this.properties =
        properties.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(getPropertiesPrefix()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  abstract String getPropertiesPrefix();

  abstract void validate();

  public static void validate(String type, Map<String, String> properties) {
    switch (type) {
      case "ranger":
        RangerAuthorizationProperties rangerAuthorizationProperties =
            new RangerAuthorizationProperties(properties);
        rangerAuthorizationProperties.validate();
        break;
      case "chain":
        ChainAuthorizationProperties chainAuthorizationProperties =
            new ChainAuthorizationProperties(properties);
        chainAuthorizationProperties.validate();
        break;
      default:
        throw new IllegalArgumentException("Unsupported authorization properties type: " + type);
    }
  }
}
