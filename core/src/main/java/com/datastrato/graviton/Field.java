/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/

package com.datastrato.graviton;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class Field {
  // The name of the field
  private String fieldName;

  // The type class of the field
  private Class<?> typeClass;

  // The detailed description of the field
  private String description;

  // Whether this field is optional or required
  private boolean optional;

  private Field() {}

  public static Field required(String fieldName, Class<?> typeClass, String description) {
    return new Builder(false)
        .withName(fieldName)
        .withTypeClass(typeClass)
        .withDescription(description)
        .build();
  }

  public static Field optional(String fieldName, Class<?> typeClass, String description) {
    return new Builder(true)
        .withName(fieldName)
        .withTypeClass(typeClass)
        .withDescription(description)
        .build();
  }

  public static Field required(String fieldName, Class<?> typeClass) {
    return new Builder(false).withName(fieldName).withTypeClass(typeClass).build();
  }

  public static Field optional(String fieldName, Class<?> typeClass) {
    return new Builder(true).withName(fieldName).withTypeClass(typeClass).build();
  }

  public <T> void validate(T fieldValue) {
    if (fieldValue == null && !optional) {
      throw new IllegalArgumentException("Field " + fieldName + " is required");
    }

    if (fieldValue != null && !typeClass.isAssignableFrom(fieldValue.getClass())) {
      throw new IllegalArgumentException(
          "Field " + fieldName + " is not of type " + typeClass.getName());
    }
  }

  public static class Builder {
    private final Field field;

    public Builder(boolean isOptional) {
      field = new Field();
      field.optional = isOptional;
    }

    public Builder withName(String name) {
      field.fieldName = name;
      return this;
    }

    public Builder withTypeClass(Class<?> typeClass) {
      field.typeClass = typeClass;
      return this;
    }

    public Builder withDescription(String description) {
      field.description = description;
      return this;
    }

    public Field build() {
      if (field.fieldName == null) {
        throw new IllegalArgumentException("Field name is required");
      }

      if (field.typeClass == null) {
        throw new IllegalArgumentException("Field type class is required");
      }

      return field;
    }
  }
}
