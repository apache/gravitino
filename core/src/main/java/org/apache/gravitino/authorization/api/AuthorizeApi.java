package org.apache.gravitino.authorization.api;

import org.apache.gravitino.authorization.Privilege;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AuthorizeApi {

    Privilege.Name privilege();

    String type();
}
