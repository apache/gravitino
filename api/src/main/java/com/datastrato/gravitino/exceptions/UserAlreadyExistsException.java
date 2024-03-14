package com.datastrato.gravitino.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

public class UserAlreadyExistsException extends AlreadyExistsException {


    @FormatMethod
    public UserAlreadyExistsException(@FormatString String message, Object... args) {
        super(message, args);
    }

    @FormatMethod
    public UserAlreadyExistsException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }
}
