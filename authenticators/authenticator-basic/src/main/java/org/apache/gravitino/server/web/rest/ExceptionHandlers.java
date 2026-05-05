/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.server.web.rest;

import com.google.common.annotations.VisibleForTesting;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.server.web.Utils;

public class ExceptionHandlers {

  private ExceptionHandlers() {}

  public static Response handleUserException(OperationType op, String user, Exception e) {
    return UserExceptionHandler.INSTANCE.handle(op, user, null, e);
  }

  public static Response handleGroupException(OperationType op, String group, Exception e) {
    return GroupExceptionHandler.INSTANCE.handle(op, group, null, e);
  }

  private static class UserExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new UserExceptionHandler();

    private static String getUserErrorMsg(
        String user, String operation, String parent, String reason) {
      if (parent == null) {
        return String.format(
            "Failed to operate built-in IdP user%s operation [%s], reason [%s]",
            user, operation, reason);
      } else {
        return String.format(
            "Failed to operate user%s operation [%s] under [%s], reason [%s]",
            user, operation, parent, reason);
      }
    }

    @Override
    public Response handle(OperationType op, String user, String parent, Exception e) {
      String formatted = StringUtils.isBlank(user) ? "" : " [" + user + "]";
      String errorMsg = getUserErrorMsg(formatted, op.name(), parent, getErrorMsg(e));

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NoSuchUserException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof UserAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof ForbiddenException) {
        return Utils.forbidden(errorMsg, e);

      } else if (e instanceof UnsupportedOperationException) {
        return Utils.unsupportedOperation(errorMsg, e);

      } else {
        return super.handle(op, user, parent, e);
      }
    }
  }

  private static class GroupExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new GroupExceptionHandler();

    private static String getGroupErrorMsg(
        String group, String operation, String parent, String reason) {
      if (parent == null) {
        return String.format(
            "Failed to operate built-in IdP group%s operation [%s], reason [%s]",
            group, operation, reason);
      } else {
        return String.format(
            "Failed to operate group%s operation [%s] under [%s], reason [%s]",
            group, operation, parent, reason);
      }
    }

    @Override
    public Response handle(OperationType op, String group, String parent, Exception e) {
      String formatted = StringUtils.isBlank(group) ? "" : " [" + group + "]";
      String errorMsg = getGroupErrorMsg(formatted, op.name(), parent, getErrorMsg(e));

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NoSuchGroupException || e instanceof NoSuchUserException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof GroupAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof ForbiddenException) {
        return Utils.forbidden(errorMsg, e);

      } else if (e instanceof UnsupportedOperationException) {
        return Utils.unsupportedOperation(errorMsg, e);

      } else {
        return super.handle(op, group, parent, e);
      }
    }
  }

  @VisibleForTesting
  static class BaseExceptionHandler extends ExceptionHandler {

    private static final String EXCEPTION_KEYWORD = "Exception: ";

    private static String getBaseErrorMsg(
        String object, String operation, String parent, String reason) {
      return String.format(
          "Failed to operate object%s operation [%s]%s, reason [%s]",
          object, operation, parent, reason);
    }

    @Override
    public Response handle(OperationType op, String object, String parent, Exception e) {
      String formattedObject = StringUtils.isBlank(object) ? "" : " [" + object + "]";
      String formattedParent = StringUtils.isBlank(parent) ? "" : " under [" + parent + "]";
      String errorMsg =
          getBaseErrorMsg(formattedObject, op.name(), formattedParent, getErrorMsg(e));
      return Utils.internalError(errorMsg, e);
    }

    @VisibleForTesting
    static String getErrorMsg(Throwable throwable) {
      if (throwable == null || throwable.getMessage() == null) {
        return "";
      }

      String message = throwable.getMessage();
      int pos = message.lastIndexOf(EXCEPTION_KEYWORD);
      if (pos == -1) {
        return message;
      } else {
        return message.substring(pos + EXCEPTION_KEYWORD.length());
      }
    }
  }
}
