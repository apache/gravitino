package org.apache.gravitino.server.web.auth;

import com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.server.web.auth.annotations.AuthorizeApi;
import org.apache.gravitino.server.web.auth.annotations.AuthorizeResource;

public class AuthorizeInterceptor implements MethodInterceptor {

  @Override
  public Object invoke(MethodInvocation methodInvocation) throws Throwable {
    Method resourceMethod = methodInvocation.getMethod();
    AuthorizeApi authorizeApi = resourceMethod.getAnnotation(AuthorizeApi.class);
    if (authorizeApi == null) {
      return methodInvocation.proceed();
    }
    String privilege = authorizeApi.privilege();
    Parameter[] parameters = resourceMethod.getParameters();
    Object[] args = methodInvocation.getArguments();
    Map<String, Object> resourceContext = getResourceContext(parameters, args);
    Preconditions.checkArgument(
        GravitinoAuthorizerProvider.INSTANCE
            .getGravitinoAuthorizer()
            .authorize(
                "3606534323078438382",
                String.valueOf(resourceContext.get("metalake")),
                String.valueOf(resourceContext.get(authorizeApi.resourceType())),
                privilege),
        "has no permission");
    return methodInvocation.proceed();
  }

  private Map<String, Object> getResourceContext(Parameter[] parameters, Object[] args) {
    Map<String, Object> resourceContext = new HashMap<>();
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      AuthorizeResource authorizeResource = parameter.getAnnotation(AuthorizeResource.class);
      if (authorizeResource == null) {
        continue;
      }
      String resourceName = authorizeResource.value();
      resourceContext.put(resourceName, args[i]);
    }
    return resourceContext;
  }
}
