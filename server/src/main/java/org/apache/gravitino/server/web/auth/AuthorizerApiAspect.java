package org.apache.gravitino.server.web.auth;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.server.web.auth.annotations.AuthorizeApi;
import org.apache.gravitino.server.web.auth.annotations.AuthorizeResource;

public class AuthorizerApiAspect implements InvocationHandler {

  GravitinoAuthorizer gravitinoAuthorizer =
      GravitinoAuthorizerProvider.INSTANCE.getGravitinoAuthorizer();

  private final Object target;

  public AuthorizerApiAspect(Object target) {
    this.target = target;
  }

  @Override
  public Object invoke(Object proxy, Method resourceMethod, Object[] args) throws Throwable {
    AuthorizeApi authorizeApi = resourceMethod.getAnnotation(AuthorizeApi.class);
    if (authorizeApi == null) {
      return resourceMethod.invoke(target, args);
    }
    String privilege = authorizeApi.privilege();
    Parameter[] parameters = resourceMethod.getParameters();
    Map<String, Object> resourceContext = getResourceContext(parameters, args);
    gravitinoAuthorizer.authorize(
        "3606534323078438382",
        String.valueOf(resourceContext.get("metalake")),
        String.valueOf(resourceContext.get(authorizeApi.resourceType())),
        privilege);
    return resourceMethod.invoke(target, args);
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

  public static <T> T createProxy(T target) {
    return (T)
        Proxy.newProxyInstance(
            target.getClass().getClassLoader(),
            target.getClass().getInterfaces(),
            new AuthorizerApiAspect(target));
  }
}
