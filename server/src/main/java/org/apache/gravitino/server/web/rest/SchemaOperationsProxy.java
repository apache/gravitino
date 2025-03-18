package org.apache.gravitino.server.web.rest;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.server.web.auth.GravitinoAuthorizer;
import org.apache.gravitino.server.web.auth.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.web.auth.annotations.AuthorizeApi;
import org.apache.gravitino.server.web.auth.annotations.AuthorizeResource;
import org.glassfish.hk2.api.Factory;

/** Use Cglib to implement dynamic proxy to call GravitinoAuthorizer for authentication */
public class SchemaOperationsProxy implements Factory<SchemaOperations> {

  private final SchemaDispatcher dispatcher;

  @Inject
  public SchemaOperationsProxy(SchemaDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  GravitinoAuthorizer gravitinoAuthorizer =
      GravitinoAuthorizerProvider.INSTANCE.getGravitinoAuthorizer();

  @Override
  public SchemaOperations provide() {
    Enhancer enhancer = new Enhancer();
    enhancer.setSuperclass(SchemaOperations.class);
    enhancer.setCallback(
        new MethodInterceptor() {
          @Override
          public Object intercept(
              Object obj, Method resourceMethod, Object[] args, MethodProxy proxy)
              throws Throwable {
            AuthorizeApi authorizeApi = resourceMethod.getAnnotation(AuthorizeApi.class);
            if (authorizeApi == null) {
              return proxy.invokeSuper(obj, args);
            }
            String privilege = authorizeApi.privilege();
            Parameter[] parameters = resourceMethod.getParameters();
            Map<String, Object> resourceContext = getResourceContext(parameters, args);
            gravitinoAuthorizer.authorize(
                "3606534323078438382",
                String.valueOf(resourceContext.get("metalake")),
                String.valueOf(resourceContext.get(authorizeApi.resourceType())),
                privilege);
            return proxy.invokeSuper(obj, args);
          }
        });
    return (SchemaOperations)
        enhancer.create(new Class[] {SchemaDispatcher.class}, new Object[] {dispatcher});
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

  @Override
  public void dispose(SchemaOperations schemaOperations) {}
}
