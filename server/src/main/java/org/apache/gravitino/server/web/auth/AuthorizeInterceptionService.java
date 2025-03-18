package org.apache.gravitino.server.web.auth;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.ext.Provider;
import org.aopalliance.intercept.ConstructorInterceptor;
import org.aopalliance.intercept.MethodInterceptor;
import org.apache.gravitino.server.web.rest.SchemaOperations;
import org.glassfish.hk2.api.Filter;
import org.glassfish.hk2.api.InterceptionService;
import org.glassfish.hk2.utilities.BuilderHelper;

@Provider
public class AuthorizeInterceptionService implements InterceptionService {

  @Override
  public Filter getDescriptorFilter() {
    return BuilderHelper.createContractFilter(SchemaOperations.class.getName());
  }

  @Override
  public List<MethodInterceptor> getMethodInterceptors(Method method) {
    return Collections.singletonList(new AuthorizeInterceptor());
  }

  @Override
  public List<ConstructorInterceptor> getConstructorInterceptors(Constructor<?> constructor) {
    return Collections.emptyList();
  }
}
