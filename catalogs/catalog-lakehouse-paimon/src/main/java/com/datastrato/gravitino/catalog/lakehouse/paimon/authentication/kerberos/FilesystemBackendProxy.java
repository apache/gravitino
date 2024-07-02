/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.kerberos;

import com.datastrato.gravitino.utils.PrincipalUtils;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

/**
 * Proxy class for FilesystemCatalog to support kerberos authentication. We can also make
 * FilesystemCatalog as a generic type and pass it as a parameter to the constructor.
 */
public class FilesystemBackendProxy implements MethodInterceptor {

  private FileSystemCatalog target;
  private final CatalogContext catalogContext;
  private final String kerberosRealm;
  private final UserGroupInformation proxyUser;

  public FilesystemBackendProxy(
      FileSystemCatalog target, CatalogContext catalogContext, String kerberosRealm) {
    this.target = target;
    this.catalogContext = catalogContext;
    this.kerberosRealm = kerberosRealm;
    try {
      proxyUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get current user", e);
    }
  }

  @Override
  public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy)
      throws Throwable {

    String proxyKerberosPrincipalName = PrincipalUtils.getCurrentPrincipal().getName();
    if (!proxyKerberosPrincipalName.contains("@")) {
      proxyKerberosPrincipalName =
          String.format("%s@%s", proxyKerberosPrincipalName, kerberosRealm);
    }

    UserGroupInformation realUser =
        UserGroupInformation.createProxyUser(proxyKerberosPrincipalName, proxyUser);

    return realUser.doAs(
        (PrivilegedExceptionAction<Object>)
            () -> {
              try {
                updateFileIO(catalogContext);
                return methodProxy.invoke(target, objects);
              } catch (Throwable e) {
                if (RuntimeException.class.isAssignableFrom(e.getClass())) {
                  throw (RuntimeException) e;
                }
                throw new RuntimeException("Failed to invoke method", e);
              }
            });
  }

  public FileSystemCatalog getProxy() {
    Enhancer e = new Enhancer();
    e.setClassLoader(target.getClass().getClassLoader());
    e.setSuperclass(target.getClass());
    e.setCallback(this);
    // FileSystemCatalog does not have a no argument constructor.
    return (FileSystemCatalog)
        e.create(
            new Class[] {FileIO.class, Path.class, Options.class},
            new Object[] {
              target.fileIO(), new Path(target.warehouse()), new Options(target.options())
            });
  }

  /**
   * Before invoking the doAs method, a Filesystem client has been created when initializing the
   * FilesystemCatalog. Therefore, the Filesystem client should be recreated within the doAs method
   * to ensure the ugi corresponds to the impersonated user.
   */
  private void updateFileIO(CatalogContext catalogContext)
      throws IOException, NoSuchFieldException, IllegalAccessException {

    String warehouse =
        Preconditions.checkNotNull(
            catalogContext.options().get(CatalogOptions.WAREHOUSE),
            String.format("Paimon %s path must be set.", CatalogOptions.WAREHOUSE.key()));
    Path warehousePath = new Path(warehouse);
    FileIO newFileIO = FileIO.get(warehousePath, catalogContext);

    Class<?> superclass = target.getClass().getSuperclass();
    Field oldFileIO = superclass.getDeclaredField("fileIO");
    oldFileIO.setAccessible(true);
    HadoopFileIO oldHadoopFileIO = (HadoopFileIO) oldFileIO.get(target);
    closeFileSystem(oldHadoopFileIO);
    oldFileIO.set(target, newFileIO);
  }

  private void closeFileSystem(HadoopFileIO hadoopFileIO)
      throws NoSuchFieldException, IllegalAccessException {
    Field fsMapField = hadoopFileIO.getClass().getDeclaredField("fsMap");
    fsMapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<Pair<String, String>, FileSystem> fsMap =
        (Map<Pair<String, String>, FileSystem>) fsMapField.get(hadoopFileIO);
    fsMap
        .values()
        .forEach(
            fs -> {
              if (fs != null) {
                try {
                  fs.close();
                } catch (IOException e) {
                  throw new RuntimeException("Failed to close Hadoop Filesystem client", e);
                }
              }
            });
  }
}
