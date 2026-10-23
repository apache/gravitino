/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.gravitino.integration.test.util.CloseContainerExtension;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ModifierSupport;

/**
 * Starts the containers a test class declares with fixture annotations, such as {@link
 * KafkaFixture}, and passes them to test and lifecycle methods by parameter type and to fields
 * marked {@link FixtureContainer}.
 *
 * <p>This is dependency injection built on JUnit's own extension points ({@link ParameterResolver}
 * and {@link TestInstancePostProcessor}), so tests need no DI framework.
 *
 * <p>Fixtures on superclasses apply too, but a class's fixture replaces a superclass's fixture of
 * the same annotation type, so {@code @HiveFixture(version = HIVE3)} on a subclass of a Hive2 test
 * runs Hive3 only.
 *
 * <p>Each container starts once per run and is shared by every class that asks for the same spec.
 * The containers close when the run ends, through {@link CloseContainerExtension}.
 */
public class ContainerExtension
    implements BeforeAllCallback, ParameterResolver, TestInstancePostProcessor {

  private final CloseContainerExtension suiteCloser = new CloseContainerExtension();

  @Override
  public void beforeAll(ExtensionContext context) {
    suiteCloser.beforeAll(context);
    Class<?> testClass = context.getRequiredTestClass();
    for (ContainerSpec<?> spec : specs(testClass)) {
      spec.getOrStart(ContainerSuite.getInstance());
    }
    injectFields(testClass, null, ModifierSupport::isStatic);
  }

  // Runs when the instance is created, before beforeAll(), so instance fields are filled before an
  // overridden @BeforeAll method such as BaseIT.startIntegrationTest() runs.
  @Override
  public void postProcessTestInstance(Object testInstance, ExtensionContext context) {
    injectFields(testInstance.getClass(), testInstance, ModifierSupport::isNotStatic);
  }

  private static void injectFields(
      Class<?> testClass, @Nullable Object testInstance, Predicate<Field> which) {
    for (Field field :
        AnnotationSupport.findAnnotatedFields(
            testClass, FixtureContainer.class, which, HierarchyTraversalMode.TOP_DOWN)) {
      if (ModifierSupport.isFinal(field)) {
        throw new ExtensionConfigurationException(
            String.format(
                "%s: @FixtureContainer field %s must not be final",
                testClass.getSimpleName(), field.getName()));
      }
      try {
        field.setAccessible(true);
        field.set(testInstance, resolve(field.getType(), testClass, "field " + field.getName()));
      } catch (IllegalAccessException e) {
        throw new ExtensionConfigurationException("Cannot set field " + field.getName(), e);
      }
    }
  }

  @Override
  public boolean supportsParameter(ParameterContext parameter, ExtensionContext context) {
    return !matching(parameter.getParameter().getType(), context.getRequiredTestClass()).isEmpty();
  }

  @Override
  public Object resolveParameter(ParameterContext parameter, ExtensionContext context) {
    return resolve(
        parameter.getParameter().getType(),
        context.getRequiredTestClass(),
        "parameter " + parameter.getParameter().getName());
  }

  private static Object resolve(Class<?> wanted, Class<?> testClass, String target) {
    List<ContainerSpec<?>> matches = matching(wanted, testClass);
    if (matches.size() != 1) {
      throw new ParameterResolutionException(
          String.format(
              "%s: %s of type %s matches %d fixtures; declare exactly one fixture for it",
              testClass.getSimpleName(), target, wanted.getSimpleName(), matches.size()));
    }
    return matches.get(0).getOrStart(ContainerSuite.getInstance());
  }

  private static List<ContainerSpec<?>> matching(Class<?> wanted, Class<?> testClass) {
    List<ContainerSpec<?>> matches = new ArrayList<>();
    for (ContainerSpec<?> spec : specs(testClass)) {
      if (wanted.isAssignableFrom(spec.containerType())) {
        matches.add(spec);
      }
    }
    return matches;
  }

  // Fixtures on the class and its superclasses, nearest class first, one per annotation type.
  // Hierarchies such as TestHive3HMS extends TestHive2HMS re-declare a fixture with another
  // version; keeping both would start two containers and make injection by type ambiguous.
  static List<ContainerSpec<?>> specs(Class<?> testClass) {
    List<ContainerSpec<?>> specs = new ArrayList<>();
    Set<Class<? extends Annotation>> seen = new HashSet<>();
    for (Class<?> c = testClass; c != null && c != Object.class; c = c.getSuperclass()) {
      for (Annotation annotation : c.getDeclaredAnnotations()) {
        ContainerFixture fixture =
            annotation.annotationType().getAnnotation(ContainerFixture.class);
        if (fixture != null && seen.add(annotation.annotationType())) {
          specs.add(specFor(fixture, annotation));
        }
      }
    }
    return specs;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static ContainerSpec<?> specFor(ContainerFixture fixture, Annotation annotation) {
    try {
      Constructor<? extends ContainerFixture.SpecFactory<?>> constructor =
          fixture.value().getDeclaredConstructor();
      constructor.setAccessible(true);
      return ((ContainerFixture.SpecFactory) constructor.newInstance()).specFor(annotation);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Cannot create the spec factory " + fixture.value().getName(), e);
    }
  }
}
