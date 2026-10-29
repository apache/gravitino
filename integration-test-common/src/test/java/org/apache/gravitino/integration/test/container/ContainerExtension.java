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
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
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
 * <p>By default ({@link Scope#RUN}) each container starts once per run and is shared by every class
 * that asks for the same spec; the containers close when the run ends, through {@link
 * CloseContainerExtension}. A {@link Scope#CLASS} container is private to the class and closes
 * after it; a {@link Scope#METHOD} container is private to one test and closes after it, so it can
 * only reach instance fields and the parameters of test and {@code @BeforeEach} methods.
 */
public class ContainerExtension
    implements BeforeAllCallback, BeforeEachCallback, ParameterResolver, TestInstancePostProcessor {

  private static final String PRIVATE_CONTAINER = "privateContainer";

  private final CloseContainerExtension suiteCloser = new CloseContainerExtension();

  /** A fixture on the test class: what it asks for and how long its container lives. */
  record Declaration(ContainerSpec<?> spec, Scope scope) {}

  @Override
  public void beforeAll(ExtensionContext context) {
    suiteCloser.beforeAll(context);
    Class<?> testClass = context.getRequiredTestClass();
    for (Declaration declaration : declarations(testClass)) {
      if (declaration.scope() != Scope.METHOD) {
        container(declaration, context);
      }
    }
    injectFields(testClass, null, ModifierSupport::isStatic, context, false);
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();
    for (Declaration declaration : declarations(testClass)) {
      if (declaration.scope() == Scope.METHOD) {
        container(declaration, context);
      }
    }
    // Each test gets a new METHOD container, so refill the fields that hold one.
    injectFields(
        testClass, context.getRequiredTestInstance(), ModifierSupport::isNotStatic, context, true);
  }

  // Runs when the instance is created, before beforeAll(), so instance fields are filled before an
  // overridden @BeforeAll method such as BaseIT.startIntegrationTest() runs.
  @Override
  public void postProcessTestInstance(Object testInstance, ExtensionContext context) {
    injectFields(
        testInstance.getClass(), testInstance, ModifierSupport::isNotStatic, context, false);
  }

  // Fills the matching fields whose container is METHOD scoped (methodScoped) or not.
  private static void injectFields(
      Class<?> testClass,
      @Nullable Object testInstance,
      Predicate<Field> which,
      ExtensionContext context,
      boolean methodScoped) {
    for (Field field :
        AnnotationSupport.findAnnotatedFields(
            testClass, FixtureContainer.class, which, HierarchyTraversalMode.TOP_DOWN)) {
      if (ModifierSupport.isFinal(field)) {
        throw new ExtensionConfigurationException(
            String.format(
                "%s: @FixtureContainer field %s must not be final",
                testClass.getSimpleName(), field.getName()));
      }
      Declaration declaration =
          declarationFor(field.getType(), testClass, "field " + field.getName());
      if (declaration.scope() == Scope.METHOD && ModifierSupport.isStatic(field)) {
        throw methodScopeOutsideTest(testClass, declaration.spec());
      }
      if ((declaration.scope() == Scope.METHOD) != methodScoped) {
        continue;
      }
      try {
        field.setAccessible(true);
        field.set(testInstance, container(declaration, context));
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
    Declaration declaration =
        declarationFor(
            parameter.getParameter().getType(),
            context.getRequiredTestClass(),
            "parameter " + parameter.getParameter().getName());
    return container(declaration, context);
  }

  private static Declaration declarationFor(Class<?> wanted, Class<?> testClass, String target) {
    List<Declaration> matches = matching(wanted, testClass);
    if (matches.size() != 1) {
      throw new ParameterResolutionException(
          String.format(
              "%s: %s of type %s matches %d fixtures; declare exactly one fixture for it",
              testClass.getSimpleName(), target, wanted.getSimpleName(), matches.size()));
    }
    return matches.get(0);
  }

  // A private container lives in the store of the class or test that owns it; it is AutoCloseable,
  // so JUnit closes it with that context. Stores inherit from parent contexts, so a CLASS container
  // also resolves inside the class's tests.
  private static BaseContainer container(Declaration declaration, ExtensionContext context) {
    ContainerSpec<?> spec = declaration.spec();
    if (declaration.scope() == Scope.RUN) {
      return spec.getOrStart(ContainerSuite.getInstance());
    }
    ExtensionContext owner =
        declaration.scope() == Scope.CLASS ? classContext(context) : methodContext(context, spec);
    return owner
        .getStore(Namespace.create(ContainerExtension.class, spec))
        .computeIfAbsent(
            PRIVATE_CONTAINER,
            key -> spec.startPrivate(ContainerSuite.getInstance()),
            BaseContainer.class);
  }

  // Instance post-processing and test methods may run in a method context; a CLASS container
  // belongs to the class.
  private static ExtensionContext classContext(ExtensionContext context) {
    ExtensionContext current = context;
    while (current.getTestMethod().isPresent()) {
      current = current.getParent().orElseThrow();
    }
    return current;
  }

  private static ExtensionContext methodContext(ExtensionContext context, ContainerSpec<?> spec) {
    if (context.getTestMethod().isEmpty()) {
      throw methodScopeOutsideTest(context.getRequiredTestClass(), spec);
    }
    return context;
  }

  private static ExtensionConfigurationException methodScopeOutsideTest(
      Class<?> testClass, ContainerSpec<?> spec) {
    return new ExtensionConfigurationException(
        String.format(
            "%s: %s has Scope.METHOD, so only instance fields and test and @BeforeEach methods"
                + " can receive its container",
            testClass.getSimpleName(), spec));
  }

  private static List<Declaration> matching(Class<?> wanted, Class<?> testClass) {
    List<Declaration> matches = new ArrayList<>();
    for (Declaration declaration : declarations(testClass)) {
      if (wanted.isAssignableFrom(declaration.spec().containerType())) {
        matches.add(declaration);
      }
    }
    return matches;
  }

  static List<ContainerSpec<?>> specs(Class<?> testClass) {
    return declarations(testClass).stream().map(Declaration::spec).toList();
  }

  // Fixtures on the class and its superclasses, nearest class first, one per annotation type.
  // Hierarchies such as TestHive3HMS extends TestHive2HMS re-declare a fixture with another
  // version; keeping both would start two containers and make injection by type ambiguous.
  private static List<Declaration> declarations(Class<?> testClass) {
    List<Declaration> declarations = new ArrayList<>();
    Set<Class<? extends Annotation>> seen = new HashSet<>();
    for (Class<?> c = testClass; c != null && c != Object.class; c = c.getSuperclass()) {
      for (Annotation annotation : c.getDeclaredAnnotations()) {
        ContainerFixture fixture =
            annotation.annotationType().getAnnotation(ContainerFixture.class);
        if (fixture != null && seen.add(annotation.annotationType())) {
          declarations.add(declare(fixture, annotation));
        }
      }
    }
    return declarations;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Declaration declare(ContainerFixture fixture, Annotation annotation) {
    ContainerFixture.SpecFactory factory;
    try {
      Constructor<? extends ContainerFixture.SpecFactory<?>> constructor =
          fixture.value().getDeclaredConstructor();
      constructor.setAccessible(true);
      factory = constructor.newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Cannot create the spec factory " + fixture.value().getName(), e);
    }
    return new Declaration(factory.specFor(annotation), factory.scopeFor(annotation));
  }
}
