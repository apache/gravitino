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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.gravitino.integration.test.util.CloseContainerExtension;
import org.junit.jupiter.api.extension.AfterAllCallback;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <p>A fixture annotation on a {@link FixtureContainer} field picks that field's container instead
 * of the class's fixtures, so one class can hold two containers of one type.
 *
 * <p>By default ({@link Scope#RUN}) each container starts once per run and is shared by every class
 * that asks for the same spec; the containers close when the run ends, through {@link
 * CloseContainerExtension}. A {@link Scope#CLASS} container is private to the class and closes
 * after it; a {@link Scope#METHOD} container is private to one test and closes after it, so it can
 * only reach instance fields and the parameters of test and {@code @BeforeEach} methods.
 */
public class ContainerExtension
    implements AfterAllCallback,
        BeforeAllCallback,
        BeforeEachCallback,
        ParameterResolver,
        TestInstancePostProcessor {

  private static final String PRIVATE_CONTAINER = "privateContainer";
  private static final String INJECTED = "injectedSpecs";
  private static final Logger LOG = LoggerFactory.getLogger(ContainerExtension.class);

  // Where unused-fixture warnings go; a test swaps it to read them.
  static Consumer<String> warnings = LOG::warn;

  private final CloseContainerExtension suiteCloser = new CloseContainerExtension();

  /** A fixture on the test class: what it asks for and how long its container lives. */
  record Declaration(ContainerSpec<?> spec, Scope scope, Startup startup, Durability durability) {}

  @Override
  public void beforeAll(ExtensionContext context) {
    suiteCloser.beforeAll(context);
    Class<?> testClass = context.getRequiredTestClass();
    for (Declaration declaration : declarations(testClass)) {
      if (declaration.scope() != Scope.METHOD && declaration.startup() == Startup.EAGER) {
        container(declaration, context);
      }
    }
    injectFields(testClass, null, ModifierSupport::isStatic, context, false);
    // JUnit registers an extension declared only on an instance field after it post-processes the
    // instance, so postProcessTestInstance() never ran for it. A PER_CLASS instance exists here.
    context
        .getTestInstance()
        .ifPresent(
            instance ->
                injectFields(
                    instance.getClass(), instance, ModifierSupport::isNotStatic, context, false));
  }

  // A fixture whose container no field or parameter received still started; say so, since it
  // usually means a leftover fixture or a test that reaches the container another way.
  @Override
  public void afterAll(ExtensionContext context) {
    Set<ContainerSpec<?>> injected = injected(context);
    for (Declaration declaration : declarations(context.getRequiredTestClass())) {
      if (!injected.contains(declaration.spec())) {
        warnings.accept(
            String.format(
                "%s declares a fixture for %s but never injects its container; inject it into a"
                    + " field or parameter, or remove the fixture",
                context.getRequiredTestClass().getSimpleName(), declaration.spec()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Set<ContainerSpec<?>> injected(ExtensionContext context) {
    return classContext(context)
        .getStore(Namespace.create(ContainerExtension.class))
        .computeIfAbsent(INJECTED, key -> ConcurrentHashMap.newKeySet(), Set.class);
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    // An extension declared only on an instance field of a PER_METHOD class never gets
    // beforeAll(), so register the run's closer here too; registering twice is harmless.
    suiteCloser.beforeAll(context);
    Class<?> testClass = context.getRequiredTestClass();
    for (Declaration declaration : declarations(testClass)) {
      if (declaration.scope() == Scope.METHOD) {
        container(declaration, context);
      }
    }
    Object instance = context.getRequiredTestInstance();
    // An extension declared only on an instance field missed postProcessTestInstance().
    injectFields(testClass, instance, ModifierSupport::isNotStatic, context, false);
    // Each test gets a new METHOD container, so refill the fields that hold one.
    injectFields(testClass, instance, ModifierSupport::isNotStatic, context, true);
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
          fieldDeclaration(field)
              .orElseGet(
                  () -> declarationFor(field.getType(), testClass, "field " + field.getName()));
      if (declaration.scope() == Scope.METHOD && ModifierSupport.isStatic(field)) {
        throw methodScopeOutsideTest(testClass, declaration.spec());
      }
      if ((declaration.scope() == Scope.METHOD) != methodScoped) {
        continue;
      }
      try {
        field.setAccessible(true);
        // A RUN or CLASS container does not change within an instance, so a filled instance field
        // stays; several callbacks below may each be the first to see the instance.
        if (!methodScoped && testInstance != null && field.get(testInstance) != null) {
          continue;
        }
        field.set(testInstance, inject(declaration, field.getType(), context));
        injected(context).add(declaration.spec());
      } catch (IllegalAccessException e) {
        throw new ExtensionConfigurationException("Cannot set field " + field.getName(), e);
      }
    }
  }

  // A fixture annotation on the field itself, which wins over the class's fixtures.
  private static Optional<Declaration> fieldDeclaration(Field field) {
    List<Declaration> found = new ArrayList<>();
    for (Annotation annotation : field.getDeclaredAnnotations()) {
      ContainerFixture fixture = annotation.annotationType().getAnnotation(ContainerFixture.class);
      if (fixture != null) {
        found.add(declare(fixture, annotation));
      }
    }
    if (found.size() > 1) {
      throw new ExtensionConfigurationException(
          String.format(
              "%s: field %s has %d fixture annotations; use one",
              field.getDeclaringClass().getSimpleName(), field.getName(), found.size()));
    }
    if (found.isEmpty()) {
      return Optional.empty();
    }
    Class<?> containerType = found.get(0).spec().containerType();
    if (!found.get(0).spec().supplies(field.getType())) {
      throw new ExtensionConfigurationException(
          String.format(
              "%s: field %s of type %s cannot hold a %s",
              field.getDeclaringClass().getSimpleName(),
              field.getName(),
              field.getType().getSimpleName(),
              containerType.getSimpleName()));
    }
    return Optional.of(found.get(0));
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
    injected(context).add(declaration.spec());
    return inject(declaration, parameter.getParameter().getType(), context);
  }

  // The running container, as the type the field or parameter asks for.
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Object inject(Declaration declaration, Class<?> wanted, ExtensionContext context) {
    ContainerSpec spec = declaration.spec();
    return spec.injectAs(wanted, container(declaration, context));
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
    ContainerSpec<?> spec =
        context
            .getTestInstance()
            .<ContainerSpec<?>>map(instance -> declaration.spec().forTestInstance(instance))
            .orElse(declaration.spec());
    if (declaration.scope() == Scope.RUN) {
      return ContainerSpec.startWithDependencies(spec, ContainerSuite.getInstance());
    }
    ExtensionContext owner =
        declaration.scope() == Scope.CLASS ? classContext(context) : methodContext(context, spec);
    return owner
        .getStore(Namespace.create(ContainerExtension.class, spec))
        .computeIfAbsent(
            PRIVATE_CONTAINER,
            key -> {
              ContainerSpec.startDependencies(spec, ContainerSuite.getInstance());
              return spec.startPrivate(
                  ContainerSuite.getInstance(),
                  // -PdurableTestContainers makes every container durable.
                  Durability.runDefault() == Durability.DURABLE
                      ? Durability.DURABLE
                      : declaration.durability());
            },
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
      if (declaration.spec().supplies(wanted)) {
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
    Declaration declaration =
        new Declaration(
            factory.specFor(annotation),
            factory.scopeFor(annotation),
            factory.startupFor(annotation),
            factory.durabilityFor(annotation));
    if (declaration.scope() != Scope.RUN && !declaration.spec().supportsPrivate()) {
      throw new ExtensionConfigurationException(
          String.format(
              "@%s does not support scope = Scope.%s yet; today only @HiveFixture with HDFS"
                  + " storage and no Ranger has containers of its own",
              annotation.annotationType().getSimpleName(), declaration.scope()));
    }
    // A shared container is disposable for every class; durable data needs a container of its own.
    if (declaration.durability() == Durability.DURABLE
        && declaration.scope() == Scope.RUN
        && Durability.runDefault() == Durability.DISPOSABLE) {
      throw new ExtensionConfigurationException(
          String.format(
              declaration.spec().supportsPrivate()
                  ? "@%s(durability = DURABLE) needs a container of its own: add scope ="
                      + " Scope.CLASS or Scope.METHOD"
                  : "@%s(durability = DURABLE) needs a container of its own, which only"
                      + " @HiveFixture with HDFS storage supports today",
              annotation.annotationType().getSimpleName()));
    }
    return declaration;
  }
}
