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

package org.apache.gravitino.job;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URLClassLoader;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.CreateMetalakeEvent;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event listener that automatically registers built-in job templates when metalakes are created.
 *
 * <p>This listener monitors metalake creation events and registers all discovered built-in job
 * templates (via JobTemplateProvider SPI) into the newly created metalake. It also handles
 * registration for existing metalakes on first startup.
 */
public class BuiltInJobTemplateEventListener implements EventListenerPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(BuiltInJobTemplateEventListener.class);

  private static final Pattern BUILTIN_JOB_TEMPLATE_NAME_PATTERN =
      Pattern.compile(JobTemplateProvider.BUILTIN_NAME_PATTERN);
  private static final Pattern VERSION_PATTERN =
      Pattern.compile(JobTemplateProvider.VERSION_VALUE_PATTERN);

  private final JobManager jobManager;
  private final EntityStore entityStore;
  private final IdGenerator idGenerator;

  public BuiltInJobTemplateEventListener(
      JobManager jobManager, EntityStore entityStore, IdGenerator idGenerator) {
    this.jobManager = jobManager;
    this.entityStore = entityStore;
    this.idGenerator = idGenerator;
  }

  @Override
  public void init(Map<String, String> properties) throws RuntimeException {
    // Dependencies will be set via setDependencies() method
    // This is called from EventListenerManager before start()
  }

  @Override
  public void start() throws RuntimeException {
    // Register built-in job templates for all existing metalakes on first startup
    try {
      List<String> existingMetalakes = MetalakeManager.listInUseMetalakes(entityStore);
      if (existingMetalakes.isEmpty()) {
        return;
      }

      LOG.info(
          "Registering built-in job templates for {} existing metalakes", existingMetalakes.size());

      Map<String, JobTemplate> builtInTemplates = loadBuiltInJobTemplates();
      if (builtInTemplates.isEmpty()) {
        LOG.info("No built-in job templates discovered via JobTemplateProvider");
        return;
      }

      existingMetalakes.forEach(
          metalake -> {
            try {
              reconcileBuiltInJobTemplates(metalake, builtInTemplates);
            } catch (Exception e) {
              LOG.error("Failed to register built-in job templates for metalake: {}", metalake, e);
            }
          });

    } catch (Exception e) {
      LOG.error("Failed to register built-in job templates for existing metalakes", e);
    }
  }

  @Override
  public void stop() throws RuntimeException {
    // No resources to clean up
  }

  @Override
  public void onPostEvent(Event postEvent) throws RuntimeException {
    if (postEvent instanceof CreateMetalakeEvent) {
      CreateMetalakeEvent event = (CreateMetalakeEvent) postEvent;
      String metalakeName = event.identifier().name();

      try {
        Map<String, JobTemplate> builtInTemplates = loadBuiltInJobTemplates();
        if (builtInTemplates.isEmpty()) {
          LOG.debug("No built-in job templates to register for metalake: {}", metalakeName);
          return;
        }

        reconcileBuiltInJobTemplates(metalakeName, builtInTemplates);
        LOG.info("Registered built-in job templates for metalake: {}", metalakeName);
      } catch (Exception e) {
        LOG.error("Failed to register built-in job templates for metalake: {}", metalakeName, e);
      }
    }
  }

  @Override
  public Mode mode() {
    // Use async isolated to avoid blocking metalake creation
    return Mode.ASYNC_ISOLATED;
  }

  @VisibleForTesting
  Map<String, JobTemplate> loadBuiltInJobTemplates() {
    Map<String, JobTemplate> builtInTemplates = Maps.newHashMap();

    // Load from auxlib directory if available
    ClassLoader auxlibClassLoader = null;
    try {
      auxlibClassLoader = createAuxlibClassLoader();
      ServiceLoader<JobTemplateProvider> loader =
          ServiceLoader.load(JobTemplateProvider.class, auxlibClassLoader);

      loader.forEach(
          provider ->
              provider
                  .jobTemplates()
                  .forEach(
                      template -> {
                        if (!isValidBuiltInJobTemplate(template)) {
                          LOG.warn("Skip invalid built-in job template {}", template.name());
                          return;
                        }

                        JobTemplate existing = builtInTemplates.get(template.name());
                        int newVersion = version(template.customFields());
                        int existingVersion =
                            Optional.ofNullable(existing)
                                .map(jt -> version(jt.customFields()))
                                .orElse(0);
                        if (existing == null || newVersion > existingVersion) {
                          builtInTemplates.put(template.name(), template);
                        }
                      }));
      return builtInTemplates;

    } finally {
      if (auxlibClassLoader instanceof URLClassLoader) {
        try {
          ((URLClassLoader) auxlibClassLoader).close();
        } catch (IOException e) {
          LOG.warn("Failed to close auxlib classloader", e);
        }
      }
    }
  }

  private ClassLoader createAuxlibClassLoader() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    if (gravitinoHome == null) {
      LOG.warn("GRAVITINO_HOME not set, using current classloader for built-in job templates");
      return Thread.currentThread().getContextClassLoader();
    }

    File auxlibDir = new File(gravitinoHome, "auxlib");
    if (!auxlibDir.exists() || !auxlibDir.isDirectory()) {
      LOG.warn(
          "Auxlib directory {} does not exist, using current classloader for built-in job templates",
          auxlibDir.getAbsolutePath());
      return Thread.currentThread().getContextClassLoader();
    }

    // Only load gravitino-jobs-*.jar files
    File[] jarFiles =
        auxlibDir.listFiles(
            (dir, name) -> name.startsWith("gravitino-jobs-") && name.endsWith(".jar"));
    if (jarFiles == null || jarFiles.length == 0) {
      LOG.info(
          "No gravitino-jobs JAR files found in auxlib directory {}", auxlibDir.getAbsolutePath());
      return Thread.currentThread().getContextClassLoader();
    }

    try {
      URI[] jarUris = Arrays.stream(jarFiles).map(File::toURI).toArray(URI[]::new);
      LOG.info(
          "Loading built-in job templates from {} gravitino-jobs JAR file(s) in auxlib directory",
          jarUris.length);
      return new URLClassLoader(
          Arrays.stream(jarUris)
              .map(
                  uri -> {
                    try {
                      return uri.toURL();
                    } catch (MalformedURLException e) {
                      throw new RuntimeException("Failed to convert URI to URL: " + uri, e);
                    }
                  })
              .toArray(java.net.URL[]::new),
          Thread.currentThread().getContextClassLoader());
    } catch (Exception e) {
      LOG.error("Failed to create auxlib classloader", e);
      return Thread.currentThread().getContextClassLoader();
    }
  }

  private boolean isValidBuiltInJobTemplate(JobTemplate jobTemplate) {
    if (!isValidBuiltInName(jobTemplate.name())) {
      return false;
    }

    return getVersion(jobTemplate.customFields()).isPresent();
  }

  @VisibleForTesting
  void reconcileBuiltInJobTemplates(String metalake, Map<String, JobTemplate> builtInTemplates) {
    Map<String, JobTemplateEntity> existingBuiltIns =
        jobManager.listJobTemplates(metalake).stream()
            .filter(entity -> isValidBuiltInName(entity.name()))
            .collect(Collectors.toMap(JobTemplateEntity::name, entity -> entity));

    builtInTemplates.forEach(
        (name, newTemplate) -> {
          JobTemplateEntity existing = existingBuiltIns.remove(name);
          if (existing == null) {
            registerNewBuiltInJobTemplate(metalake, newTemplate);
          } else {
            int existingVersion = version(existing.templateContent().customFields());
            int newVersion = version(newTemplate.customFields());
            if (newVersion > existingVersion) {
              updateBuiltInJobTemplate(metalake, existing, newTemplate);
            } else {
              LOG.info("Built-in job template {} under metalake {} is up to date", name, metalake);
            }
          }
        });

    existingBuiltIns.values().forEach(entity -> deleteObsoleteBuiltInJobTemplate(metalake, entity));
  }

  private void registerNewBuiltInJobTemplate(String metalake, JobTemplate jobTemplate) {
    JobTemplateEntity jobTemplateEntity =
        JobTemplateEntity.builder()
            .withId(idGenerator.nextId())
            .withName(jobTemplate.name())
            .withNamespace(NamespaceUtil.ofJobTemplate(metalake))
            .withComment(jobTemplate.comment())
            .withTemplateContent(JobTemplateEntity.TemplateContent.fromJobTemplate(jobTemplate))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      jobManager.registerJobTemplate(metalake, jobTemplateEntity);
      LOG.info(
          "Registered built-in job template {} under metalake {}", jobTemplate.name(), metalake);
    } catch (JobTemplateAlreadyExistsException e) {
      LOG.warn(
          "Built-in job template {} already exists under metalake {}, skip registering",
          jobTemplate.name(),
          metalake,
          e);
    }
  }

  private void updateBuiltInJobTemplate(
      String metalake, JobTemplateEntity existing, JobTemplate newTemplate) {
    NameIdentifier identifier = NameIdentifierUtil.ofJobTemplate(metalake, newTemplate.name());

    TreeLockUtils.doWithTreeLock(
        identifier,
        LockType.WRITE,
        () -> {
          try {
            return entityStore.update(
                identifier,
                JobTemplateEntity.class,
                Entity.EntityType.JOB_TEMPLATE,
                jobTemplateEntity ->
                    JobTemplateEntity.builder()
                        .withId(jobTemplateEntity.id())
                        .withName(newTemplate.name())
                        .withNamespace(jobTemplateEntity.namespace())
                        .withComment(newTemplate.comment())
                        .withTemplateContent(
                            JobTemplateEntity.TemplateContent.fromJobTemplate(newTemplate))
                        .withAuditInfo(
                            AuditInfo.builder()
                                .withCreator(jobTemplateEntity.auditInfo().creator())
                                .withCreateTime(jobTemplateEntity.auditInfo().createTime())
                                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                                .withLastModifiedTime(Instant.now())
                                .build())
                        .build());
          } catch (NoSuchEntityException | IOException e) {
            throw new RuntimeException(
                String.format(
                    "Failed to update built-in job template %s under metalake %s",
                    newTemplate.name(), metalake),
                e);
          }
        });

    LOG.info(
        "Updated built-in job template {} under metalake {} from v{} to v{}",
        newTemplate.name(),
        metalake,
        version(existing.templateContent().customFields()),
        version(newTemplate.customFields()));
  }

  private void deleteObsoleteBuiltInJobTemplate(String metalake, JobTemplateEntity jobTemplate) {
    try {
      if (jobManager.deleteJobTemplate(metalake, jobTemplate.name())) {
        LOG.info(
            "Deleted obsolete built-in job template {} under metalake {}",
            jobTemplate.name(),
            metalake);
      }
    } catch (Exception e) {
      LOG.warn(
          "Failed to delete obsolete built-in job template {} under metalake {}",
          jobTemplate.name(),
          metalake,
          e);
    }
  }

  private boolean isValidBuiltInName(String name) {
    return BUILTIN_JOB_TEMPLATE_NAME_PATTERN.matcher(name).matches();
  }

  private Optional<Integer> getVersion(Map<String, String> customFields) {
    return Optional.ofNullable(customFields)
        .map(fields -> fields.get(JobTemplateProvider.PROPERTY_VERSION_KEY))
        .filter(StringUtils::isNotBlank)
        .flatMap(
            version -> {
              Matcher matcher = VERSION_PATTERN.matcher(version);
              if (!matcher.matches()) {
                return Optional.empty();
              }
              return Optional.of(Integer.parseInt(version.substring(1)));
            });
  }

  private int version(Map<String, String> customFields) {
    return getVersion(customFields).orElse(0);
  }
}
