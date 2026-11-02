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

package org.apache.gravitino.connector.job;

import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.utils.FileFetcher;

/**
 * Utilities to fetch the resources of a runtime job template, that is its executable, scripts,
 * jars, files and archives, into a local directory. Job executors that run jobs on the Gravitino
 * server use {@link #localizeJobTemplate} to get a job template whose resources are local files
 * before launching a job.
 */
@DeveloperApi
public final class JobResourceUtils {

  /** The connect and read timeout in milliseconds used to fetch a remote file. */
  public static final int DEFAULT_FETCH_TIMEOUT_IN_MS = 30 * 1000;

  private JobResourceUtils() {}

  /**
   * Localizes a runtime job template: fetches all its resources into a directory, and returns a
   * copy of the job template whose resources point to the fetched local files. The other fields of
   * the job template are kept as they are.
   *
   * @param jobTemplate the runtime job template, whose resources are the URIs to fetch
   * @param dir the directory to fetch the resources into
   * @return the localized job template, whose resources are the paths of the fetched local files
   * @throws IllegalArgumentException if a resource has no file name, two different resources have
   *     the same file name, or a script has the file name of a command-name executable. Nothing is
   *     fetched in these cases.
   * @throws RuntimeException if a resource cannot be fetched
   */
  public static JobTemplate localizeJobTemplate(JobTemplate jobTemplate, File dir) {
    checkFileNames(jobTemplate);

    if (jobTemplate instanceof ShellJobTemplate) {
      ShellJobTemplate shellJobTemplate = (ShellJobTemplate) jobTemplate;
      // A command name is looked up in the environment the job runs in, not fetched.
      String executable =
          isCommandName(shellJobTemplate.executable())
              ? shellJobTemplate.executable()
              : fetchFile(shellJobTemplate.executable(), dir, DEFAULT_FETCH_TIMEOUT_IN_MS);
      return ShellJobTemplate.builder()
          .withName(shellJobTemplate.name())
          .withComment(shellJobTemplate.comment())
          .withExecutable(executable)
          .withArguments(shellJobTemplate.arguments())
          .withEnvironments(shellJobTemplate.environments())
          .withCustomFields(shellJobTemplate.customFields())
          .withScripts(fetchFiles(shellJobTemplate.scripts(), dir, DEFAULT_FETCH_TIMEOUT_IN_MS))
          .build();
    }

    if (jobTemplate instanceof SparkJobTemplate) {
      SparkJobTemplate sparkJobTemplate = (SparkJobTemplate) jobTemplate;
      return SparkJobTemplate.builder()
          .withName(sparkJobTemplate.name())
          .withComment(sparkJobTemplate.comment())
          .withExecutable(
              fetchFile(sparkJobTemplate.executable(), dir, DEFAULT_FETCH_TIMEOUT_IN_MS))
          .withArguments(sparkJobTemplate.arguments())
          .withEnvironments(sparkJobTemplate.environments())
          .withCustomFields(sparkJobTemplate.customFields())
          .withClassName(sparkJobTemplate.className())
          .withJars(fetchFiles(sparkJobTemplate.jars(), dir, DEFAULT_FETCH_TIMEOUT_IN_MS))
          .withFiles(fetchFiles(sparkJobTemplate.files(), dir, DEFAULT_FETCH_TIMEOUT_IN_MS))
          .withArchives(fetchFiles(sparkJobTemplate.archives(), dir, DEFAULT_FETCH_TIMEOUT_IN_MS))
          .withConfigs(sparkJobTemplate.configs())
          .build();
    }

    throw new IllegalArgumentException("Unsupported job type: " + jobTemplate.jobType());
  }

  /**
   * Whether the executable of a shell job template is a command name, such as {@code python}: a
   * name with no scheme and no path separator, which is looked up in the environment the job runs
   * in, for example on the {@code PATH} of the process that launches it, instead of a file to
   * fetch.
   *
   * @param executable the executable of a shell job template
   * @return true if the executable is a command name
   */
  public static boolean isCommandName(String executable) {
    if (StringUtils.isBlank(executable)
        || executable.equals(".")
        || executable.equals("..")
        || executable.indexOf('/') >= 0
        || executable.indexOf(File.separatorChar) >= 0) {
      return false;
    }
    try {
      return new URI(executable).getScheme() == null;
    } catch (URISyntaxException e) {
      return false;
    }
  }

  /**
   * Fetches the files of the given URIs into a directory.
   *
   * @param uris the URIs of the files
   * @param dir the directory to fetch the resources into
   * @param timeoutInMs the connect and read timeout in milliseconds for a remote file
   * @return the paths of the fetched local files, in the order of the URIs
   * @throws RuntimeException if a file cannot be fetched
   */
  public static List<String> fetchFiles(List<String> uris, File dir, int timeoutInMs) {
    return uris.stream().map(uri -> fetchFile(uri, dir, timeoutInMs)).collect(Collectors.toList());
  }

  /**
   * Fetches the file of the given URI into a directory, keeping its file name.
   *
   * @param uri the URI of the file, a URI without scheme is a local path
   * @param dir the directory to fetch the file into
   * @param timeoutInMs the connect and read timeout in milliseconds for a remote file
   * @return the path of the fetched local file
   * @throws IllegalArgumentException if the URI has no file name
   * @throws RuntimeException if the file cannot be fetched
   */
  public static String fetchFile(String uri, File dir, int timeoutInMs) {
    File destFile = new File(dir, fileNameOf(uri));
    try {
      return FileFetcher.get()
          .fetchFileFromUri(
              uri,
              destFile,
              timeoutInMs,
              null /* hadoopConf: job file URIs never use the hdfs scheme */);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to fetch file from URI %s", uri), e);
    }
  }

  /**
   * Checks the file names the resources of a job template get in the directory they are fetched
   * into, before anything is fetched: every resource is stored under the file name of its URI, so
   * two resources with the same file name would silently overwrite each other.
   */
  private static void checkFileNames(JobTemplate jobTemplate) {
    List<String> uris = new ArrayList<>();
    String commandName = null;
    if (jobTemplate instanceof ShellJobTemplate) {
      ShellJobTemplate shellJobTemplate = (ShellJobTemplate) jobTemplate;
      if (isCommandName(shellJobTemplate.executable())) {
        commandName = shellJobTemplate.executable();
      } else {
        uris.add(shellJobTemplate.executable());
      }
      uris.addAll(shellJobTemplate.scripts());
    } else if (jobTemplate instanceof SparkJobTemplate) {
      SparkJobTemplate sparkJobTemplate = (SparkJobTemplate) jobTemplate;
      uris.add(sparkJobTemplate.executable());
      uris.addAll(sparkJobTemplate.jars());
      uris.addAll(sparkJobTemplate.files());
      uris.addAll(sparkJobTemplate.archives());
    }

    Map<String, String> uriByFileName = new HashMap<>();
    for (String uri : uris) {
      String fileName = fileNameOf(uri);
      // The command is run from the PATH, so a fetched file of the same name is never the one
      // that runs, although the template looks like it should be.
      Preconditions.checkArgument(
          !fileName.equals(commandName),
          "The executable %s of job template %s is a command name, which is not fetched but looked "
              + "up where the job runs, while the resource %s with the same file name is fetched. "
              + "To run the fetched file, use its URI or absolute path as the executable",
          commandName,
          jobTemplate.name(),
          uri);

      String previousUri = uriByFileName.putIfAbsent(fileName, uri);
      Preconditions.checkArgument(
          previousUri == null || previousUri.equals(uri),
          "The resources %s and %s of job template %s have the same file name %s, and would "
              + "overwrite each other when they are fetched. Give them different file names",
          previousUri,
          uri,
          jobTemplate.name(),
          fileName);
    }
  }

  /** Returns the file name a resource gets when it is fetched: the last segment of its path. */
  private static String fileNameOf(String uri) {
    String path;
    try {
      path = new URI(uri).getPath();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("The resource URI %s is invalid", uri), e);
    }

    String fileName = path == null ? null : new File(path).getName();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fileName),
        "The resource URI %s has no file name, it must point to a file",
        uri);
    return fileName;
  }
}
