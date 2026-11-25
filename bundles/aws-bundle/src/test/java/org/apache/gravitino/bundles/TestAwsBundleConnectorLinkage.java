/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.bundles;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;

/**
 * Checks that the S3A connector in the AWS bundle only links against relocated classes that the
 * bundle contains. A relocated class can only come from the bundle itself, so a missing one fails
 * with a {@link NoClassDefFoundError} at runtime, for example when a new Hadoop release starts to
 * use a library that the bundle does not package.
 */
class TestAwsBundleConnectorLinkage {
  private static final File BUNDLE = new File(System.getProperty("shadowJarPath"));
  private static final String CONNECTOR = "org/apache/hadoop/fs/s3a/";
  private static final String RELOCATED = "org/apache/gravitino/aws/shaded/";

  /**
   * Connector classes that may reference relocated classes the bundle does not package, because the
   * bundle does not support the feature: AWS role policies of S3A delegation tokens (Jackson) and
   * the marker tool command line (Commons IO).
   */
  private static final Set<String> UNSUPPORTED =
      new HashSet<>(
          Arrays.asList(CONNECTOR + "auth/RoleModel.class", CONNECTOR + "tools/MarkerTool.class"));

  @Test
  void testRelocatedReferencesAreBundled() throws IOException {
    Map<String, Set<String>> missing = new TreeMap<>();
    try (JarFile jar = new JarFile(BUNDLE)) {
      Set<String> bundled = new TreeSet<>();
      jar.stream().map(JarEntry::getName).forEach(bundled::add);
      for (JarEntry entry : (Iterable<JarEntry>) jar.stream()::iterator) {
        String name = entry.getName();
        if (!name.startsWith(CONNECTOR) || !name.endsWith(".class") || UNSUPPORTED.contains(name)) {
          continue;
        }
        try (InputStream in = jar.getInputStream(entry)) {
          for (String reference : referencedClasses(in)) {
            if (reference.startsWith(RELOCATED) && !bundled.contains(reference + ".class")) {
              missing.computeIfAbsent(reference, k -> new TreeSet<>()).add(name);
            }
          }
        }
      }
    }
    assertTrue(missing.isEmpty(), "Relocated classes missing from the bundle: " + missing);
  }

  /** Returns the classes in the constant pool of a class file, as internal names. */
  private static Set<String> referencedClasses(InputStream classFile) throws IOException {
    DataInputStream in = new DataInputStream(classFile);
    in.readInt(); // magic
    in.readUnsignedShort(); // minor version
    in.readUnsignedShort(); // major version
    int count = in.readUnsignedShort();
    String[] utf8 = new String[count];
    int[] classNames = new int[count];
    for (int i = 1; i < count; i++) {
      int tag = in.readUnsignedByte();
      switch (tag) {
        case 1: // Utf8
          utf8[i] = in.readUTF();
          break;
        case 7: // Class
          classNames[i] = in.readUnsignedShort();
          break;
        case 8: // String
        case 16: // MethodType
        case 19: // Module
        case 20: // Package
          in.readUnsignedShort();
          break;
        case 15: // MethodHandle
          in.readUnsignedByte();
          in.readUnsignedShort();
          break;
        case 3: // Integer
        case 4: // Float
        case 9: // Fieldref
        case 10: // Methodref
        case 11: // InterfaceMethodref
        case 12: // NameAndType
        case 17: // Dynamic
        case 18: // InvokeDynamic
          in.readInt();
          break;
        case 5: // Long
        case 6: // Double
          in.readLong();
          i++;
          break;
        default:
          throw new IOException("Unknown constant pool tag " + tag);
      }
    }
    Set<String> classes = new TreeSet<>();
    for (int index : classNames) {
      if (index != 0) {
        // Array classes are written as descriptors, for example [Lorg/example/Type;.
        classes.add(utf8[index].replaceFirst("^\\[+L?", "").replaceFirst(";$", ""));
      }
    }
    return classes;
  }
}
