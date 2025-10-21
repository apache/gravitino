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
package org.apache.gravitino.filesystem.hadoop.integration.test;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class TestFileset {
  public static void main(String[] args) throws IOException {
    f1();
  }

  public static void f1() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
    conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
    conf.set("fs.gravitino.server.uri", "http://localhost:8090");
    conf.set("fs.gravitino.client.metalake", "test");
    // set the location name if you want to access a specific location
    // conf.set("fs.gravitino.current.location.name","test_location_name");
    Path filesetPath1 = new Path("gvfs://fileset/fileset_catalog/test_schema/fs6");
    Path filesetPath2 = new Path("gvfs://fileset/fileset_catalog/test_schema/fs7");
    FileSystem fs = filesetPath1.getFileSystem(conf);
    fs.create(new Path(filesetPath1 + "/file1.txt")).close();
    fs.create(new Path(filesetPath2 + "/file1.txt")).close();
    fs.create(new Path(filesetPath1 + "/file2.txt")).close();
    fs.create(new Path(filesetPath2 + "/file2.txt")).close();

    System.out.println(filesetPath1 + ":");
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(filesetPath1, false);
    while (files.hasNext()) {
      LocatedFileStatus f = files.next();
      System.out.println(f.getPath());
    }

    System.out.println();
    System.out.println(filesetPath2 + ":");
    FSDataOutputStream fsDataOutputStream = fs.create(new Path(filesetPath2 + "/file1.txt"));
    fsDataOutputStream.close();

    files = fs.listFiles(filesetPath2, false);
    while (files.hasNext()) {
      LocatedFileStatus f = files.next();
      System.out.println(f.getPath());
    }

    fs.delete(new Path(filesetPath2 + "/file1.txt"), true);
  }

  public static void f2() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
    conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
    conf.set("fs.gravitino.server.uri", "http://localhost:8090");
    conf.set("fs.gravitino.client.metalake", "test");
    // set the location name if you want to access a specific location
    // conf.set("fs.gravitino.current.location.name","test_location_name");
    Path filesetPath1 = new Path("gvfs://fileset/fileset_catalog/test_schema/fs6");
    Path filesetPath2 = new Path("gvfs://fileset/fileset_catalog/test_schema/fs7");
    FileSystem fs = filesetPath1.getFileSystem(conf);
    fs.create(new Path(filesetPath1 + "/file1.txt")).close();
    fs.create(new Path(filesetPath1 + "/file1.txt")).close();
    // fs.delete(new Path(filesetPath1 + "/file1.txt"), true);
    fs.create(new Path(filesetPath2 + "/file2.txt")).close();
    fs.create(new Path(filesetPath2 + "/file2.txt")).close();
  }
}
