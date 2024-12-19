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

// join the parent and name to a path
pub fn join_file_path(parent: &str, name: &str) -> String {
    //TODO handle corner cases
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", parent, name)
    }
}

// split the path to parent and name
pub fn split_file_path(path: &str) -> (&str, &str) {
    match path.rfind('/') {
        Some(pos) => (&path[..pos], &path[pos + 1..]),
        None => ("", path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_file_path() {
        assert_eq!(join_file_path("", "a"), "a");
        assert_eq!(join_file_path("", "a.txt"), "a.txt");
        assert_eq!(join_file_path("a", "b"), "a/b");
        assert_eq!(join_file_path("a/b", "c"), "a/b/c");
        assert_eq!(join_file_path("a/b", "c.txt"), "a/b/c.txt");
    }

    #[test]
    fn test_split_file_path() {
        assert_eq!(split_file_path("a"), ("", "a"));
        assert_eq!(split_file_path("a.txt"), ("", "a.txt"));
        assert_eq!(split_file_path("a/b"), ("a", "b"));
        assert_eq!(split_file_path("a/b/c"), ("a/b", "c"));
        assert_eq!(split_file_path("a/b/c.txt"), ("a/b", "c.txt"));
    }
}
