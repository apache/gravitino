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

// Keep in sync with AccessControlContent.PERMITTED_PRIVILEGES and the OpenAPI enum.
export const accessControlPrivilegeOptions = [
  { label: 'Select Table', value: 'select_table' },
  { label: 'Modify Table', value: 'modify_table' },
  { label: 'Probe Table Like', value: 'probe_table_like' },
  { label: 'Select View', value: 'select_view' },
  { label: 'Read Fileset', value: 'read_fileset' },
  { label: 'Write Fileset', value: 'write_fileset' },
  { label: 'Consume Topic', value: 'consume_topic' },
  { label: 'Produce Topic', value: 'produce_topic' },
  { label: 'Use Model', value: 'use_model' },
  { label: 'Execute Function', value: 'execute_function' },
  { label: 'Use Semantic Model', value: 'use_semantic_model' }
]
