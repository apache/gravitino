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

export const privilegeTypes = [
  {
    label: 'Metalake',
    value: 'metalake'
  },
  {
    label: 'Catalog',
    value: 'catalog'
  },
  {
    label: 'Schema',
    value: 'schema'
  },
  {
    label: 'Table',
    value: 'table'
  },
  {
    label: 'Topic',
    value: 'topic'
  },
  {
    label: 'Fileset',
    value: 'fileset'
  },
  {
    label: 'Model',
    value: 'model'
  },
  {
    label: 'Tag',
    value: 'tag'
  },
  {
    label: 'Policy',
    value: 'policy'
  },
  {
    label: 'Job Template',
    value: 'job_template'
  }
]

export const privilegeOptions = [
  {
    label: 'User privileges',
    options: [{ label: 'Manage Users', value: 'manage_users' }]
  },
  {
    label: 'Group privileges',
    options: [{ label: 'Manage Groups', value: 'manage_groups' }]
  },
  {
    label: 'Role privileges',
    options: [{ label: 'Manage Grants', value: 'manage_grants' }]
  },
  {
    label: 'Permission privileges',
    options: [{ label: 'Create Role', value: 'create_role' }]
  },
  {
    label: 'Catalog privileges',
    options: [
      { label: 'Create Catalog', value: 'create_catalog' },
      { label: 'Use Catalog', value: 'use_catalog' }
    ]
  },
  {
    label: 'Schema privileges',
    options: [
      { label: 'Create Schema', value: 'create_schema' },
      { label: 'Use Schema', value: 'use_schema' }
    ]
  },
  {
    label: 'Table privileges',
    options: [
      { label: 'Create Table', value: 'create_table' },
      { label: 'Modify Table', value: 'modify_table' },
      { label: 'Select Table', value: 'select_table' }
    ]
  },
  {
    label: 'Topic privileges',
    options: [
      { label: 'Create Topic', value: 'create_topic' },
      { label: 'Produce Topic', value: 'produce_topic' },
      { label: 'Consume Topic', value: 'consume_topic' }
    ]
  },
  {
    label: 'Fileset privileges',
    options: [
      { label: 'Create Fileset', value: 'create_fileset' },
      { label: 'Write Fileset', value: 'write_fileset' },
      { label: 'Read Fileset', value: 'read_fileset' }
    ]
  },
  {
    label: 'Model privileges',
    options: [
      { label: 'Register Model', value: 'register_model' },
      { label: 'Link Model Version', value: 'link_model_version' },
      { label: 'Use Model', value: 'use_model' }
    ]
  },
  {
    label: 'Tag privileges',
    options: [
      { label: 'Create Tag', value: 'create_tag' },
      { label: 'Apply Tag', value: 'apply_tag' }
    ]
  },
  {
    label: 'Policy privileges',
    options: [
      { label: 'Create Policy', value: 'create_policy' },
      { label: 'Apply Policy', value: 'apply_policy' }
    ]
  },
  {
    label: 'Job Template privileges',
    options: [
      { label: 'Register Job Template', value: 'register_job_template' },
      { label: 'Use Job Template', value: 'use_job_template' }
    ]
  },
  {
    label: 'Job privileges',
    options: [{ label: 'Run Job', value: 'run_job' }]
  }
]
