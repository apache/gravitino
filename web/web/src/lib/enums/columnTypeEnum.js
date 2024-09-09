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

/**
 * @enum {string}
 */
const ColumnTypeColorEnum = Object.freeze({
  boolean: 'primary',
  short: 'primary',
  integer: 'primary',
  long: 'primary',
  float: 'primary',
  double: 'primary',
  decimal: 'primary',
  fixed: 'primary',

  date: 'info',
  time: 'info',
  timestamp: 'info',
  timestamp_tz: 'info',
  interval_day: 'info',
  interval_year: 'info',

  string: 'warning',
  char: 'warning',
  varchar: 'warning',

  byte: 'success',
  uuid: 'success',
  binary: 'success'
})

export { ColumnTypeColorEnum }
