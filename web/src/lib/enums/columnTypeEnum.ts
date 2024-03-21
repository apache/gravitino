/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

export enum ColumnTypeColorEnum {
  boolean = 'primary',
  short = 'primary',
  integer = 'primary',
  long = 'primary',
  float = 'primary',
  double = 'primary',
  decimal = 'primary',
  fixed = 'primary',

  date = 'info',
  time = 'info',
  timestamp = 'info',
  timestamp_tz = 'info',
  interval_day = 'info',
  interval_year = 'info',

  string = 'warning',
  char = 'warning',
  varchar = 'warning',

  byte = 'success',
  uuid = 'success',
  binary = 'success'
}
