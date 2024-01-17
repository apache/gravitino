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
  'decimal(10,2)' = 'primary',
  'fixed(16)' = 'primary',

  date = 'info',
  time = 'info',
  timestamp = 'info',
  timestamp_tz = 'info',
  interval_day = 'info',
  interval_year = 'info',

  string = 'warning',
  'char(10)' = 'warning',
  'varchar(10)' = 'warning',

  byte = 'success',
  uuid = 'success',
  binary = 'success'
}
