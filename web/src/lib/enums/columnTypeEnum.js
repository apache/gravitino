/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

export var ColumnTypeColorEnum

;(function (ColumnTypeColorEnum) {
  ColumnTypeColorEnum['boolean'] = 'primary'
  ColumnTypeColorEnum['short'] = 'primary'
  ColumnTypeColorEnum['integer'] = 'primary'
  ColumnTypeColorEnum['long'] = 'primary'
  ColumnTypeColorEnum['float'] = 'primary'
  ColumnTypeColorEnum['double'] = 'primary'
  ColumnTypeColorEnum['decimal'] = 'primary'
  ColumnTypeColorEnum['fixed'] = 'primary'
  ColumnTypeColorEnum['date'] = 'info'
  ColumnTypeColorEnum['time'] = 'info'
  ColumnTypeColorEnum['timestamp'] = 'info'
  ColumnTypeColorEnum['timestamp_tz'] = 'info'
  ColumnTypeColorEnum['interval_day'] = 'info'
  ColumnTypeColorEnum['interval_year'] = 'info'
  ColumnTypeColorEnum['string'] = 'warning'
  ColumnTypeColorEnum['char'] = 'warning'
  ColumnTypeColorEnum['varchar'] = 'warning'
  ColumnTypeColorEnum['byte'] = 'success'
  ColumnTypeColorEnum['uuid'] = 'success'
  ColumnTypeColorEnum['binary'] = 'success'
})(ColumnTypeColorEnum || (ColumnTypeColorEnum = {}))

export default ColumnTypeColorEnum
