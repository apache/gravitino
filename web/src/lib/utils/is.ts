/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

export { isString, isFunction, isUndefined, isEmpty, isNull } from 'lodash-es'

export function is(val: unknown, type: string) {
  return toString.call(val) === `[object ${type}]`
}

export function isObject(val: any): val is Record<any, any> {
  return val !== null && is(val, 'Object')
}

export function isArray(val: any): val is Array<any> {
  return val && Array.isArray(val)
}
