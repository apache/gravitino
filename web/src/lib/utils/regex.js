/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

export const nameRegex = /^\w[\w]{0,63}$/

export const nameRegexDesc =
  'This field must begin with a letter or underscore, contain only alphanumeric characters or underscores, and be between 1 and 64 characters in length'

export const keyRegex = /^[a-zA-Z_][a-zA-Z0-9-_.]*$/
