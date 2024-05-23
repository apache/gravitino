/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

// @ts-check

/**
 * @typedef {import('react-hot-toast').ToastPosition} ToastPosition
 */

const themeConfig = {
  /**
   * @description message box position
   * @type {ToastPosition}
   */
  toastPosition: 'top-right',

  /**
   * @description message box duration in milliseconds
   * @type {number}
   */
  toastDuration: 4000,

  /**
   * @description tracks whether user is being inactive in milliseconds, default is 30 minutes
   * @type {number}
   */
  idleOn: 18e5
}

export default themeConfig
