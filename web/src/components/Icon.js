/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { Icon } from '@iconify/react'
import clsx from 'clsx'

const IconifyIcon = ({ icon, ...props }) => {
  return icon.startsWith('custom-icons') ? (
    <i className={clsx(icon, 'twc-text-[16px]')}></i>
  ) : (
    <Icon icon={icon} fontSize='24px' {...props} />
  )
}

export default IconifyIcon
