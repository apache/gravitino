/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { Icon } from '@iconify/react'

const IconifyIcon = ({ icon, ...props }) => {
  return <Icon icon={icon} fontSize='24px' {...props} />
}

export default IconifyIcon
