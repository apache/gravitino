/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { Icon } from '@iconify/react'

const IconifyIcon = ({ icon, ...rest }) => {
  return <Icon icon={icon} fontSize='22px' {...rest} />
}

export default IconifyIcon
