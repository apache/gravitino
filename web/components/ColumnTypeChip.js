/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { Chip } from '@mui/material'
import { ColumnTypeColorEnum } from '@/lib/enums/columnTypeEnum'
import colors from '@/lib/theme/colors'
import { alpha } from '@/lib/utils/color'

const ColumnTypeChip = props => {
  const { type } = props

  const bgColor = alpha(colors[ColumnTypeColorEnum[type]].main, 0.1)

  return (
    <Chip
      size='small'
      label={type}
      sx={{ backgroundColor: bgColor }}
      color={ColumnTypeColorEnum[type]}
      variant='outlined'
    />
  )
}

export default ColumnTypeChip
