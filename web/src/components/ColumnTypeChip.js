/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { Chip } from '@mui/material'
import { ColumnTypeColorEnum } from '@/lib/enums/columnTypeEnum'
import colors from '@/lib/theme/colors'
import { alpha } from '@/lib/utils/color'
import { isString } from 'lodash-es'

const ColumnTypeChip = props => {
  const { type = '' } = props

  const formatType = type && isString(type) ? type.replace(/\(.*\)/, '') : type

  const label = isString(type) ? type : `${type?.type}`

  const columnTypeColor = ColumnTypeColorEnum[formatType] || 'secondary'
  const color = colors[columnTypeColor]?.main || '#8592A3'
  const bgColor = alpha(color, 0.1)

  return (
    <>
      {label ? (
        <Chip size='small' label={label} sx={{ backgroundColor: bgColor }} color={columnTypeColor} variant='outlined' />
      ) : null}
    </>
  )
}

export default ColumnTypeChip
