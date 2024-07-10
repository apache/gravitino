/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Chip } from '@mui/material'
import { ColumnTypeColorEnum } from '@/lib/enums/columnTypeEnum.js'
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
