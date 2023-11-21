/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import MuiBadge from '@mui/material/Badge'

import useBgColor from 'src/@core/hooks/useBgColor'

const Badge = props => {
  const { sx, skin, color } = props

  const bgColors = useBgColor()

  const colors = {
    primary: { ...bgColors.primaryLight },
    secondary: { ...bgColors.secondaryLight },
    success: { ...bgColors.successLight },
    error: { ...bgColors.errorLight },
    warning: { ...bgColors.warningLight },
    info: { ...bgColors.infoLight }
  }

  return (
    <MuiBadge
      {...props}
      sx={skin === 'light' && color ? Object.assign({ '& .MuiBadge-badge': colors[color] }, sx) : sx}
    />
  )
}

export default Badge
