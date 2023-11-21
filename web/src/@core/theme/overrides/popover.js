/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

const Popover = skin => {
  return {
    MuiPopover: {
      styleOverrides: {
        root: ({ theme }) => ({
          '& .MuiPopover-paper': {
            boxShadow: theme.shadows[skin === 'bordered' ? 0 : 6],
            ...(skin === 'bordered' && { border: `1px solid ${theme.palette.divider}` })
          }
        })
      }
    }
  }
}

export default Popover
