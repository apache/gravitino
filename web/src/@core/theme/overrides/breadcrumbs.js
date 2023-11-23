/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

const Breadcrumbs = () => {
  return {
    MuiBreadcrumbs: {
      styleOverrides: {
        root: ({ theme }) => ({
          '& a': {
            textDecoration: 'none',
            color: theme.palette.primary.main
          }
        }),
        li: ({ theme }) => ({
          color: theme.palette.text.secondary,
          '& .MuiTypography-root': {
            color: 'inherit'
          }
        })
      }
    }
  }
}

export default Breadcrumbs
