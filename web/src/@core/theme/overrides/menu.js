const Menu = skin => {
  const boxShadow = theme => {
    if (skin === 'bordered') {
      return theme.shadows[0]
    } else if (theme.palette.mode === 'light') {
      return theme.shadows[8]
    } else return theme.shadows[9]
  }

  return {
    MuiMenu: {
      styleOverrides: {
        root: ({ theme }) => ({
          '& .MuiMenu-paper': {
            boxShadow: boxShadow(theme),
            ...(skin === 'bordered' && { border: `1px solid ${theme.palette.divider}` })
          }
        }),
        list: ({ theme }) => ({
          padding: theme.spacing(1.25, 0)
        }),
        paper: ({ theme }) => ({
          marginTop: theme.spacing(1)
        })
      }
    },
    MuiMenuItem: {
      styleOverrides: {
        root: ({ theme }) => ({
          padding: theme.spacing(2, 5),
          '&.Mui-selected': {
            color: theme.palette.primary.main
          }
        })
      }
    }
  }
}

export default Menu
