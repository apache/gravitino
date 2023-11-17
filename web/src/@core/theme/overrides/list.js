const List = () => {
  return {
    MuiListItemIcon: {
      styleOverrides: {
        root: ({ theme }) => ({
          minWidth: '0 !important',
          marginRight: theme.spacing(2.25),
          color: theme.palette.text.secondary
        })
      }
    },
    MuiListItemAvatar: {
      styleOverrides: {
        root: ({ theme }) => ({
          minWidth: 0,
          marginRight: theme.spacing(4)
        })
      }
    },
    MuiListItemText: {
      styleOverrides: {
        dense: ({ theme }) => ({
          '& .MuiListItemText-primary': {
            color: theme.palette.text.primary
          }
        })
      }
    },
    MuiListSubheader: {
      styleOverrides: {
        root: ({ theme }) => ({
          fontWeight: 600,
          textTransform: 'uppercase',
          color: theme.palette.text.primary
        })
      }
    }
  }
}

export default List
