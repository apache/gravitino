const Tabs = skin => {
  return {
    MuiTabs: {
      styleOverrides: {
        vertical: {
          minWidth: 130,
          '& .MuiTab-root': {
            minWidth: 130
          }
        }
      }
    },
    MuiTab: {
      styleOverrides: {
        textColorSecondary: ({ theme }) => ({
          '&.Mui-selected': {
            color: theme.palette.text.secondary
          }
        })
      }
    },
    MuiTabPanel: {
      styleOverrides: {
        root: ({ theme }) => ({
          padding: theme.spacing(6),
          backgroundColor: theme.palette.background.paper,
          ...(skin === 'bordered' ? { border: `1px solid ${theme.palette.divider}` } : { boxShadow: theme.shadows[6] })
        })
      }
    }
  }
}

export default Tabs
