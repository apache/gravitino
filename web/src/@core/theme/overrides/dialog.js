const Dialog = skin => {
  return {
    MuiDialog: {
      styleOverrides: {
        paper: ({ theme }) => ({
          boxShadow: theme.shadows[skin === 'bordered' ? 0 : 6],
          ...(skin === 'bordered' && { border: `1px solid ${theme.palette.divider}` }),
          '&:not(.MuiDialog-paperFullScreen)': {
            borderRadius: 8,
            [theme.breakpoints.down('sm')]: {
              margin: theme.spacing(4),
              width: `calc(100% - ${theme.spacing(8)})`,
              maxWidth: `calc(100% - ${theme.spacing(8)}) !important`
            }
          },
          '& > .MuiList-root': {
            paddingLeft: theme.spacing(1),
            paddingRight: theme.spacing(1)
          }
        })
      }
    },
    MuiDialogTitle: {
      styleOverrides: {
        root: ({ theme }) => ({
          padding: theme.spacing(6, 6, 1),
          '& + .MuiDialogContent-root': {
            paddingTop: `${theme.spacing(6)} !important`
          }
        })
      }
    },
    MuiDialogContent: {
      styleOverrides: {
        root: ({ theme }) => ({
          padding: theme.spacing(6),
          '& + .MuiDialogContent-root': {
            paddingTop: '0 !important'
          },
          '& + .MuiDialogActions-root': {
            paddingTop: '0 !important'
          }
        })
      }
    },
    MuiDialogActions: {
      styleOverrides: {
        root: ({ theme }) => ({
          padding: theme.spacing(1, 6, 6),
          '&.dialog-actions-dense': {
            padding: theme.spacing(3),
            paddingTop: theme.spacing(1)
          }
        })
      }
    }
  }
}

export default Dialog
