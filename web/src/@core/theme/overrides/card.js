const Card = skin => {
  return {
    MuiCard: {
      styleOverrides: {
        root: ({ theme }) => ({
          borderRadius: 8,
          ...(skin === 'bordered' && { border: `1px solid ${theme.palette.divider}` }),
          '& .card-more-options': {
            marginTop: theme.spacing(-1),
            marginRight: theme.spacing(-3)
          },
          '& .MuiTableContainer-root, & .MuiDataGrid-root, & .MuiDataGrid-columnHeaders': {
            borderRadius: 0
          }
        })
      },
      defaultProps: {
        elevation: skin === 'bordered' ? 0 : 6
      }
    },
    MuiCardHeader: {
      styleOverrides: {
        root: ({ theme }) => ({
          padding: theme.spacing(6),
          '& + .MuiCardContent-root, & + .MuiCardActions-root, & + .MuiCollapse-root .MuiCardContent-root': {
            paddingTop: 0
          },
          '& .MuiCardHeader-subheader': {
            fontSize: '0.875rem',
            marginTop: theme.spacing(1.25),
            color: theme.palette.text.secondary
          }
        }),
        title: {
          lineHeight: 1.6,
          fontWeight: 500,
          fontSize: '1.125rem',
          letterSpacing: '0.15px',
          '@media (min-width: 600px)': {
            fontSize: '1.25rem'
          }
        },
        action: {
          marginTop: 0,
          marginRight: 0
        }
      }
    },
    MuiCardContent: {
      styleOverrides: {
        root: ({ theme }) => ({
          padding: theme.spacing(6),
          '& + .MuiCardHeader-root, & + .MuiCardContent-root, & + .MuiCardActions-root': {
            paddingTop: 0
          },
          '&:last-of-type': {
            paddingBottom: theme.spacing(6)
          }
        })
      }
    },
    MuiCardActions: {
      styleOverrides: {
        root: ({ theme }) => ({
          padding: theme.spacing(6),
          '& .MuiButton-text': {
            paddingLeft: theme.spacing(3),
            paddingRight: theme.spacing(3)
          },
          '&.card-action-dense': {
            padding: theme.spacing(0, 3, 3),
            '.MuiCard-root .MuiCardMedia-root + &': {
              paddingTop: theme.spacing(3)
            }
          },
          '.MuiCard-root &:first-of-type': {
            paddingTop: theme.spacing(3),
            '& + .MuiCardHeader-root, & + .MuiCardContent-root, & + .MuiCardActions-root': {
              paddingTop: 0
            }
          }
        })
      }
    }
  }
}

export default Card
