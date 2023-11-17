const Avatar = () => {
  return {
    MuiAvatar: {
      styleOverrides: {
        colorDefault: ({ theme }) => ({
          color: theme.palette.text.secondary,
          backgroundColor: theme.palette.customColors.avatarBg
        }),
        rounded: {
          borderRadius: 5
        }
      }
    },
    MuiAvatarGroup: {
      styleOverrides: {
        root: ({ theme }) => ({
          '&.pull-up': {
            '& .MuiAvatar-root': {
              cursor: 'pointer',
              transition: 'box-shadow 0.2s ease, transform 0.2s ease',
              '&:hover': {
                zIndex: 2,
                boxShadow: theme.shadows[3],
                transform: 'translateY(-4px)'
              }
            }
          },
          justifyContent: 'flex-end',
          '.MuiCard-root & .MuiAvatar-root': {
            borderColor: theme.palette.background.paper
          }
        })
      }
    }
  }
}

export default Avatar
