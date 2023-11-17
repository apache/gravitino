const Rating = () => {
  return {
    MuiRating: {
      styleOverrides: {
        root: ({ theme }) => ({
          color: theme.palette.warning.main,
          '& svg': {
            flexShrink: 0
          }
        })
      }
    }
  }
}

export default Rating
