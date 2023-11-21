/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

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
