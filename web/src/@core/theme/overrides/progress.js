const Progress = () => {
  return {
    MuiLinearProgress: {
      styleOverrides: {
        root: ({ theme }) => ({
          height: 12,
          borderRadius: 10,
          backgroundColor: theme.palette.customColors.trackBg
        }),
        bar: {
          borderRadius: 10
        }
      }
    }
  }
}

export default Progress
