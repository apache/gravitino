import dateFN from 'dayjs'

const DATE_TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'

// const DATE_FORMAT = 'YYYY-MM-DD'

function formatToDateTime(date, format = DATE_TIME_FORMAT) {
  return dateFN(date).format(format)
}

export {
  dateFN,
  formatToDateTime

  //
}
