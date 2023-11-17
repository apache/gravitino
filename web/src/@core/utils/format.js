import { format } from 'date-fns'
import { enUS } from 'date-fns/locale'

export const formatToDate = value => {
  const date = new Date(value)

  const formatted = format(date, 'yyyy-MM-dd HH:mm:ss', {
    locale: enUS
  })

  return formatted
}
