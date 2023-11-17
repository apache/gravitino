import { Icon } from '@iconify/react'

const IconifyIcon = ({ icon, ...rest }) => {
  return <Icon icon={icon} fontSize='22px' {...rest} />
}

export default IconifyIcon
