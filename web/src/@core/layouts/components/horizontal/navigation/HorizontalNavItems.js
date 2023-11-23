/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import HorizontalNavLink from './HorizontalNavLink'

const resolveComponent = item => {
  return HorizontalNavLink
}

const HorizontalNavItems = props => {
  const RenderMenuItems = props.horizontalNavItems?.map((item, index) => {
    const TagName = resolveComponent(item)

    return <TagName {...props} key={index} item={item} />
  })

  return <>{RenderMenuItems}</>
}

export default HorizontalNavItems
