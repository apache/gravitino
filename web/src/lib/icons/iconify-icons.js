/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import fs from 'node:fs'
import path from 'node:path'

import { cleanupSVG, importDirectory, runSVGO } from '@iconify/tools'
import { getIconsCSS } from '@iconify/utils'

const SVGs = {
  dir: 'src/lib/icons/svg',
  prefix: 'custom-icons'
}

const target = path.join(__dirname, 'iconify-icons.css')

;(async () => {
  const icons = []

  const iconSet = await importDirectory(SVGs.dir, {
    prefix: SVGs.prefix
  })

  await iconSet.forEach(async name => {
    const svg = iconSet.toSVG(name)
    cleanupSVG(svg)
    runSVGO(svg)
    iconSet.fromSVG(name, svg)
  })

  icons.push(iconSet.export())

  await fs.promises.writeFile(
    target,
    `
/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

${icons
  .map(iconSet =>
    getIconsCSS(iconSet, Object.keys(iconSet.icons), {
      iconSelector: '.{prefix}-{name}'
    })
  )
  .join('\n')}
  `,
    'utf8'
  )
})()
