/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

const fs = require('fs')
const path = require('path')

const ROOT = path.resolve(__dirname, '..')
const SRC = path.join(ROOT, 'src')

function ensureExists(p, name) {
  if (!fs.existsSync(p)) {
    console.error(`required ${name} not found: ${p}`)
    process.exit(1)
  }
}

function isSymlinkTo(linkPath, targetPath) {
  try {
    const s = fs.lstatSync(linkPath)
    if (!s.isSymbolicLink()) return false
    const real = fs.readlinkSync(linkPath)
    return path.resolve(path.dirname(linkPath), real) === path.resolve(targetPath)
  } catch (e) {
    return false
  }
}

function backupIfNeeded(p) {
  if (!fs.existsSync(p)) return
  const stat = fs.lstatSync(p)
  if (stat.isSymbolicLink()) return
  const bak = p + '.bak'
  if (fs.existsSync(bak)) return
  fs.renameSync(p, bak)
}

function restoreBackupIfExists(p) {
  const bak = p + '.bak'
  if (fs.existsSync(bak) && !fs.existsSync(p)) {
    fs.renameSync(bak, p)
  }
}

function makeSymlink(target, linkPath) {
  if (isSymlinkTo(linkPath, target)) return
  if (fs.existsSync(linkPath)) {
    // remove existing file/dir/symlink
    fs.rmSync(linkPath, { recursive: true, force: true })
  }
  fs.symlinkSync(target, linkPath, 'dir')
}

function copyDirectory(target, dest) {
  // copy from target -> dest (recursive). Use cpSync when available (Node 16+)
  if (fs.existsSync(dest)) {
    fs.rmSync(dest, { recursive: true, force: true })
  }
  fs.cpSync(target, dest, { recursive: true })
}

function removeSymlinkIfPointsTo(linkPath, target) {
  if (isSymlinkTo(linkPath, target)) {
    fs.unlinkSync(linkPath)
  }
}

const buildTarget = process.env.BUILD_TARGET || 'new'
console.log(`select-build-target: BUILD_TARGET=${buildTarget}`)

// map of names under src
const items = [
  { name: 'app', alt: 'appOld' },
  { name: 'components', alt: 'componentsOld' }
]

for (const it of items) {
  const normal = path.join(SRC, it.name)
  const alt = path.join(SRC, it.alt)

  if (buildTarget === 'old') {
    ensureExists(alt, it.alt)
    // backup existing normal if it's a real dir
    backupIfNeeded(normal)
    // create a physical copy normal <- alt to avoid symlink/watch issues
    try {
      copyDirectory(alt, normal)
      console.log(`copied ${alt} -> ${normal}`)
    } catch (e) {
      // fallback to symlink if copy fails for any reason
      makeSymlink(alt, normal)
      console.log(`linked ${normal} -> ${alt} (fallback)`)
    }
  } else {
    // for new/default target: if normal is symlink pointing to alt, remove it and restore backup if present
    // if normal is symlink to alt, remove it
    removeSymlinkIfPointsTo(normal, alt)
    // if normal is a copied directory (from previous 'old'), remove it so backup can be restored
    try {
      if (fs.existsSync(normal) && !fs.lstatSync(normal).isSymbolicLink()) {
        // Only remove the directory if we previously backed up the original (i.e. .bak exists).
        // This avoids deleting a native `src/app` or `src/components` that were never backed up.
        const bak = normal + '.bak'
        if (fs.existsSync(bak)) {
          // remove copied directory (it was created from alt previously)
          fs.rmSync(normal, { recursive: true, force: true })
        } else {
          console.log(`preserving ${normal} (no ${path.basename(normal)}.bak found)`)
        }
      }
    } catch (e) {
      // ignore errors
    }
    restoreBackupIfExists(normal)
    console.log(`ensured ${normal} is using native version (restored from backup if present)`)
  }
}

process.exit(0)
