# Gravitino Web

> **⚠️ Important**
>
> Before running commands, you must ensure that you are in the front-end directory `gravitino/web`. If not, run `cd web` first.

---

## Getting Started

### Preparation | Framework & Dependencies

- [Node.js](https://nodejs.org)(v20.x+) & [npm](https://www.npmjs.com/) / [yarn](https://yarnpkg.com/)
- React
- Next.js
- MUI
- `react-redux`

> **TIP**
>
> Yarn package manager is recommended
>
> **Requirements**
>
> Please make sure you use the node’s LTS version
> Before installing the **node_modules**, make sure you have files starting with a **dot(.eslintrc, .env etc..)**

## Installation

### Development environment

- Run below command in console:

```bash
# install dependencies
yarn install
```

- After installing the modules run your project with following command:

```bash
# start development server
yarn dev
```

- Visit <http://localhost:3000> to check it in your browser. You can start editing the page such as `pages/index.js`. The page auto-updates as you edit the file.

## Self-hosting Deployment

### Node.js Server

Next.js can be deployed to any hosting provider that supports Node.js. Make sure your `package.json` has the `build` and `start` scripts:

```json
{
  "scripts": {
    "dev": "next dev",
    "build": "next build",
    "start": "next start"
  }
}
```

`next build` builds the production application in the `.next` folder. After building, `next start` starts a Node.js server that supports hybrid pages, serving both statically generated and server-side rendered pages.

```bash
# build production files
yarn build

# start the nodejs server
yarn start
```

### Static HTML Export

Command `next export` allows you to export your app to static HTML, which can be run standalone without the need of a Node.js server.

`next export` will generate an `out` directory, which can be served by any static hosting service.

```bash
yarn export
# then copy the files within the 'out' directory to the root directory of the static server
```
