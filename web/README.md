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
yarn server
```

- Visit <http://localhost:3000> to check it in your browser. You can start editing the page such as `pages/index.js`. The page auto-updates as you edit the file.

## Self-hosting Deployment

### Node.js Server

Next.js can be deployed to any hosting provider that supports Node.js. Make sure your `package.json` has the `build` and `start` scripts:

```json
{
  "scripts": {
    "server": "next dev",
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

`next export` will generate an `dist` directory, which can be served by any static hosting service.

```bash
yarn dist
# then copy the files within the 'dist' directory to the root directory of the static server
```

## Docker

make sure you have installed the recent version of [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/install/#scenario-two-install-the-compose-plugin). ([Docker Desktop](https://www.docker.com/products/docker-desktop/) already includes Docker Engine, Docker CLI and Docker Compose)

> **⚠️ Important**
>
> All commands below are meant to be run in a `macOS` environment. If you are using a different system, you may encounter errors. Please modify the commands according to the system prompts. For example, if you are using `Windows`, replace `${PWD}` with `%cd%`, etc.

Only use Docker to build static `HTML\CSS\JS` files directory

Run below command in console:

```bash
# ensure you are in the `web` directory
docker run -it --rm --name gravitino-web-docker -v ${PWD}:/web -w /web node:20-slim /bin/bash -c "yarn install && yarn dist"
```

This command will run `yarn install` to install the dependencies specified in the `package.json` file and then run `yarn export` to export a static version of the application. The exported files will be saved to the `dist` directory inside the container, which is mounted to the `dist` directory in the current directory of the host machine. This means that the exported files will be accessible on the host machine after the command is executed.

If you also want to start a server to view with demo, please change to the following code:

```bash
docker run -it --rm --name gravitino-web-docker -v ${PWD}:/web -p 3000:3000 -w /web node:20-slim /bin/bash -c "yarn install && yarn server"

# Open http://localhost:3000
```
