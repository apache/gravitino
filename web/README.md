<!--
  Copyright 2023 Datastrato Pvt Ltd.
  This software is licensed under the Apache License version 2.
-->

# Gravitino Web UI

> **ℹ️ Tips**
>
> Under normal circumstances, you only need to visit [http://localhost:8090](http://localhost:8090) if you're just using the web UI to manage the Gravitino.
>
> You don't need to start the web development mode. If you need to modify the web part of the code, you can refer to the following document content for development and testing.

---

> **⚠️ Important**
>
> Before running commands, you must ensure that you are in the front-end directory `gravitino/web`. If not, run `cd web` first.

---

## Getting started

### Preparation | framework & dependencies

- [Node.js](https://nodejs.org)(v20.x+) & [npm](https://www.npmjs.com/) / [pnpm](https://pnpm.io/)
- [React](https://react.dev/)
- [Next.js](https://nextjs.org)
- [MUI](https://mui.com/)
- [tailwindcss](https://tailwindcss.com/)
- [`react-redux`](https://react-redux.js.org/)

> **TIP**
>
> You should use the `pnpm` package manager.
>
> **Requirements**
>
> Please make sure you use the node's LTS version
> Before installing the **node_modules**, make sure you have files starting with a **dot(.eslintrc, .env etc..)**

## Installation

### Development environment

- Run the below command in the console to install the required dependencies.

```bash
pnpm install
```

- After installing the modules start the development server with following command:

```bash
pnpm dev
```

- Visit <http://localhost:3000> to view the Gravitino Web UI in your browser. You can start editing the page such as `pages/index.js`. The page auto-updates as you edit the file.

:::caution important
The Gravitino Web UI only works in the latest version of the Chrome browser. You may encounter issues in other browsers.
:::

### Development scripts

This command runs ESLint to help you inspect the code. If errors occur, please make modifications based on the provided prompts.

```bash
pnpm lint
```

This command runs Prettier to help you check your code styles. You can manually fix the code when errors are shown, or use `pnpm format` to fix the code with Prettier CLI.

```bash
pnpm prettier:check
```

This command automatically formats the code.

```bash
pnpm format
```

## Self-hosting deployment

### Static HTML export

```bash
pnpm dist
```

The command `pnpm dist` allows you to export your app to static HTML, which runs standalone without the need for a Node.js server.

`pnpm dist` will generate a `dist` directory, producing content for any static hosting service.

## Docker

Make sure you have installed the most recent version of [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/install/#scenario-two-install-the-compose-plugin). [Docker Desktop](https://www.docker.com/products/docker-desktop/) includes Docker Engine, Docker CLI, and Docker Compose.

> **⚠️ Important**
>
> All commands in this document run in a `macOS` environment. If you are using a different operating system, you may encounter errors.

Only use Docker to build the static `HTML\CSS\JS` files directory.

Run the following commands in the console:

```bash
# ensure you are in the `web` directory
docker run -it --rm --name gravitino-web-docker -v ${PWD}:/web -w /web node:20-slim /bin/bash -c "pnpm install && pnpm dist"
docker run -it -p 3000:3000 -v ${PWD}:/web -w /web --name gravitino-web node:20-slim /bin/bash
docker run -p 3000:3000 -v ${PWD}:/web --name gravitino-web node:20-slim /bin/bash -c "pnpm install && pnpm dist"
```

This command runs `pnpm install` to install the dependencies specified in the `package.json` file and then runs `pnpm dist` to export a static version of the application.
The exported files are saved to the `dist` directory inside the container, and mounted in the `dist` directory in the current directory of the host machine.
This means that the exported files are accessible on the host machine after running the command.

If you also want to start a server to view the demo, please use the following code:

```bash
docker run -it --rm --name gravitino-web-docker -v ${PWD}:/web -p 3000:3000 -w /web node:20-slim /bin/bash -c "pnpm install && pnpm dev"
```

You can access the Gravitino Web UI by typing <http://localhost:3000> in your browser.
