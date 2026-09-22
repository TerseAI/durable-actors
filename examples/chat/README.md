# Express + React chat

Requires Node.js 22.19+ and Bun 1.4.2+ on your PATH; Bun executes the actors.

The sample bundled with `little-actors init`.

## Run it

```sh
npm install
cp .env.example .env
npm run dev:actors
```

Wait for `Ready`. In another terminal in this directory, start the application:

```sh
npm run dev
```

Open [the chat](http://127.0.0.1:3000) in two tabs. Send a message, then reload to see the saved history.

Both processes read the project ID, local development API key, and control-plane URL from `.env`. `dev:actors` runs the actors; `dev` generates the backend client and starts Express and Vite. Run one example at a time with the default ports.

`npm run build` generates the client, checks TypeScript, and builds the frontend.

## How it works

Everyone is a guest in this demo. In your app, authenticate the request before `actors.ChatRoom.prepareWebsocket` and derive metadata from the signed-in user.

The frontend fetches a signed URL from the backend and passes it to `new WebSocket()`. The actor sends history explicitly on connection and after each message. Reload to reconnect if the socket closes.

The actor server watches source changes. After changing the actor's public types, restart `npm run dev` to regenerate the backend client. State remains in `.little-actors/` across restarts.
