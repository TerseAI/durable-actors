# Express + React chat

Requires Node.js 20+ and Bun 1.4.2+ on your PATH; Bun executes the actors.

The sample bundled with `little-actors init`.

## Run it

```sh
npm install
npx little-actors generate
export DURABLE_OBJECT_API_KEY=local-dev-key
npx little-actors dev
```

Wait for `Local actors ready`. In another terminal, from this directory:

```sh
export DURABLE_OBJECT_API_KEY=local-dev-key
npm run dev
```

Open [the chat](http://127.0.0.1:3000) in two tabs. Send a message, then reload to see the saved history.

## How it works

Everyone is a guest in this demo. In your app, authenticate the request before `actors.ChatRoom.prepareWebsocket` and derive metadata from the signed-in user.

The frontend fetches a signed URL from the backend and passes it to `new WebSocket()`. The actor sends history explicitly on connection and after each message. Reload to reconnect if the socket closes.

After changing the actor's types, rerun `npx little-actors generate` and restart the actor server.
