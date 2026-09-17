# Express + React chat

The sample bundled with `little-actors init`.

## Run it

```sh
npm install
npx little-actors generate
npx little-actors dev
```

Wait for `Local actors ready`. In another terminal, from this directory:

```sh
npm run dev
```

Open [the chat](http://127.0.0.1:3000) in two tabs. Send a message, then reload to see the saved history.

## How it works

Everyone is a guest in this demo. In your app, authenticate the request before `ActorProxy.handle` and derive metadata from the signed-in user.

After changing the actor's types, rerun `npx little-actors generate` and restart the actor server.
