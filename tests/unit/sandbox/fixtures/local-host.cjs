#!/usr/bin/env node
const fs = require("node:fs");
const id = process.env.DURABLE_ACTORS_HOST_ID;
fs.writeFileSync(`${id}.started`, String(process.pid));
process.stdin.resume();
process.stdin.on("end", () => process.exit(0));
process.on("exit", () => {
    try { fs.writeFileSync(`${id}.stopped`, ""); } catch {}
});
let published = false;
setInterval(() => {
    if (fs.existsSync(`${id}.fail`)) process.exit(1);
    if (published || !fs.existsSync(`${id}.release`)) return;
    const ready = process.env.DURABLE_ACTORS_HOST_READY_FILE;
    fs.copyFileSync(`${id}.release`, `${ready}.tmp`);
    fs.renameSync(`${ready}.tmp`, ready);
    published = true;
}, 5);
