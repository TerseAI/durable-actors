#!/usr/bin/env node
const fs = require('node:fs');
const http = require('node:http');

const server = http.createServer((incoming, response) => {
  let document = '';
  incoming.on('data', chunk => document += chunk);
  incoming.on('end', () => {
    const { request } = JSON.parse(document);
    const reply = result => response.end(JSON.stringify({ status: 'success', result }));
    if (request.barrier) {
      fs.writeFileSync(`${request.barrier}/${request.index}.started`, '');
      const timer = setInterval(() => {
        if (fs.readdirSync(request.barrier).filter(name => name.endsWith('.started')).length === 5) {
          clearInterval(timer);
          reply({ pid: process.pid, index: request.index });
        }
      }, 5);
      response.on('close', () => clearInterval(timer));
    } else if (request.fail) {
      response.end(JSON.stringify({ status: 'failure', error: 'test failure' }));
    } else if (request.oversized) {
      response.end('x'.repeat(5 * 1024 * 1024 + 1));
    } else if (request.malformed) {
      response.end('not json');
    } else if (request.disconnect) {
      fs.appendFileSync(request.attempts, 'attempt\n');
      server.close();
      incoming.socket.destroy();
      setInterval(() => {}, 1000);
    } else if (request.marker) {
      fs.writeFileSync(request.marker + '.tmp', String(process.pid));
      fs.renameSync(request.marker + '.tmp', request.marker);
      response.on('close', () => fs.writeFileSync(request.marker + '.cancelled', 'cancelled'));
    } else {
      reply({ pid: process.pid, index: request.index });
    }
  });
});
server.listen(process.argv[3], () => process.stdout.write('{"protocol":1}\n'));
