import request from 'supertest';
import assert from 'assert';
import http from 'http';
import net from 'net';
import { Server as WebSocketServer } from 'ws';
import WebSocket from 'ws';

import createServer from './server';
import { connectMuxClient } from './test/mux-client';

async function waitConnected(server, id) {
    for (let i = 0; i < 50; i += 1) {
        const res = await request(server).get('/api/tunnels/' + id + '/status');
        if (res.statusCode === 200 && res.body.connected_sockets === 1) {
            return;
        }
        await new Promise(resolve => setTimeout(resolve, 20));
    }
    throw new Error('tunnel session did not connect');
}

describe('Server', () => {
    it('server starts and stops', async () => {
        const server = createServer();
        await new Promise(resolve => server.listen(resolve));
        await new Promise(resolve => server.close(resolve));
    });

    it('should redirect root requests to landing page', async () => {
        const server = createServer();
        const res = await request(server).get('/');
        assert.equal('https://localtunnel.github.io/www/', res.headers.location);
    });

    it('should support custom base domains', async () => {
        const server = createServer({
            domain: 'domain.example.com',
        });

        const res = await request(server).get('/');
        assert.equal('https://localtunnel.github.io/www/', res.headers.location);
    });

    it('reject long domain name requests', async () => {
        const server = createServer();
        const res = await request(server).get('/thisdomainisoutsidethesizeofwhatweallowwhichissixtythreecharacters');
        assert.equal(res.body.message, 'Invalid subdomain. Subdomains must be lowercase and between 4 and 63 alphanumeric characters.');
    });

    it('should upgrade websocket requests', async () => {
        const hostname = 'websocket-test';
        const server = createServer({
            domain: 'example.com',
        });
        await new Promise(resolve => server.listen(resolve));

        const res = await request(server).get('/websocket-test');
        const localTunnelPort = res.body.port;

        const wss = await new Promise((resolve) => {
            const wsServer = new WebSocketServer({ port: 0 }, () => {
                resolve(wsServer);
            });
        });

        const websocketServerPort = wss.address().port;

        const peer = await connectMuxClient(localTunnelPort, (stream) => {
            const wsSocket = net.createConnection({ port: websocketServerPort });
            wsSocket.on('error', () => stream.destroy());
            stream.pipe(wsSocket).pipe(stream);
        });
        await waitConnected(server, hostname);

        wss.once('connection', (ws) => {
            ws.once('message', (message) => {
                ws.send(message);
            });
        });

        const ws = new WebSocket('http://localhost:' + server.address().port, {
            headers: {
                host: hostname + '.example.com',
            }
        });

        ws.on('open', () => {
            ws.send('something');
        });

        await new Promise((resolve, reject) => {
            ws.once('error', reject);
            ws.once('message', (msg) => {
                assert.equal(msg, 'something');
                resolve();
            });
        });

        ws.close();
        peer.close();
        wss.close();
        await new Promise(resolve => server.close(resolve));
    });

    it('should multiplex requests over one client session', async () => {
        const server = createServer({
            domain: 'example.com',
        });
        await new Promise(resolve => server.listen(resolve));

        const created = await request(server).get('/mux-app');
        assert.equal(created.statusCode, 200);

        const local = http.createServer((req, res) => {
            if (req.url === '/slow') {
                setTimeout(() => res.end('slow'), 200);
                return;
            }
            res.end('fast');
        });
        await new Promise(resolve => local.listen(resolve));

        const peer = await connectMuxClient(created.body.port, (stream) => {
            const sock = net.connect({ port: local.address().port, host: '127.0.0.1' });
            sock.setNoDelay(true);
            sock.on('error', () => stream.destroy());
            stream.pipe(sock).pipe(stream);
        });
        await waitConnected(server, 'mux-app');

        const hit = (path) => new Promise((resolve, reject) => {
            const req = http.get({
                host: '127.0.0.1',
                port: server.address().port,
                path,
                headers: {
                    host: 'mux-app.example.com',
                },
            }, (res) => {
                const chunks = [];
                res.on('data', (chunk) => chunks.push(chunk));
                res.on('end', () => {
                    resolve({
                        status: res.statusCode,
                        body: Buffer.concat(chunks).toString(),
                    });
                });
            });
            req.on('error', reject);
        });

        const paths = ['/slow'].concat(Array.from({ length: 20 }, (_, i) => '/fast-' + i));
        const results = await Promise.all(paths.map(hit));
        assert.equal(results[0].status, 200);
        assert.equal(results[0].body, 'slow');
        for (const result of results.slice(1)) {
            assert.equal(result.status, 200);
            assert.equal(result.body, 'fast');
        }

        peer.close();
        await new Promise(resolve => local.close(resolve));
        await new Promise(resolve => server.close(resolve));
    }).timeout(10000);

    it('should support the /api/tunnels/:id/status endpoint', async () => {
        const server = createServer();
        await new Promise(resolve => server.listen(resolve));

        // no such tunnel yet
        const res = await request(server).get('/api/tunnels/foobar-test/status');
        assert.equal(res.statusCode, 404);

        // request a new client called foobar-test
        {
            const res = await request(server).get('/foobar-test');
        }

        {
            const res = await request(server).get('/api/tunnels/foobar-test/status');
            assert.equal(res.statusCode, 200);
            assert.deepEqual(res.body, {
                connected_sockets: 0,
            });
        }

        await new Promise(resolve => server.close(resolve));
    });

    it('should require a bearer token when one is configured', async () => {
        const server = createServer({
            token: 'secret',
        });
        await new Promise(resolve => server.listen(resolve));

        const denied = await request(server).get('/foobar-test');
        assert.equal(denied.statusCode, 401);
        assert.equal(denied.body.message, 'Unauthorized');

        const created = await request(server)
            .get('/foobar-test')
            .set('Authorization', 'Bearer secret');
        assert.equal(created.statusCode, 200);
        assert.equal(created.body.id, 'foobar-test');

        await new Promise(resolve => server.close(resolve));
    });

    it('should reject a taken name and allow the owner token to replace it', async () => {
        const server = createServer();
        await new Promise(resolve => server.listen(resolve));

        const first = await request(server).get('/same-name');
        assert.equal(first.body.id, 'same-name');
        assert.ok(first.body.owner_token);

        const denied = await request(server).get('/same-name');
        assert.equal(denied.statusCode, 409);
        assert.equal(denied.body.message, 'Subdomain "same-name" is already in use.');

        const replaced = await request(server)
            .get('/same-name')
            .set('X-Tunnel-Owner', first.body.owner_token);
        assert.equal(replaced.statusCode, 200);
        assert.equal(replaced.body.id, 'same-name');
        assert.equal(replaced.body.owner_token, first.body.owner_token);

        const status = await request(server).get('/api/status');
        assert.equal(status.body.tunnels, 1);

        await new Promise(resolve => server.close(resolve));
    });
});