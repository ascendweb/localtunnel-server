import http from 'http';
import net from 'net';
import assert from 'assert';

import TunnelAgent from './TunnelAgent';
import { connectMuxClient } from '../test/mux-client';

function once(emitter, event) {
    return new Promise(resolve => emitter.once(event, resolve));
}

describe('TunnelAgent', () => {
    it('should create an empty agent', async () => {
        const agent = new TunnelAgent();
        assert.equal(agent.started, false);

        const info = await agent.listen();
        assert.ok(info.port > 0);
        agent.destroy();
    });

    it('should create a new server and accept connections', async () => {
        const agent = new TunnelAgent();
        assert.equal(agent.started, false);

        const info = await agent.listen();
        const online = once(agent, 'online');
        const received = new Promise((resolve) => {
            connectMuxClient(info.port, (stream) => {
                stream.once('data', (chunk) => resolve(chunk.toString()));
            });
        });
        await online;

        const agentSock = await new Promise((resolve, reject) => {
            agent.createConnection({}, (err, sock) => {
                if (err) {
                    reject(err);
                }
                resolve(sock);
            });
        });

        agentSock.write('foo');
        assert.equal(await received, 'foo');
        agent.destroy();
    });

    it('should replace the session when the client reconnects', async () => {
        const agent = new TunnelAgent();
        const info = await agent.listen();
        let offline = false;
        agent.on('offline', () => {
            offline = true;
        });

        const firstOnline = once(agent, 'online');
        const firstPromise = connectMuxClient(info.port, (stream) => {
            stream.resume();
        });
        await firstOnline;
        const first = await firstPromise;

        let echoed;
        const gotEcho = new Promise((resolve) => {
            echoed = resolve;
        });
        const firstClosed = once(first.socket, 'close');
        const secondOnline = once(agent, 'online');
        const secondPromise = connectMuxClient(info.port, (stream) => {
            stream.once('data', (chunk) => echoed(chunk.toString()));
        });
        await firstClosed;
        await secondOnline;
        const second = await secondPromise;
        assert.equal(offline, false);

        const agentSock = await new Promise((resolve, reject) => {
            agent.createConnection({}, (err, sock) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(sock);
            });
        });
        agentSock.write('bar');
        assert.equal(await gotEcho, 'bar');

        agent.destroy();
        second.close();
    });

    it('should fail createConnection when the session is down', async () => {
        const agent = new TunnelAgent();
        await agent.listen();

        const err = await new Promise((resolve) => {
            agent.createConnection({}, (error) => resolve(error));
        });
        assert.ok(err);
        agent.destroy();
    });

    it('should emit online when the client session connects', async () => {
        const agent = new TunnelAgent();
        const info = await agent.listen();

        const onlinePromise = once(agent, 'online');
        const peerPromise = connectMuxClient(info.port, (stream) => {
            stream.resume();
        });

        await onlinePromise;
        const peer = await peerPromise;
        agent.destroy();
        peer.close();
    });

    it('should emit offline event when socket disconnects', async () => {
        const agent = new TunnelAgent();
        const info = await agent.listen();

        const onlinePromise = once(agent, 'online');
        const peerPromise = connectMuxClient(info.port, (stream) => {
            stream.resume();
        });
        await onlinePromise;
        const peer = await peerPromise;

        const offlinePromise = once(agent, 'offline');
        peer.close();
        await offlinePromise;
        agent.destroy();
    });

    it('should keep the tunnel online when a second handshake fails', async () => {
        const agent = new TunnelAgent();
        const info = await agent.listen();
        let offline = false;
        agent.on('offline', () => {
            offline = true;
        });

        const onlinePromise = once(agent, 'online');
        const peerPromise = connectMuxClient(info.port, (stream) => {
            stream.resume();
        });
        await onlinePromise;
        const peer = await peerPromise;

        try {
            await new Promise((resolve, reject) => {
                const raw = net.connect({ port: info.port }, () => {
                    raw.end();
                    resolve();
                });
                raw.on('error', reject);
            });
            await new Promise(resolve => setTimeout(resolve, 100));
            assert.equal(offline, false);
            assert.equal(agent.stats().connectedSockets, 1);
        } finally {
            agent.destroy();
            peer.close();
        }
    });

    it('should error an http request', async () => {
        class ErrorAgent extends http.Agent {
            constructor() {
                super();
            }
        
            createConnection(options, cb) {
                cb(new Error('foo'));
            }
        }

        const agent = new ErrorAgent();

        const opt = {
            host: 'localhost',
            port: 1234,
            path: '/',
            agent: agent,
        };

        const err = await new Promise((resolve) => {
            const req = http.get(opt, (res) => {});
            req.once('error', resolve);
        });
        assert.equal(err.message, 'foo');
    });

    it('should return stats', async () => {
        const agent = new TunnelAgent();
        assert.deepEqual(agent.stats(), {
            connectedSockets: 0,
        });
    });
});
