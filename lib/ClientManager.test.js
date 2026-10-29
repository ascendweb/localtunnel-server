import assert from 'assert';

import ClientManager from './ClientManager';
import { connectMuxClient, waitFor } from '../test/mux-client';

describe('ClientManager', () => {
    it('should construct with no tunnels', () => {
        const manager = new ClientManager();
        assert.equal(manager.stats.tunnels, 0);
    });

    it('should create a new client with random id', async () => {
        const manager = new ClientManager();
        const client = await manager.newClient();
        assert(manager.hasClient(client.id));
        manager.removeClient(client.id);
    });

    it('should create a new client with id', async () => {
        const manager = new ClientManager();
        const client = await manager.newClient('foobar');
        assert(manager.hasClient('foobar'));
        manager.removeClient('foobar');
    });

    it('should reject a name that is already in use', async () => {
        const manager = new ClientManager();
        const created = await manager.newClient('foobar');
        await assert.rejects(
            () => manager.newClient('foobar'),
            (err) => err.statusCode === 409 && /already in use/.test(err.message),
        );
        assert.equal(manager.stats.tunnels, 1);
        assert.ok(created.owner_token);
        manager.removeClient('foobar');
    });

    it('should let the owner token replace a name', async () => {
        const manager = new ClientManager();
        const created = await manager.newClient('foobar');
        const first = manager.getClient('foobar');
        const replaced = await manager.newClient('foobar', { ownerToken: created.owner_token });
        assert.equal(replaced.id, 'foobar');
        assert.equal(replaced.owner_token, created.owner_token);
        assert.equal(manager.stats.tunnels, 1);
        assert.notEqual(manager.getClient('foobar'), first);
        manager.removeClient('foobar');
    });

    it('should reject a different owner token', async () => {
        const manager = new ClientManager();
        const created = await manager.newClient('foobar');
        await assert.rejects(
            () => manager.newClient('foobar', { ownerToken: 'someone-else' }),
            (err) => err.statusCode === 409,
        );
        assert.equal(manager.getClient('foobar').ownerToken, created.owner_token);
        manager.removeClient('foobar');
    });

    it('should remove client once it goes offline', async () => {
        const manager = new ClientManager();
        const client = await manager.newClient('foobar');

        const peerPromise = connectMuxClient(client.port, (stream) => {
            stream.resume();
        });
        await waitFor(() => {
            const current = manager.getClient('foobar');
            return current && current.stats().connectedSockets === 1;
        });
        const peer = await peerPromise;
        peer.close();

        // should still have client - grace period has not expired
        assert(manager.hasClient('foobar'));

        // wait past grace period (1s)
        await new Promise(resolve => setTimeout(resolve, 1500));
        assert(!manager.hasClient('foobar'));
    }).timeout(5000);

    it('should remove correct client once it goes offline', async () => {
        const manager = new ClientManager();
        const clientFoo = await manager.newClient('foo');
        await manager.newClient('bar');

        const peerPromise = connectMuxClient(clientFoo.port, (stream) => {
            stream.resume();
        });
        await waitFor(() => {
            const current = manager.getClient('foo');
            return current && current.stats().connectedSockets === 1;
        });

        await new Promise(resolve => setTimeout(resolve, 1500));

        // foo should still be ok
        assert(manager.hasClient('foo'));

        // clientBar shound be removed - nothing connected to it
        assert(!manager.hasClient('bar'));

        const peer = await peerPromise;
        manager.removeClient('foo');
        peer.close();
    }).timeout(5000);

    it('should remove clients if they do not connect within 5 seconds', async () => {
        const manager = new ClientManager();
        const clientFoo = await manager.newClient('foo');
        assert(manager.hasClient('foo'));

        // wait past grace period (1s)
        await new Promise(resolve => setTimeout(resolve, 1500));
        assert(!manager.hasClient('foo'));
    }).timeout(5000);
});
