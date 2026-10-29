import { randomBytes, timingSafeEqual } from 'crypto';
import { hri } from 'human-readable-ids';
import Debug from 'debug';

import Client from './Client';
import TunnelAgent from './TunnelAgent';

function newOwnerToken() {
    return randomBytes(24).toString('hex');
}

function tokensMatch(provided, expected) {
    if (!provided || !expected) {
        return false;
    }
    const left = Buffer.from(String(provided));
    const right = Buffer.from(String(expected));
    if (left.length !== right.length) {
        return false;
    }
    return timingSafeEqual(left, right);
}

// Manage sets of clients
//
// A client is a "user session" established to service a remote localtunnel client
class ClientManager {
    constructor(opt) {
        this.opt = opt || {};

        // id -> client instance
        this.clients = new Map();

        // statistics
        this.stats = {
            tunnels: 0
        };

        this.debug = Debug('lt:ClientManager');
    }

    // create a new tunnel with `id`
    // a name that is already registered returns 409
    // the client that received owner_token can send it back and replace that tunnel
    async newClient(id, meta) {
        const stats = this.stats;
        meta = meta || {};

        if (!id) {
            id = hri.random();
        }

        const existing = this.clients.get(id);
        let ownerToken = newOwnerToken();
        let replaced = false;
        if (existing) {
            if (!tokensMatch(meta.ownerToken, existing.ownerToken)) {
                const err = new Error(`Subdomain "${id}" is already in use.`);
                err.statusCode = 409;
                throw err;
            }
            ownerToken = existing.ownerToken;
            replaced = true;
            this.removeClient(id);
        }

        const agent = new TunnelAgent({
            clientId: id,
            minPort: this.opt.min_port,
            maxPort: this.opt.max_port,
        });

        const client = new Client({
            id,
            agent,
        });
        client.ownerToken = ownerToken;

        // add to clients map immediately
        // avoiding races with other clients requesting same id
        this.clients.set(id, client);

        client.once('close', () => {
            this.removeClient(id);
        });

        // try/catch used here to remove client id
        try {
            const info = await agent.listen();
            if (this.clients.get(id) !== client) {
                throw new Error('tunnel was replaced before it started');
            }
            client.counted = true;
            stats.tunnels += 1;
            return {
                id: id,
                port: info.port,
                owner_token: ownerToken,
                replaced: replaced,
            };
        }
        catch (err) {
            if (this.clients.get(id) === client) {
                this.removeClient(id);
            } else {
                client.close();
            }
            // rethrow error for upstream to handle
            throw err;
        }
    }

    removeClient(id) {
        this.debug('removing client: %s', id);
        const client = this.clients.get(id);
        if (!client) {
            return;
        }
        this.clients.delete(id);
        if (client.counted) {
            this.stats.tunnels -= 1;
            client.counted = false;
        }
        client.close();
    }

    hasClient(id) {
        return this.clients.has(id);
    }

    getClient(id) {
        return this.clients.get(id);
    }
}

export default ClientManager;
