import { Agent } from 'http';
import http2 from 'http2';
import net from 'net';
import log from 'book';
import Debug from 'debug';

// One TCP session per tunnel. Each proxied request or websocket is an
// HTTP/2 CONNECT stream opened by this server. The Ascend client accepts
// the stream and dials localhost. A slow response blocks only its stream.
const MAX_CONCURRENT_STREAMS = 256;
const INITIAL_WINDOW_SIZE = 1024 * 1024;
const STREAM_OPEN_TIMEOUT_MS = 10000;
const HANDSHAKE_TIMEOUT_MS = 5000;
const SOCKET_KEEPALIVE_DELAY_MS = 15000;

// http.request expects a net.Socket. CONNECT streams are duplexes with the
// same read/write behavior, plus a few socket methods the agent calls.
function asSocket(stream) {
    for (const name of ['setNoDelay', 'setKeepAlive', 'ref', 'unref']) {
        if (typeof stream[name] !== 'function') {
            stream[name] = () => stream;
        }
    }
    if (!stream.listenerCount('error')) {
        stream.on('error', () => {});
    }
    return stream;
}

// Implements an http.Agent interface over one multiplexed client session.
class TunnelAgent extends Agent {
    constructor(options = {}) {
        super({
            keepAlive: false,
            maxSockets: Infinity,
        });

        this.debug = Debug(`lt:TunnelAgent[${options.clientId}]`);
        this.minPort = options.minPort;
        this.maxPort = options.maxPort;

        this.server = net.createServer();
        this.started = false;
        this.closed = false;
        this.ready = false;
        this.connectedSockets = 0;
        this.generation = 0;
        this.session = null;
        this.socket = null;
        this.pending = new Set();
    }

    stats() {
        return {
            connectedSockets: this.connectedSockets,
        };
    }

    listen() {
        const server = this.server;
        if (this.started) {
            throw new Error('already started');
        }
        this.started = true;

        const setupListeners = () => {
            server.on('close', this._onClose.bind(this));
            server.on('connection', this._onConnection.bind(this));
            server.on('error', (err) => {
                if (err.code == 'ECONNRESET' || err.code == 'ETIMEDOUT') {
                    return;
                }
                log.error(err);
            });
        };

        return new Promise((resolve, reject) => {
            if (!this.minPort || !this.maxPort) {
                server.listen(() => {
                    const port = server.address().port;
                    this.debug('tcp server listening on port: %d', port);
                    setupListeners();
                    resolve({
                        port: port,
                    });
                });
                return;
            }

            const maxRetries = 100;
            let attempts = 0;

            const tryListen = () => {
                if (attempts >= maxRetries) {
                    reject(new Error('Could not find a free port in range'));
                    return;
                }
                attempts += 1;

                const port = Math.floor(Math.random() * (this.maxPort - this.minPort + 1)) + parseInt(this.minPort, 10);

                const onError = (err) => {
                    server.removeListener('error', onError);
                    if (err.code === 'EADDRINUSE') {
                        this.debug('port %d in use, retrying...', port);
                        server.close();
                        setImmediate(tryListen);
                    } else {
                        reject(err);
                    }
                };

                server.once('error', onError);

                server.listen(port, () => {
                    server.removeListener('error', onError);
                    const chosenPort = server.address().port;
                    this.debug('tcp server listening on port: %d', chosenPort);
                    setupListeners();
                    resolve({
                        port: chosenPort,
                    });
                });
            };

            tryListen();
        });
    }

    _onClose() {
        this.closed = true;
        this.debug('closed tcp socket');
        this._retireActive();
        for (const pending of this.pending) {
            pending.destroy();
        }
        this.pending.clear();
        this.emit('end');
    }

    // A client TCP connection. It becomes the tunnel session only after the
    // HTTP/2 handshake, so a stray connection cannot replace a live one.
    _onConnection(socket) {
        if (this.closed) {
            socket.destroy();
            return;
        }

        socket.setKeepAlive(true, SOCKET_KEEPALIVE_DELAY_MS);
        socket.setNoDelay(true);
        socket.on('error', () => {
            if (!socket.destroyed) {
                socket.destroy();
            }
        });

        let session;
        try {
            session = http2.connect('http://tunnel.internal', {
                createConnection: () => socket,
                settings: {
                    initialWindowSize: INITIAL_WINDOW_SIZE,
                    maxConcurrentStreams: MAX_CONCURRENT_STREAMS,
                },
            });
        } catch (err) {
            this.debug('http2 connect failed %s', err.message);
            socket.destroy();
            return;
        }

        const pending = {
            destroy() {
                session.destroy();
                if (!socket.destroyed) {
                    socket.destroy();
                }
            },
        };
        this.pending.add(pending);

        let dropped = false;
        let handshakeTimer;
        const discardPending = () => {
            if (dropped || this.session === session) {
                return;
            }
            dropped = true;
            clearTimeout(handshakeTimer);
            this.pending.delete(pending);
            pending.destroy();
        };

        // 'connect' fires before the peer speaks HTTP/2. remoteSettings is the
        // SETTINGS frame from a real client, which a stray socket never sends.
        session.on('error', discardPending);
        session.on('close', discardPending);
        socket.on('close', discardPending);
        handshakeTimer = setTimeout(discardPending, HANDSHAKE_TIMEOUT_MS);
        handshakeTimer.unref();
        session.once('remoteSettings', () => {
            if (dropped) {
                return;
            }
            dropped = true;
            clearTimeout(handshakeTimer);
            this.pending.delete(pending);
            if (this.closed) {
                pending.destroy();
                return;
            }
            this._adopt(session, socket);
        });
    }

    _adopt(session, socket) {
        this._retireActive();
        const generation = this.generation;
        this.session = session;
        this.socket = socket;
        this.ready = true;
        this.connectedSockets = 1;
        this.debug('session online');

        const fail = () => {
            if (this.generation !== generation || this.closed) {
                return;
            }
            this.debug('session offline');
            this._retireActive();
            this.emit('offline');
        };

        session.on('close', fail);
        socket.on('close', fail);
        session.on('error', () => {});
        this.emit('online');
    }

    _retireActive() {
        const session = this.session;
        const socket = this.socket;
        this.generation += 1;
        this.session = null;
        this.socket = null;
        this.ready = false;
        this.connectedSockets = 0;
        if (session) {
            session.destroy();
        }
        if (socket && !socket.destroyed) {
            socket.destroy();
        }
    }

    // Open a CONNECT stream for one proxied request or upgrade.
    // cb(err, socket)
    createConnection(options, cb) {
        const session = this.session;
        if (this.closed || !this.ready || !session || session.closed || session.destroyed) {
            cb(new Error('closed'));
            return;
        }

        this.debug('open stream');
        let stream;
        try {
            stream = session.request({
                ':method': 'CONNECT',
                ':authority': 'tunnel',
            });
        } catch (err) {
            cb(err);
            return;
        }

        let settled = false;
        const finish = (err) => {
            if (settled) {
                return;
            }
            settled = true;
            clearTimeout(timer);
            stream.removeListener('error', onError);
            if (err) {
                stream.destroy();
                cb(err);
                return;
            }
            cb(null, asSocket(stream));
        };
        const onError = (err) => finish(err || new Error('tunnel stream failed'));
        const timer = setTimeout(() => finish(new Error('tunnel stream timeout')), STREAM_OPEN_TIMEOUT_MS);
        timer.unref();

        stream.on('error', onError);
        stream.once('response', (headers) => {
            if (Number(headers[':status']) !== 200) {
                finish(new Error('tunnel stream refused'));
                return;
            }
            finish();
        });
    }

    destroy() {
        this.closed = true;
        for (const pending of this.pending) {
            pending.destroy();
        }
        this.pending.clear();
        this._retireActive();
        this.server.close();
        super.destroy();
    }
}

export default TunnelAgent;
