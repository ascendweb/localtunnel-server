import http2 from 'http2';
import net from 'net';

const MAX_CONCURRENT_STREAMS = 256;
const INITIAL_WINDOW_SIZE = 1024 * 1024;

// Test stand-in for the Ascend client: one TCP connection, HTTP/2 server,
// each CONNECT stream handed to onStream after the 200 response.
export function connectMuxClient(port, onStream) {
    return new Promise((resolve, reject) => {
        const socket = net.connect({ port });
        let settled = false;
        const fail = (err) => {
            if (settled) {
                return;
            }
            settled = true;
            reject(err);
        };

        socket.once('error', fail);
        socket.once('connect', () => {
            const h2 = http2.createServer({
                settings: {
                    maxConcurrentStreams: MAX_CONCURRENT_STREAMS,
                    initialWindowSize: INITIAL_WINDOW_SIZE,
                },
            });
            h2.on('error', () => {});
            h2.on('stream', (stream, headers) => {
                stream.on('error', () => {});
                if (headers[':method'] !== 'CONNECT') {
                    stream.close(http2.constants.NGHTTP2_REFUSED_STREAM);
                    return;
                }
                stream.respond({ ':status': 200 });
                if (onStream) {
                    onStream(stream, headers);
                }
            });
            h2.on('session', (session) => {
                session.on('error', () => {});
                if (settled) {
                    return;
                }
                settled = true;
                resolve({
                    socket,
                    session,
                    close() {
                        session.destroy();
                        if (!socket.destroyed) {
                            socket.destroy();
                        }
                        h2.close();
                    },
                });
            });
            h2.emit('connection', socket);
        });
    });
}

export function waitFor(fn, attempts = 50) {
    return new Promise((resolve, reject) => {
        const tick = (left) => {
            let ok = false;
            try {
                ok = fn();
            } catch (err) {
                reject(err);
                return;
            }
            if (ok) {
                resolve();
                return;
            }
            if (left <= 0) {
                reject(new Error('timed out'));
                return;
            }
            setTimeout(() => tick(left - 1), 20);
        };
        tick(attempts);
    });
}
