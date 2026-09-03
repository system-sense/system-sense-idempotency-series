"""A Redis client in fifty lines of standard library.

Everything under scripts/ in this repository runs on the python3 that is already
on your machine — no virtualenv, no pip install, no requirements file. That is a
promise worth keeping, and RESP is a small enough protocol that keeping it is
cheaper than breaking it.

Commands go out as RESP arrays. Replies are identified by their first byte:

    +OK          simple string
    -ERR ...     error
    :42          integer
    $5\\r\\nhello  bulk string  ($-1 is nil)
    *2\\r\\n...    array        (*-1 is nil)

That is the whole protocol as far as this file is concerned.
"""
import socket


class RedisError(RuntimeError):
    pass


class Redis:
    def __init__(self, host="localhost", port=6379, timeout=30.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.f = self.sock.makefile("rb")

    def close(self):
        try:
            self.f.close()
            self.sock.close()
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def cmd(self, *args):
        out = bytearray(b"*%d\r\n" % len(args))
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            out += b"$%d\r\n" % len(b) + b + b"\r\n"
        self.sock.sendall(out)
        return self._read()

    def _read(self):
        line = self.f.readline()
        if not line:
            raise ConnectionError("redis closed the connection")
        tag, body = line[:1], line[1:-2]
        if tag == b"+":
            return body.decode()
        if tag == b":":
            return int(body)
        if tag == b"-":
            raise RedisError(body.decode())
        if tag == b"$":
            n = int(body)
            return None if n == -1 else self.f.read(n + 2)[:-2].decode(errors="replace")
        if tag == b"*":
            n = int(body)
            return None if n == -1 else [self._read() for _ in range(n)]
        raise ConnectionError(f"unexpected reply: {line!r}")


def pairs(flat: list) -> dict:
    """Redis returns hashes as a flat [k, v, k, v] list."""
    return dict(zip(flat[::2], flat[1::2])) if flat else {}
