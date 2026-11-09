#!/usr/bin/env python3
"""Fetch the pinned official Linux amd64 release and verify its SHA-256."""
import argparse
import hashlib
import io
from pathlib import Path
import tarfile
import urllib.request

VERSION = "2.11.10"
SHA256 = "e99ddaf76e0de2cce90e22d6cbe78d52f003e5e97d45b934d287af2f867c864e"
URL = f"https://github.com/nats-io/nats-server/releases/download/v{VERSION}/nats-server-v{VERSION}-linux-amd64.tar.gz"


def install(destination):
    destination = Path(destination)
    if destination.exists():
        raise ValueError("destination already exists; refusing overwrite")
    with urllib.request.urlopen(URL, timeout=60) as response:
        data = response.read(32 * 1024 * 1024 + 1)
    if hashlib.sha256(data).hexdigest() != SHA256:
        raise ValueError("NATS release checksum mismatch")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        member = archive.getmember(f"nats-server-v{VERSION}-linux-amd64/nats-server")
        if not member.isfile() or member.size > 32 * 1024 * 1024:
            raise ValueError("unexpected NATS binary archive entry")
        binary = archive.extractfile(member).read()
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb") as output:
        output.write(binary)
    destination.chmod(0o755)
    return destination


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("/tmp/rovqet-nats-server-2.11.10"))
    print(install(parser.parse_args().output))
