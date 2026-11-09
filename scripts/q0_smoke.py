#!/usr/bin/env python3
"""Real broker and independent Rust SDK processes; build binaries first."""
import os
from pathlib import Path
import selectors
import signal
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def line_within(pipe, seconds=10):
    with selectors.DefaultSelector() as selector:
        selector.register(pipe, selectors.EVENT_READ)
        if not selector.select(seconds):
            raise RuntimeError("process did not become ready")
        line = pipe.readline().strip()
        if not line:
            raise RuntimeError("process exited before readiness")
        return line


def main():
    processes = []
    try:
        server = subprocess.Popen(
            [str(ROOT / "target/debug/rovqet"), "--listen", "127.0.0.1:0"],
            stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        processes.append(server)
        ready = line_within(server.stderr)
        assert "listening on 127.0.0.1:" in ready, ready
        endpoint = ready.rsplit(" ", 1)[1]
        env = dict(os.environ, ROVQET_ADDR=endpoint)
        command = [str(ROOT / "target/debug/examples/q0")]
        subscribers = []
        for _ in range(2):
            subscriber = subprocess.Popen(command + ["sub", "demo"], env=env,
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(subscriber)
            assert line_within(subscriber.stdout).startswith("SUBSCRIBED ")
            subscribers.append(subscriber)
        publisher = subprocess.run(command + ["pub", "demo", "independent-process-proof"],
                                   env=env, capture_output=True, text=True, timeout=10, check=True)
        assert publisher.stdout.strip() == "Admitted", publisher
        for subscriber in subscribers:
            output, error = subscriber.communicate(timeout=10)
            assert subscriber.returncode == 0, error
            assert output.strip() == "demo: independent-process-proof", output
        server.send_signal(signal.SIGINT)
        server.communicate(timeout=10)
        assert server.returncode == 0
        print("PASS: independent publisher and two subscribers exchanged Q0 over TCP; clean shutdown")
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate()


if __name__ == "__main__":
    main()
