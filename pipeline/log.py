import time

_INICIO = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - _INICIO:7.1f}s] {msg}", flush=True)


def titulo(msg: str) -> None:
    print("\n" + "=" * 70 + f"\n{msg}\n" + "=" * 70, flush=True)
