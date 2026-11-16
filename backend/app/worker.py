"""Run the scanner API with a local, graceful stop-file control."""
import asyncio
import logging
from pathlib import Path

import uvicorn


ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / ".runtime"
STOP_FILE = RUNTIME / "backend.stop"


async def serve():
    RUNTIME.mkdir(exist_ok=True)
    STOP_FILE.unlink(missing_ok=True)
    server = uvicorn.Server(uvicorn.Config(
        "app.main:app", host="127.0.0.1", port=8000,
        proxy_headers=False, log_level="info",
    ))
    task = asyncio.create_task(server.serve(), name="uvicorn-server")
    try:
        while not task.done():
            if STOP_FILE.exists():
                server.should_exit = True
                break
            await asyncio.sleep(0.25)
        await task
    finally:
        server.should_exit = True
        if not task.done():
            await asyncio.shield(task)
        STOP_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())