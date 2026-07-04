"""HTTP API server for client requests"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from ..storage.storage import PersistentStorage
from typing import Optional, Any
from src.logging_config import kv_logger
from .handlers import get_handler, put_handler, delete_handler, scan_handler

logger_base = kv_logger("kvstore_base", "log_file.log")
logger_api = kv_logger("kvstore_api", "api/api_log.log", format_style="full")

class PutRequest(BaseModel):
    key: str
    value: str

class APIServer:
    def __init__(
        self,
        host: str,
        port: int,
        storage: Optional[PersistentStorage] = None,
        raft_node=None,
    ):
        self.host = host
        self.port = port
        self.storage = storage or getattr(raft_node, "storage", None)
        self.raft_node = raft_node
        self.app = FastAPI()
        self._setup_routes()

    def _setup_routes(self):
        @self.app.get("/get/{key}")
        async def get(key: str):
            value = await self._get(key)
            if value is None:
                raise HTTPException(status_code=404, detail="key not found")
            return {"value": value}
        
        @self.app.put("/put")
        async def put(body: PutRequest):
            result = await self._put(body.key, body.value)
            if result.get("status") != "ok":
                raise HTTPException(status_code=409, detail=result)
            return result

        @self.app.delete("/delete/{key}")
        async def delete(key: str):
            result = await self._delete(key)
            if result.get("status") != "ok":
                raise HTTPException(status_code=409, detail=result)
            return result

        @self.app.post("/scan")
        async def scan(start_key: Optional[str] = "", end_key: Optional[str] = "\xff"):
            return await self._scan(start_key, end_key)

    async def _get(self, key: str) -> Any:
        if self.raft_node is not None:
            result = await get_handler({"key": key}, self.raft_node)
            return result.get("value")
        return await self.storage.get(key)

    async def _put(self, key: str, value: Any) -> dict:
        if self.raft_node is not None:
            return await put_handler({"key": key, "value": value}, self.raft_node)
        await self.storage.put(key, value)
        return {"status": "ok"}

    async def _delete(self, key: str) -> dict:
        if self.raft_node is not None:
            return await delete_handler({"key": key}, self.raft_node)
        await self.storage.delete(key)
        return {"status": "ok"}

    async def _scan(self, start_key: str, end_key: str) -> dict:
        if self.raft_node is not None:
            return await scan_handler(
                {"start_key": start_key, "end_key": end_key},
                self.raft_node,
            )
        result = await self.storage.scan(start_key, end_key)
        return {"data": result}

    async def start(self):
        config = uvicorn.Config(self.app, host=self.host, port=self.port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()
