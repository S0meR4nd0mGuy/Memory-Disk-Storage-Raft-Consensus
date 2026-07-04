"""CLI client for the distributed key-value store

Similar to redis-cli, provides interactive shell for querying the cluster.
"""

import asyncio
import json
from typing import Any, Dict, Optional
from urllib import error, parse, request

from src.logging_config import kv_logger

logger_base = kv_logger("kvstore_base", "log_file.log")
logger_client = kv_logger("kvstore_client", "client/client_log.log", format_style="full")


class CLIClient:
    """
    Interactive CLI client
    
    Commands:
        GET <key>           - Retrieve value
        PUT <key> <value>   - Set value
        DELETE <key>        - Delete key
        SCAN <start> <end>  - Range scan
        QUIT                - Exit
    """

    def __init__(self, server_addr: str, server_port: int):
        self.server_addr = server_addr
        self.server_port = server_port
        self.base_url = f"http://{server_addr}:{server_port}"
        logger_base.info(f"Initialized CLI client connecting to {server_addr}:{server_port}")
        logger_client.info(f"Initialized CLI client connecting to {server_addr}:{server_port}")

    async def run(self) -> None:
        """Run interactive CLI loop"""
        logger_base.info("Starting CLI client")
        logger_client.info("Starting CLI client")
        print(f"Connected to {self.server_addr}:{self.server_port}")
        print("Commands: GET <key>, PUT <key> <value>, DELETE <key>, SCAN <start> <end>, QUIT")
        
        while True:
            try:
                command = input("> ").strip()
                
                if not command:
                    continue
                
                if command.upper() == "QUIT":
                    print("Goodbye!")
                    break
                
                await self.execute_command(command)
            
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")
                logger_client.error(f"Error executing command: {e}")

    async def execute_command(self, command: str) -> None:
        """Execute a command"""
        parts = command.split(maxsplit=2)
        op = parts[0].upper()
        
        if op == "GET" and len(parts) >= 2:
            key = parts[1]
            result = await self.request_json("GET", f"/get/{parse.quote(key, safe='')}")
            if "error" in result:
                print(f"ERROR {result['error']}")
            else:
                print(result.get("value"))
        
        elif op == "PUT" and len(parts) >= 3:
            key = parts[1]
            value = parts[2]
            result = await self.request_json(
                "PUT",
                "/put",
                body={"key": key, "value": value},
            )
            if result.get("status") == "ok":
                print("OK")
            else:
                print(f"ERROR {result.get('error', result)}")
        
        elif op == "DELETE" and len(parts) >= 2:
            key = parts[1]
            result = await self.request_json("DELETE", f"/delete/{parse.quote(key, safe='')}")
            if result.get("status") == "ok":
                print("OK")
            else:
                print(f"ERROR {result.get('error', result)}")
        
        elif op == "SCAN" and len(parts) >= 3:
            start = parts[1]
            end = parts[2]
            result = await self.request_json(
                "POST",
                "/scan",
                query={"start_key": start, "end_key": end},
            )
            if "error" in result:
                print(f"ERROR {result['error']}")
            else:
                for key, value in result.get("data", {}).items():
                    print(f"{key}: {value}")
        
        else:
            print("Unknown command. Try: GET, PUT, DELETE, SCAN, QUIT")

    async def request_json(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Send an HTTP request to the KV API and parse the JSON response."""
        return await asyncio.to_thread(self._request_json_sync, method, path, body, query)

    def _request_json_sync(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]],
        query: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        query_string = f"?{parse.urlencode(query)}" if query else ""
        url = f"{self.base_url}{path}{query_string}"
        payload = None
        headers = {}

        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(url, data=payload, headers=headers, method=method)

        try:
            with request.urlopen(req, timeout=5) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except error.HTTPError as exc:
            raw = exc.read().decode("utf-8")
            detail = json.loads(raw) if raw else {"detail": exc.reason}
            return {"error": detail, "status_code": exc.code}
        except error.URLError as exc:
            return {"error": str(exc.reason)}


async def main():
    """Entry point for CLI client"""
    import argparse
    
    parser = argparse.ArgumentParser(description="CLI client for distributed key-value store")
    parser.add_argument("--host", default="localhost", help="Server host")
    parser.add_argument("--port", type=int, default=8000, help="Server port")
    
    args = parser.parse_args()
    
    client = CLIClient(args.host, args.port)
    await client.run()


if __name__ == "__main__":
    asyncio.run(main())
