"""Tests for the CLI client command layer."""

import pytest

from src.client.cli import CLIClient


class FakeClient(CLIClient):
    def __init__(self, responses):
        super().__init__("localhost", 8000)
        self.responses = responses
        self.calls = []

    async def request_json(self, method, path, body=None, query=None):
        self.calls.append((method, path, body, query))
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_client_get_prints_value(capsys):
    client = FakeClient([{"value": "bar"}])

    await client.execute_command("GET foo")

    assert client.calls == [("GET", "/get/foo", None, None)]
    assert capsys.readouterr().out == "bar\n"


@pytest.mark.asyncio
async def test_client_put_sends_json_body(capsys):
    client = FakeClient([{"status": "ok"}])

    await client.execute_command("PUT foo bar baz")

    assert client.calls == [("PUT", "/put", {"key": "foo", "value": "bar baz"}, None)]
    assert capsys.readouterr().out == "OK\n"


@pytest.mark.asyncio
async def test_client_delete_calls_delete_endpoint(capsys):
    client = FakeClient([{"status": "ok"}])

    await client.execute_command("DELETE foo")

    assert client.calls == [("DELETE", "/delete/foo", None, None)]
    assert capsys.readouterr().out == "OK\n"


@pytest.mark.asyncio
async def test_client_scan_prints_key_values(capsys):
    client = FakeClient([{"data": {"a": "1", "b": "2"}}])

    await client.execute_command("SCAN a z")

    assert client.calls == [
        ("POST", "/scan", None, {"start_key": "a", "end_key": "z"})
    ]
    assert capsys.readouterr().out == "a: 1\nb: 2\n"
