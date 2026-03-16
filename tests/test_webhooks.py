from fastapi.testclient import TestClient

import API.webhooks as webhooks
from main import app


def test_create_invoice_webhook(monkeypatch):
    async def fake_runner(order_id: int) -> dict:
        assert order_id == 123
        return {"status": "ok"}

    monkeypatch.setattr(webhooks, "runner", fake_runner)

    client = TestClient(app)
    response = client.post("/amo/invoice", data={"leads[add][0][id]": "123"})
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_invoice_status_webhook(monkeypatch):
    called = {"value": False}

    async def fake_checkker() -> None:
        called["value"] = True

    monkeypatch.setattr(webhooks, "checkker", fake_checkker)

    client = TestClient(app)
    response = client.post("/amo/invoice-status")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert called["value"] is True


def test_create_agreement_webhook(monkeypatch):
    async def fake_run(order_id: int) -> dict:
        assert order_id == 456
        return {"status": "ok", "file_name": "Договор №456 от 01.01.2026.docx"}

    monkeypatch.setattr(webhooks, "agreement_runner", fake_run)

    client = TestClient(app)
    response = client.post("/amo/agreement", data={"leads[add][0][id]": "456"})
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
