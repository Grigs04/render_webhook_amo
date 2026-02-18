from fastapi import FastAPI
from API.webhooks import router as webhook_router

app = FastAPI()

app.include_router(webhook_router)
