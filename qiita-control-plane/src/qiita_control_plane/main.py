from fastapi import FastAPI

app = FastAPI(title="qiita-control-plane")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "qiita-control-plane"}
