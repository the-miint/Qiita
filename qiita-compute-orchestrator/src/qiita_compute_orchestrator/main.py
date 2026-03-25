from fastapi import FastAPI

app = FastAPI(title="qiita-compute-orchestrator")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "qiita-compute-orchestrator"}
