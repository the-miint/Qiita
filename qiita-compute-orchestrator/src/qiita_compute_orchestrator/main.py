from fastapi import FastAPI
from qiita_common.models import HealthResponse

app = FastAPI(title="qiita-compute-orchestrator")


@app.get("/health")
async def health() -> HealthResponse:
    return HealthResponse(status="ok", service="qiita-compute-orchestrator")
