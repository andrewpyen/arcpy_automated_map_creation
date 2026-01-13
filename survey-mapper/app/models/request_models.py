from pydantic import BaseModel, Field

class JobStatusResponse(BaseModel):
    job_id: str = Field(..., description="Unique identifier for the submitted background job")
    status: str = Field(..., description="Status of the job: queued, processing, complete, or failed")

class HealthCheckResponse(BaseModel):
    status: str = Field(..., example="ok")
    message: str = Field(..., example="DOTProcessor Async API is healthy.")
