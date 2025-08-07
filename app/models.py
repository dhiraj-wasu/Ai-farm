from pydantic import BaseModel

class TaskRequest(BaseModel):
    type: str  # "text" or "image"
    data: str  # base64 string or plain text
