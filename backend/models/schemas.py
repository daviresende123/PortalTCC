"""Schemas Pydantic para validação de dados."""
from pydantic import BaseModel
from typing import Optional


class UploadResponse(BaseModel):
    """Resposta do endpoint de upload."""
    success: bool
    message: str
    rows_processed: Optional[int] = None
    file_name: Optional[str] = None


class ErrorResponse(BaseModel):
    """Resposta de erro."""
    success: bool = False
    error: str
    detail: Optional[str] = None
