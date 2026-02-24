"""Rotas de upload de arquivos."""
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession
from db.connection import get_db
from models.schemas import UploadResponse
from services.csv_service import CSVService
from services.db_service import DatabaseService
from config import settings
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["upload"])


@router.post("/upload", response_model=UploadResponse)
async def upload_csv(
    csvFile: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Endpoint para upload de arquivo CSV.

    Args:
        csvFile: Arquivo CSV enviado
        db: Sessão assíncrona do PostgreSQL (injetada)

    Returns:
        UploadResponse com resultado do processamento
    """
    try:
        # Validar extensão do arquivo
        if not csvFile.filename.lower().endswith(".csv"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Apenas arquivos CSV são permitidos",
            )

        # Ler conteúdo do arquivo
        file_content = await csvFile.read()

        # Validar tamanho
        if len(file_content) > settings.max_file_size_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"Arquivo excede o tamanho máximo de {settings.max_file_size_mb}MB",
            )

        # Processar CSV
        logger.info(f"Processando arquivo: {csvFile.filename}")
        df = CSVService.validate_and_parse_csv(file_content, csvFile.filename)
        df = CSVService.clean_dataframe(df)

        # Salvar no PostgreSQL
        db_service = DatabaseService(db)
        rows_saved, file_id = await db_service.save_dataframe(df, csvFile.filename)

        logger.info(f"Upload concluído: {rows_saved} linhas salvas")

        # Gerar embeddings para o ChromaDB (falha não bloqueia o upload)
        try:
            from services.embedding_service import embed_records

            records_for_embedding = df.to_dict(orient="records")
            embedded_count = await embed_records(
                records=records_for_embedding,
                file_id=file_id,
                file_name=csvFile.filename,
            )
            logger.info(f"Embeddings gerados: {embedded_count} documentos")
        except Exception as e:
            logger.warning(f"Falha ao gerar embeddings (upload continuou): {e}")

        return UploadResponse(
            success=True,
            message="Arquivo processado e salvo com sucesso",
            rows_processed=rows_saved,
            file_name=csvFile.filename,
        )

    except ValueError as e:
        logger.warning(f"Erro de validação: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        logger.error(f"Erro ao processar upload: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao processar arquivo: {str(e)}",
        )


@router.get("/table-info")
async def get_table_info(db: AsyncSession = Depends(get_db)):
    """Retorna estatísticas do banco de dados PostgreSQL."""
    try:
        db_service = DatabaseService(db)
        return await db_service.get_stats()
    except Exception as e:
        logger.error(f"Erro ao obter informações do banco: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        )
