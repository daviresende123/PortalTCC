"""Rotas de upload de arquivos."""
from fastapi import APIRouter, UploadFile, File, HTTPException, status
from models.schemas import UploadResponse, ErrorResponse
from services.csv_service import CSVService
from services.delta_service import DeltaLakeService
from config import settings
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["upload"])

# Inicializar serviço Delta Lake
delta_service = DeltaLakeService(settings.delta_path)


@router.post("/upload", response_model=UploadResponse)
async def upload_csv(csvFile: UploadFile = File(...)):
    """
    Endpoint para upload de arquivo CSV.

    Args:
        csvFile: Arquivo CSV enviado

    Returns:
        UploadResponse com resultado do processamento
    """
    try:
        # Validar extensão do arquivo
        if not csvFile.filename.lower().endswith('.csv'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Apenas arquivos CSV são permitidos"
            )

        # Ler conteúdo do arquivo
        file_content = await csvFile.read()

        # Validar tamanho
        if len(file_content) > settings.max_file_size_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"Arquivo excede o tamanho máximo de {settings.max_file_size_mb}MB"
            )

        # Processar CSV
        logger.info(f"Processando arquivo: {csvFile.filename}")
        df = CSVService.validate_and_parse_csv(file_content, csvFile.filename)

        # Limpar dados
        df = CSVService.clean_dataframe(df)

        # Salvar no Delta Lake
        rows_saved = delta_service.save_dataframe(df, mode="append")

        logger.info(f"Upload concluído: {rows_saved} linhas salvas")

        return UploadResponse(
            success=True,
            message="Arquivo processado e salvo com sucesso",
            rows_processed=rows_saved,
            file_name=csvFile.filename
        )

    except ValueError as e:
        logger.warning(f"Erro de validação: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Erro ao processar upload: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao processar arquivo: {str(e)}"
        )


@router.get("/table-info")
async def get_table_info():
    """Retorna informações sobre a tabela Delta."""
    try:
        info = delta_service.get_table_info()
        return info
    except Exception as e:
        logger.error(f"Erro ao obter informações da tabela: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
