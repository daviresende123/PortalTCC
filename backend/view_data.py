#!/usr/bin/env python3
"""
Script para visualizar dados do Delta Lake de forma amigável
"""
import os
import sys
from pathlib import Path
from datetime import datetime
from deltalake import DeltaTable
import pandas as pd


def print_separator(char="=", length=80):
    """Imprime uma linha separadora"""
    print(char * length)


def print_header(text):
    """Imprime um cabeçalho formatado"""
    print_separator()
    print(f"  {text}")
    print_separator()


def format_timestamp(ts):
    """Formata timestamp para formato legível"""
    if pd.isna(ts):
        return "N/A"
    dt = pd.to_datetime(ts, unit='ms')
    return dt.strftime('%d/%m/%Y %H:%M:%S')


def main():
    # Caminho para os dados
    data_path = Path(__file__).parent / "data" / "delta_table"

    print("\n🔍 VISUALIZADOR DE DADOS - PORTAL TCC\n")

    # Verificar se a tabela existe
    if not data_path.exists():
        print("❌ Nenhum dado encontrado!")
        print(f"   Caminho esperado: {data_path}")
        print("\n💡 Faça upload de um arquivo CSV primeiro.\n")
        sys.exit(1)

    try:
        # Carregar a tabela Delta
        dt = DeltaTable(str(data_path))
        df = dt.to_pandas()

        # Formatar CPF corretamente (se existir coluna CPF)
        if 'CPF' in df.columns:
            df['CPF'] = df['CPF'].apply(lambda x: f"{int(x):011d}" if pd.notna(x) else "N/A")
            df['CPF'] = df['CPF'].apply(lambda x: f"{x[:3]}.{x[3:6]}.{x[6:9]}-{x[9:]}" if x != "N/A" else x)

        # 1. INFORMAÇÕES GERAIS
        print_header("📊 INFORMAÇÕES GERAIS")
        print(f"Total de registros: {len(df)}")
        print(f"Total de colunas: {len(df.columns)}")
        print(f"Colunas: {', '.join(df.columns)}")
        print(f"Localização: {data_path}")
        print()

        # 2. DADOS DA TABELA
        print_header("📋 DADOS ARMAZENADOS")

        # Configurar pandas para melhor visualização
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)

        if len(df) > 0:
            print(df.to_string(index=True))
        else:
            print("(Tabela vazia)")
        print()

        # 3. ESTATÍSTICAS
        print_header("📈 ESTATÍSTICAS")

        # Tipos de dados
        print("Tipos de dados:")
        for col, dtype in df.dtypes.items():
            print(f"  • {col}: {dtype}")
        print()

        # Estatísticas numéricas se houver
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            print("Estatísticas numéricas:")
            print(df[numeric_cols].describe().to_string())
            print()

        # 4. HISTÓRICO DE VERSÕES (Time Travel)
        print_header("🕐 HISTÓRICO DE VERSÕES (TIME TRAVEL)")

        history = dt.history()
        if history and len(history) > 0:
            print(f"Total de versões: {len(history)}\n")

            for idx, version in enumerate(history):
                # version é um dicionário
                version_num = version.get('version', idx)
                timestamp = version.get('timestamp', 0)
                operation = version.get('operation', 'N/A')

                print(f"Versão {version_num}:")
                print(f"  Data/Hora: {format_timestamp(timestamp)}")
                print(f"  Operação: {operation}")

                # Tentar mostrar mais informações se disponível
                if 'operationMetrics' in version and version['operationMetrics']:
                    metrics = version['operationMetrics']
                    if isinstance(metrics, dict):
                        if 'numOutputRows' in metrics:
                            print(f"  Linhas adicionadas: {metrics['numOutputRows']}")
                        if 'numFiles' in metrics:
                            print(f"  Arquivos: {metrics['numFiles']}")
                print()
        else:
            print("Nenhum histórico disponível")
            print()

        # 5. RESUMO FINAL
        print_separator()
        print("✅ Visualização concluída com sucesso!")
        print(f"💾 Total de {len(df)} registro(s) no Delta Lake")
        print_separator()
        print()

    except Exception as e:
        print(f"❌ Erro ao ler dados: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
