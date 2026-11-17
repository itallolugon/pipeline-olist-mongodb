"""Pipeline de ingestão Olist -> MongoDB (Avaliação N3).

Uso:
    python main.py                      # roda tudo: captura, limpeza, agregados, carga e análises
    python main.py --etapa carga        # roda só uma etapa
    python main.py --origem C:/csv      # usa CSV já baixados em vez de baixar
"""
import argparse
import sys
import time

from pipeline.log import log, titulo

ETAPAS = ["captura", "limpeza", "agregados", "carga", "analises"]


def main() -> int:
    ap = argparse.ArgumentParser(description="Pipeline Olist -> MongoDB")
    ap.add_argument("--etapa", choices=ETAPAS, help="executa só esta etapa")
    ap.add_argument("--a-partir-de", choices=ETAPAS, dest="inicio", help="executa desta etapa em diante")
    ap.add_argument("--origem", help="pasta com os 9 CSV já baixados (pula o download)")
    args = ap.parse_args()

    if args.etapa:
        etapas = [args.etapa]
    elif args.inicio:
        etapas = ETAPAS[ETAPAS.index(args.inicio):]
    else:
        etapas = ETAPAS

    inicio = time.time()
    try:
        if "captura" in etapas:
            titulo("ETAPA 1 - CAPTURA (Kaggle / espelho oficial -> camada raw)")
            from pipeline import etapa1_captura
            etapa1_captura.executar(args.origem)
        if "limpeza" in etapas:
            titulo("ETAPAS 2 e 3 - VALIDAÇÃO, LIMPEZA E PARQUET (Spark -> camada staging)")
            from pipeline import etapa2_limpeza
            etapa2_limpeza.executar()
        if "agregados" in etapas:
            titulo("ETAPA 4 - TRANSFORMAÇÃO EM AGREGADOS (Spark -> camada gold)")
            from pipeline import etapa3_agregados
            etapa3_agregados.executar()
        if "carga" in etapas:
            titulo("ETAPA 5 - CARGA NO MONGODB (upsert + $jsonSchema + índices)")
            from pipeline import etapa4_carga
            etapa4_carga.executar()
        if "analises" in etapas:
            titulo("ETAPA 6 - ANÁLISES (aggregation pipeline)")
            from pipeline import etapa5_analises
            etapa5_analises.executar()
    finally:
        try:
            from pipeline.spark import parar
            parar()
        except Exception:
            pass

    log(f"Pipeline concluído em {time.time() - inicio:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
