"""Etapa 1 - Captura.

Baixa os 9 CSV do dataset e grava sem alteração na camada raw, numa pasta com a data da carga.
Grava também um manifesto com o hash SHA-256 de cada arquivo. Se nada mudou desde a última
carga, a etapa avisa e o pipeline pode pular o reprocessamento.

Fonte: API do Kaggle (se houver credencial) ou o espelho oficial da Olist no GitHub.
"""
import hashlib
import json
import os
import shutil
import urllib.request
import zipfile
from datetime import date
from pathlib import Path

from . import config
from .log import log


def _sha256(caminho: Path) -> str:
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(1 << 20), b""):
            h.update(bloco)
    return h.hexdigest()


def _tem_credencial_kaggle() -> bool:
    return bool(os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY")) or \
        (Path.home() / ".kaggle" / "kaggle.json").exists()


def _baixar_kaggle(destino: Path) -> None:
    from kaggle.api.kaggle_api_extended import KaggleApi  # import só se for usar
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(config.KAGGLE_DATASET, path=str(destino), unzip=False, quiet=False)
    for z in destino.glob("*.zip"):
        with zipfile.ZipFile(z) as arq:
            arq.extractall(destino)
        z.unlink()


def _baixar_github(destino: Path) -> None:
    for nome in config.ARQUIVOS:
        url = config.GITHUB_ESPELHO + nome
        log(f"  baixando {nome}")
        with urllib.request.urlopen(url, timeout=120) as resp, open(destino / nome, "wb") as out:
            shutil.copyfileobj(resp, out)


def ultimo_raw() -> Path:
    """Pasta raw mais recente que tenha todos os arquivos."""
    pastas = sorted(p for p in config.DIR_RAW.glob("*") if p.is_dir())
    for p in reversed(pastas):
        if all((p / a).exists() for a in config.ARQUIVOS):
            return p
    raise FileNotFoundError("Nenhuma carga raw encontrada. Rode a etapa de captura primeiro.")


def executar(origem_local: str | None = None) -> dict:
    hoje = date.today().isoformat()
    destino = config.DIR_RAW / hoje
    destino.mkdir(parents=True, exist_ok=True)

    if origem_local:  # CSV já baixados à mão (ex.: zip do Kaggle extraído)
        log(f"Copiando CSV de {origem_local}")
        for nome in config.ARQUIVOS:
            shutil.copy2(Path(origem_local) / nome, destino / nome)
        fonte = f"local:{origem_local}"
    elif _tem_credencial_kaggle():
        log("Baixando pela API do Kaggle")
        _baixar_kaggle(destino)
        fonte = f"kaggle:{config.KAGGLE_DATASET}"
    else:
        log("Sem credencial do Kaggle: baixando do espelho oficial da Olist no GitHub")
        _baixar_github(destino)
        fonte = "github:olist/work-at-olist-data"

    faltando = [a for a in config.ARQUIVOS if not (destino / a).exists()]
    if faltando:
        raise FileNotFoundError(f"Arquivos faltando na captura: {faltando}")

    manifesto = {
        "data_carga": hoje,
        "fonte": fonte,
        "arquivos": {a: {"sha256": _sha256(destino / a), "bytes": (destino / a).stat().st_size}
                     for a in config.ARQUIVOS},
    }

    # Compara com a carga anterior para saber se algo mudou
    anteriores = sorted(p for p in config.DIR_RAW.glob("*/manifesto.json") if p.parent != destino)
    mudou = True
    if anteriores:
        antigo = json.loads(anteriores[-1].read_text(encoding="utf-8"))
        mudou = any(antigo["arquivos"].get(a, {}).get("sha256") != v["sha256"]
                    for a, v in manifesto["arquivos"].items())
    manifesto["mudou_desde_ultima_carga"] = mudou
    (destino / "manifesto.json").write_text(json.dumps(manifesto, indent=2), encoding="utf-8")

    total_mb = sum(v["bytes"] for v in manifesto["arquivos"].values()) / 1e6
    log(f"Captura ok: {len(config.ARQUIVOS)} arquivos, {total_mb:.1f} MB em {destino}")
    if not mudou:
        log("Os arquivos são iguais aos da carga anterior (mesmo hash).")
    return manifesto
