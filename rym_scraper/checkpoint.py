"""Système de checkpoint — permet de reprendre le scraping après interruption."""

import json
import logging
from config import PROGRESS_FILE

logger = logging.getLogger(__name__)


def load_progress() -> set[str]:
    """Charge l'ensemble des URLs déjà traitées."""
    if not PROGRESS_FILE.exists():
        return set()
    data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    urls = set(data.get("processed_urls", []))
    logger.info("Checkpoint chargé : %d URLs déjà traitées", len(urls))
    return urls


def save_progress(processed_urls: set[str]):
    """Sauvegarde l'état courant dans progress.json."""
    data = {"processed_urls": sorted(processed_urls)}
    PROGRESS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def mark_done(url: str, processed_urls: set[str]):
    """Marque une URL comme traitée et persiste immédiatement."""
    processed_urls.add(url)
    save_progress(processed_urls)
    logger.debug("Checkpoint mis à jour : +%s (%d total)", url, len(processed_urls))
