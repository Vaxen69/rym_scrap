"""Gestion du cache HTML local — évite de re-télécharger les pages déjà visitées."""

import hashlib
import logging
from pathlib import Path

from config import CACHE_DIR

logger = logging.getLogger(__name__)

# Créer le dossier cache s'il n'existe pas
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _url_to_filename(url: str) -> str:
    """Génère un nom de fichier unique à partir du hash SHA-256 de l'URL."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest() + ".html"


def is_cached(url: str) -> bool:
    """Vérifie si le HTML de cette URL est déjà en cache."""
    return (CACHE_DIR / _url_to_filename(url)).exists()


def read_cache(url: str) -> str | None:
    """Lit le HTML depuis le cache. Retourne None si absent."""
    path = CACHE_DIR / _url_to_filename(url)
    if not path.exists():
        return None
    logger.debug("Cache hit : %s", url)
    return path.read_text(encoding="utf-8")


def write_cache(url: str, html: str):
    """Sauvegarde le HTML dans le cache."""
    path = CACHE_DIR / _url_to_filename(url)
    path.write_text(html, encoding="utf-8")
    logger.debug("Cache write : %s → %s", url, path.name)
