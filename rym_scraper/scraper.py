"""Logique de navigation et téléchargement des pages avec anti-détection."""

import random
import time
import logging

from browser import BrowserManager
from cache import is_cached, read_cache, write_cache
from config import MIN_DELAY, MAX_DELAY, RETRY_MIN_DELAY, RETRY_MAX_DELAY, MAX_RETRIES

logger = logging.getLogger(__name__)


class Scraper:
    """Télécharge les pages RYM avec délais aléatoires et gestion d'erreurs."""

    def __init__(self, browser_manager: BrowserManager):
        self.browser = browser_manager

    def fetch(self, url: str) -> str | None:
        """
        Récupère le HTML d'une URL.
        - Retourne le cache si disponible
        - Sinon télécharge avec Playwright, applique les délais, gère les retries
        - Retourne None si CAPTCHA détecté (arrêt attendu côté appelant)
        """
        # Vérifier le cache d'abord
        cached = read_cache(url)
        if cached is not None:
            logger.info("Cache hit pour %s", url)
            return cached

        # Téléchargement avec retry + backoff exponentiel
        for attempt in range(1, MAX_RETRIES + 1):
            # Délai humain avant chaque requête
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            logger.info("Attente de %.1fs avant requête (tentative %d/%d)...",
                        delay, attempt, MAX_RETRIES)
            time.sleep(delay)

            try:
                html = self.browser.fetch_page(url)

                # CAPTCHA détecté → signal d'arrêt
                if html is None:
                    logger.critical("CAPTCHA détecté — arrêt immédiat requis")
                    return None

                # Vérifier les erreurs HTTP via le contenu de la page
                html_lower = html[:3000].lower()
                if "429" in html_lower and "too many requests" in html_lower:
                    raise ConnectionError("HTTP 429 — Too Many Requests")
                if "503" in html_lower and "service unavailable" in html_lower:
                    raise ConnectionError("HTTP 503 — Service Unavailable")

                # Succès → sauvegarder en cache
                write_cache(url, html)
                logger.info("Page téléchargée et mise en cache : %s", url)
                return html

            except Exception as e:
                logger.warning("Erreur sur %s (tentative %d/%d) : %s",
                               url, attempt, MAX_RETRIES, e)

                if attempt < MAX_RETRIES:
                    # Backoff exponentiel
                    backoff = random.uniform(RETRY_MIN_DELAY, RETRY_MAX_DELAY) * attempt
                    logger.info("Retry dans %.0fs (backoff x%d)...", backoff, attempt)
                    time.sleep(backoff)
                else:
                    logger.error("Échec définitif pour %s après %d tentatives", url, MAX_RETRIES)
                    return None

        return None

    def fetch_multiple(self, urls: list[str]) -> dict[str, str | None]:
        """
        Télécharge plusieurs URLs séquentiellement.
        Retourne un dict {url: html} — la valeur est None si CAPTCHA ou échec.
        Stoppe immédiatement si CAPTCHA détecté.
        """
        results = {}
        for url in urls:
            html = self.fetch(url)
            results[url] = html
            if html is None and not is_cached(url):
                # CAPTCHA ou erreur fatale → on arrête tout
                logger.critical("Arrêt du scraping suite à un échec sur %s", url)
                break
        return results
