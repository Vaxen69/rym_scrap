"""Point d'entrée — orchestre le scraping RYM (charts par année + all-time)."""

import logging
import sys
from config import BASE_URL, YEAR_START, YEAR_END, LOG_FILE
from browser import BrowserManager
from scraper import Scraper
from parser import parse_release, extract_chart_items, extract_chart_pages
from storage import Storage
from checkpoint import load_progress, mark_done


def setup_logging():
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(str(LOG_FILE), encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(file_handler)
    root.addHandler(console_handler)


logger = logging.getLogger(__name__)


def scrape_chart(chart_url: str, chart_type: str, chart_year: int | None,
                 scraper: Scraper, storage: Storage, processed: set) -> bool:
    """
    Scrape un chart complet (toutes les pages de pagination + chaque release).
    chart_type: "year" ou "alltime"
    Retourne False si CAPTCHA détecté.
    """
    label = f"{chart_type} {chart_year}" if chart_year else chart_type
    logger.info("━━━ Chart %s ━━━ %s", label, chart_url)

    # Page 1
    chart_html = scraper.fetch(chart_url)
    if chart_html is None:
        logger.critical("CAPTCHA ou erreur sur le chart %s — arrêt", label)
        return False

    chart_items = extract_chart_items(chart_html)
    mark_done(chart_url, processed)

    # Pagination
    next_pages = extract_chart_pages(chart_html)
    for page_href in next_pages:
        page_url = BASE_URL + page_href if page_href.startswith("/") else page_href
        if page_url in processed:
            continue

        page_html = scraper.fetch(page_url)
        if page_html is None:
            logger.critical("CAPTCHA sur pagination %s — arrêt", page_url)
            return False

        page_offset = len(chart_items)
        new_items = extract_chart_items(page_html)
        for item in new_items:
            item["position"] += page_offset
        chart_items.extend(new_items)
        mark_done(page_url, processed)

    logger.info("Chart %s : %d releases à traiter", label, len(chart_items))

    # Scraper chaque release
    for i, item in enumerate(chart_items, 1):
        link = item["href"]
        position = item["position"]
        url = BASE_URL + link if link.startswith("/") else link

        if url in processed:
            # L'album est déjà scrapé mais on doit quand même ajouter l'entrée chart
            storage.add_chart_entry_by_url(url, chart_type, chart_year, position)
            continue

        logger.info("[%s] [%d/%d] #%d %s", label, i, len(chart_items), position, url)
        html = scraper.fetch(url)

        if html is None:
            logger.critical("CAPTCHA ou erreur fatale — arrêt")
            return False

        data = parse_release(html, url)
        if data and data.get("title"):
            data["chart_position"] = position
            data["chart_type"] = chart_type
            data["chart_year"] = chart_year
            storage.upsert_release(data)
        else:
            logger.warning("Parsing incomplet pour %s, skip", url)

        mark_done(url, processed)

    logger.info("Chart %s terminé", label)
    return True


def main():
    setup_logging()
    logger.info("=== Démarrage du scraper RYM ===")
    logger.info("Plage : %d → %d + all-time", YEAR_START, YEAR_END)

    browser_mgr = BrowserManager()
    storage = Storage()
    processed = load_progress()

    try:
        browser_mgr.start()
        if not browser_mgr.ensure_logged_in():
            logger.critical("Impossible de se connecter — arrêt")
            return

        scraper = Scraper(browser_mgr)

        # Phase 1 : charts par année (du plus récent au plus ancien)
        logger.info("=== Phase 1 : Charts par année ===")
        for year in range(YEAR_END, YEAR_START - 1, -1):
            chart_url = f"{BASE_URL}/charts/top/album/{year}/"
            ok = scrape_chart(chart_url, "year", year, scraper, storage, processed)
            if not ok:
                logger.warning("Scraping interrompu — relancer plus tard")
                break

        # Phase 2 : chart all-time
        logger.info("=== Phase 2 : Chart all-time ===")
        alltime_url = f"{BASE_URL}/charts/top/album/all-time/"
        scrape_chart(alltime_url, "alltime", None, scraper, storage, processed)

        # Résumé
        stats = storage.get_stats()
        logger.info("=== Résumé final ===")
        logger.info("Releases      : %d", stats["releases"])
        logger.info("Artistes      : %d", stats["artists"])
        logger.info("Genres        : %d", stats["genres"])
        logger.info("Descripteurs  : %d", stats["descriptors"])
        logger.info("Tracks        : %d", stats["tracks"])
        logger.info("Chart entries : %d", stats["chart_entries"])

    except KeyboardInterrupt:
        logger.info("Interruption manuelle (Ctrl+C)")

    except Exception as e:
        logger.exception("Erreur inattendue : %s", e)

    finally:
        storage.close()
        browser_mgr.stop()
        logger.info("Nettoyage terminé")


if __name__ == "__main__":
    main()
