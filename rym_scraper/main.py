"""Point d'entrée — orchestre le scraping RYM (charts par année + all-time)."""

import logging
import sys
from config import BASE_URL, YEAR_START, YEAR_END, LOG_FILE
from browser import BrowserManager
from scraper import Scraper
from parser import extract_chart_items, extract_chart_pages, extract_next_page
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


def _save_page_items(items: list, position_offset: int, chart_type: str,
                     chart_year: int | None, storage: Storage, processed: set):
    """Insère les items d'une page en DB immédiatement."""
    for item in items:
        item["position"] += position_offset
        link = item["href"]
        url = BASE_URL + link if link.startswith("/") else link
        item["url"] = url
        item["chart_position"] = item["position"]
        item["chart_type"] = chart_type
        item["chart_year"] = chart_year

        if url in processed:
            storage.add_chart_entry_by_url(url, chart_type, chart_year, item["position"])
            continue

        if item.get("title"):
            storage.upsert_release(item)
            mark_done(url, processed)
        else:
            logger.warning("Item incomplet, skip : %s", url)


def scrape_chart(chart_url: str, chart_type: str, chart_year: int | None,
                 scraper: Scraper, storage: Storage, processed: set) -> bool:
    """
    Scrape un chart complet (toutes les pages de pagination).
    Insertion en DB au fur et à mesure de chaque page (résilient au crash).
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

    page1_items = extract_chart_items(chart_html)
    total_count = len(page1_items)
    _save_page_items(page1_items, 0, chart_type, chart_year, storage, processed)
    mark_done(chart_url, processed)
    logger.info("Chart %s : page 1 enregistrée (%d items)", label, len(page1_items))

    # Pagination
    chart_path = chart_url.replace(BASE_URL, "")
    next_pages = extract_chart_pages(chart_html, chart_path)

    if next_pages:
        logger.info("Chart %s : %d pages détectées", label, len(next_pages) + 1)
        for idx, page_href in enumerate(next_pages, 2):
            page_url = BASE_URL + page_href if page_href.startswith("/") else page_href
            if page_url in processed:
                # On compte quand même les items pour l'offset
                continue

            page_html = scraper.fetch(page_url)
            if page_html is None:
                logger.critical("CAPTCHA sur pagination %s — arrêt", page_url)
                return False

            new_items = extract_chart_items(page_html)
            _save_page_items(new_items, total_count, chart_type, chart_year, storage, processed)
            total_count += len(new_items)
            mark_done(page_url, processed)
            logger.info("Chart %s : page %d/%d enregistrée (%d items, total %d)",
                        label, idx, len(next_pages) + 1, len(new_items), total_count)
    else:
        # Fallback : suit le bouton Next
        logger.info("Chart %s : pagination via Next", label)
        current_html = chart_html
        page_count = 1
        while True:
            next_href = extract_next_page(current_html)
            if not next_href:
                break
            page_url = BASE_URL + next_href if next_href.startswith("/") else next_href
            if page_url in processed:
                break

            current_html = scraper.fetch(page_url)
            if current_html is None:
                logger.critical("CAPTCHA sur pagination %s — arrêt", page_url)
                return False

            new_items = extract_chart_items(current_html)
            _save_page_items(new_items, total_count, chart_type, chart_year, storage, processed)
            total_count += len(new_items)
            mark_done(page_url, processed)
            page_count += 1
            logger.info("Chart %s : page %d enregistrée via Next (total %d)",
                        label, page_count, total_count)

    logger.info("Chart %s terminé : %d items au total", label, total_count)
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

        # Augmenter à 100 items par page pour réduire le nombre de requêtes
        browser_mgr.set_items_per_page(100)

        scraper = Scraper(browser_mgr)

        # Phase 1 : chart all-time (priorité — chart le plus important)
        logger.info("=== Phase 1 : Chart all-time ===")
        alltime_url = f"{BASE_URL}/charts/top/album/all-time/"
        ok = scrape_chart(alltime_url, "alltime", None, scraper, storage, processed)
        if not ok:
            logger.warning("All-time interrompu — relancer plus tard")

        # Phase 2 : charts par année (du plus récent au plus ancien)
        logger.info("=== Phase 2 : Charts par année ===")
        for year in range(YEAR_END, YEAR_START - 1, -1):
            chart_url = f"{BASE_URL}/charts/top/album/{year}/"
            ok = scrape_chart(chart_url, "year", year, scraper, storage, processed)
            if not ok:
                logger.warning("Scraping interrompu — relancer plus tard")
                break

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
