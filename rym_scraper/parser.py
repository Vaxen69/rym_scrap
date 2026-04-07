"""Extraction des données depuis le HTML avec BeautifulSoup."""

import logging
import re
from bs4 import BeautifulSoup, NavigableString

logger = logging.getLogger(__name__)


def parse_release(html: str, url: str) -> dict | None:
    """Parse la page d'un album/release RYM."""
    try:
        soup = BeautifulSoup(html, "html.parser")

        # Titre — premier noeud texte direct de div.album_title
        title = None
        title_el = soup.select_one("div.album_title")
        if title_el:
            for node in title_el.children:
                if isinstance(node, NavigableString) and node.strip():
                    title = node.strip()
                    break
            if not title:
                title = title_el.get_text(strip=True)

        # Artiste
        artist_el = soup.select_one(".album_title a.artist")
        artist_name = artist_el.get_text(strip=True) if artist_el else None
        artist_url = artist_el.get("href", "") if artist_el else ""

        # Année — span.issue_year (premier match)
        year = None
        date_el = soup.select_one("span.issue_year")
        if date_el:
            m = re.search(r"(\d{4})", date_el.get_text())
            if m:
                year = int(m.group(1))

        # Type de release (Album, EP, Single…) — extrait du titre de la page
        release_type = None
        page_title = soup.title.get_text() if soup.title else ""
        type_match = re.search(r"\((?:Album|EP|Single|Compilation|Mixtape|Live Album|Bootleg|DJ Mix|Demo)[^)]*\)", page_title)
        if type_match:
            # Ex: "(Album, Alternative Rock)" → "Album"
            release_type = type_match.group(0).strip("()").split(",")[0].strip()

        # Label
        label = ""
        label_el = soup.select_one("span.issue_label a.label")
        if not label_el:
            label_el = soup.select_one("a.label")
        if label_el:
            label = label_el.get_text(strip=True)

        # Genres primaires
        pri_genres_els = soup.select("span.release_pri_genres a.genre")
        pri_genres = ", ".join(g.get_text(strip=True) for g in pri_genres_els)

        # Genres secondaires
        sec_genres_els = soup.select("span.release_sec_genres a.genre")
        sec_genres = ", ".join(g.get_text(strip=True) for g in sec_genres_els)

        # Note moyenne
        avg_rating = None
        rating_el = soup.select_one("span.avg_rating")
        if rating_el:
            try:
                avg_rating = float(rating_el.get_text(strip=True))
            except ValueError:
                pass

        # Nombre de votes — texte localisé, on extrait juste les chiffres
        num_ratings = None
        num_el = soup.select_one("span.num_ratings")
        if num_el:
            digits = re.sub(r"[^\d]", "", num_el.get_text())
            if digits:
                num_ratings = int(digits)

        # Descripteurs — texte brut dans span.release_pri_descriptors
        descriptors = ""
        desc_el = soup.select_one("span.release_pri_descriptors")
        if desc_el:
            raw = desc_el.get_text(strip=True)
            descriptors = ", ".join(d.strip() for d in raw.split(",") if d.strip())

        # Tracklist
        tracks = []
        tracklist_el = soup.select_one("ul.tracklisting")
        if tracklist_el:
            for li in tracklist_el.select("li.track"):
                track_num_el = li.select_one("span.tracklist_num")
                track_title_el = li.select_one("a.song")
                track_dur_el = li.select_one("span.tracklist_duration")

                if track_title_el:
                    track_num = None
                    if track_num_el:
                        num_text = re.sub(r"[^\d]", "", track_num_el.get_text())
                        if num_text:
                            track_num = int(num_text)

                    tracks.append({
                        "num": track_num,
                        "title": track_title_el.get_text(strip=True),
                        "duration": track_dur_el.get_text(strip=True) if track_dur_el else None,
                    })

        # Pochette (cover art URL)
        cover_url = ""
        cover_el = soup.select_one('[class*="release_art"] img')
        if cover_el:
            cover_url = cover_el.get("src", "")

        result = {
            "title": title,
            "artist_name": artist_name,
            "artist_url": artist_url,
            "year": year,
            "release_type": release_type,
            "label": label,
            "pri_genres": pri_genres,
            "sec_genres": sec_genres,
            "avg_rating": avg_rating,
            "num_ratings": num_ratings,
            "descriptors": descriptors,
            "tracks": tracks,
            "cover_url": cover_url,
            "url": url,
        }

        if not title:
            logger.warning("Titre introuvable pour %s", url)

        return result

    except Exception as e:
        logger.error("Erreur de parsing pour %s : %s", url, e)
        return None


def parse_artist(html: str, url: str) -> dict | None:
    """Parse la page d'un artiste RYM."""
    try:
        soup = BeautifulSoup(html, "html.parser")

        name_el = soup.select_one("h1.artist_name_hdr")
        name = name_el.get_text(strip=True) if name_el else None

        country = ""
        country_el = soup.select_one("div.artist_info a.location")
        if country_el:
            country = country_el.get_text(strip=True)

        genre_els = soup.select("div.artist_info a.genre")
        genres = ", ".join(g.get_text(strip=True) for g in genre_els)

        result = {"name": name, "url": url, "country": country, "genres": genres}

        if not name:
            logger.warning("Nom introuvable pour %s", url)

        return result

    except Exception as e:
        logger.error("Erreur de parsing artiste pour %s : %s", url, e)
        return None


def _parse_abbr_number(text: str) -> int | None:
    """Convertit '11k', '1.5k', '2m', '450' en int."""
    if not text:
        return None
    text = text.strip().lower().replace(",", ".")
    multiplier = 1
    if text.endswith("k"):
        multiplier = 1_000
        text = text[:-1]
    elif text.endswith("m"):
        multiplier = 1_000_000
        text = text[:-1]
    try:
        return int(float(text) * multiplier)
    except ValueError:
        return None


def extract_chart_items(html: str) -> list[dict]:
    """
    Extrait TOUTES les données de chaque item depuis la page de chart.
    Pas besoin de visiter chaque page album individuellement.
    Retourne une liste de dicts avec toutes les infos.
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []
    chart_items = soup.select("div.page_charts_section_charts_item")

    for i, div in enumerate(chart_items, 1):
        # URL release
        rel_link = div.select_one("a.page_charts_section_charts_item_link.release")
        if not rel_link:
            rel_link = div.select_one("a.release")
        if not rel_link:
            continue
        href = rel_link.get("href", "")
        if not href or not href.startswith("/release/"):
            continue

        # Titre
        title_el = div.select_one("a.page_charts_section_charts_item_link.release .ui_name_locale_original")
        if not title_el:
            title_el = rel_link
        title = title_el.get_text(strip=True) if title_el else None

        # Artiste (nom + URL)
        artist_link = div.select_one(".page_charts_section_charts_item_credited_links_primary a")
        artist_name = None
        artist_url = ""
        if artist_link:
            artist_url = artist_link.get("href", "")
            name_el = artist_link.select_one(".ui_name_locale_original") or artist_link
            artist_name = name_el.get_text(strip=True)

        # Date → année
        year = None
        date_el = div.select_one(".page_charts_section_charts_item_date")
        if date_el:
            m = re.search(r"(\d{4})", date_el.get_text())
            if m:
                year = int(m.group(1))

        # Type de release
        release_type = None
        type_el = div.select_one(".page_charts_section_charts_item_release_type")
        if type_el:
            release_type = type_el.get_text(strip=True)

        # Genres primaires
        pri_genres = ", ".join(
            g.get_text(strip=True)
            for g in div.select(".page_charts_section_charts_item_genres_primary .genre")
        )

        # Genres secondaires
        sec_genres = ", ".join(
            g.get_text(strip=True)
            for g in div.select(".page_charts_section_charts_item_genres_secondary .genre")
        )

        # Note
        avg_rating = None
        rating_el = div.select_one(".page_charts_section_charts_item_details_average_num")
        if rating_el:
            try:
                avg_rating = float(rating_el.get_text(strip=True))
            except ValueError:
                pass

        # Nombre de votes (format abrégé "11k")
        num_ratings = None
        num_el = div.select_one(".page_charts_section_charts_item_details_ratings .abbr")
        if num_el:
            num_ratings = _parse_abbr_number(num_el.get_text(strip=True))

        # Nombre de reviews
        num_reviews = None
        rev_el = div.select_one(".page_charts_section_charts_item_details_reviews .abbr")
        if rev_el:
            num_reviews = _parse_abbr_number(rev_el.get_text(strip=True))

        # Cover image — RYM utilise lazy-loading : src contient un placeholder
        # base64, la vraie URL est dans data-src
        cover_url = ""
        img = div.select_one(".page_charts_section_charts_item_image img")
        if img:
            data_src = img.get("data-src", "")
            src = img.get("src", "")
            if data_src:
                cover_url = data_src
            elif src and not src.startswith("data:"):
                cover_url = src

        items.append({
            "href": href,
            "position": i,
            "title": title,
            "artist_name": artist_name,
            "artist_url": artist_url,
            "year": year,
            "release_type": release_type,
            "pri_genres": pri_genres,
            "sec_genres": sec_genres,
            "avg_rating": avg_rating,
            "num_ratings": num_ratings,
            "num_reviews": num_reviews,
            "cover_url": cover_url,
        })

    return items


def extract_chart_pages(html: str, base_chart_url: str = "") -> list[str]:
    """
    Extrait TOUTES les pages de pagination d'un chart.
    RYM n'affiche que [2, ..., N, Next], donc on détermine N puis on génère 2..N.
    base_chart_url ex: '/charts/top/album/2026/' → génère '/charts/top/album/2026/2/' etc.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Cherche le numéro de page le plus élevé dans tous les liens de pagination
    max_page = 1
    page_pattern = re.compile(r"/(\d+)/?$")
    for a in soup.select("a.ui_pagination_btn, a[class*='pagination']"):
        href = a.get("href", "")
        m = page_pattern.search(href)
        if m:
            try:
                n = int(m.group(1))
                if n > max_page:
                    max_page = n
            except ValueError:
                pass

    if max_page > 1:
        base = base_chart_url.rstrip("/")
        return [f"{base}/{i}/" for i in range(2, max_page + 1)]

    return []


def extract_next_page(html: str) -> str | None:
    """
    Fallback : extrait le href du bouton 'Next' s'il existe.
    Utilisé si extract_chart_pages() ne trouve pas de pagination explicite.
    """
    soup = BeautifulSoup(html, "html.parser")
    next_label = soup.select_one("span.ui_pagination_next_label")
    if next_label:
        link = next_label.find_parent("a")
        if link:
            href = link.get("href", "")
            if href:
                return href
    return None
