"""Sauvegarde des données en base SQLite — schéma normalisé."""

import hashlib
import sqlite3
import logging
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from config import DB_FILE, COVERS_DIR

logger = logging.getLogger(__name__)

SCHEMA = """
-- Artistes
CREATE TABLE IF NOT EXISTS artists (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    url         TEXT    NOT NULL UNIQUE,
    location    TEXT,
    scraped_at  TEXT    NOT NULL
);

-- Releases (albums, EPs, singles…)
CREATE TABLE IF NOT EXISTS releases (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    title           TEXT    NOT NULL,
    artist_id       INTEGER NOT NULL REFERENCES artists(id),
    year            INTEGER,
    release_type    TEXT,
    label           TEXT,
    avg_rating      REAL,
    num_ratings     INTEGER,
    cover_url       TEXT,
    url             TEXT    NOT NULL UNIQUE,
    scraped_at      TEXT    NOT NULL
);

-- Positions dans les charts (un album peut apparaître dans plusieurs charts)
CREATE TABLE IF NOT EXISTS chart_entries (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    release_id  INTEGER NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    chart_type  TEXT    NOT NULL,  -- "year" ou "alltime"
    chart_year  INTEGER,          -- ex: 2024 (NULL pour alltime)
    position    INTEGER NOT NULL,
    UNIQUE(release_id, chart_type, chart_year)
);

-- Tracks (tracklist de chaque release)
CREATE TABLE IF NOT EXISTS tracks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    release_id  INTEGER NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    track_num   INTEGER,
    title       TEXT    NOT NULL,
    duration    TEXT,
    UNIQUE(release_id, track_num)
);

-- Genres (table de référence dédupliquée)
CREATE TABLE IF NOT EXISTS genres (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT    NOT NULL UNIQUE
);

-- Descripteurs (table de référence dédupliquée)
CREATE TABLE IF NOT EXISTS descriptors (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT    NOT NULL UNIQUE
);

-- Jointure release ↔ genres (avec distinction primaire/secondaire)
CREATE TABLE IF NOT EXISTS release_genres (
    release_id INTEGER NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    genre_id   INTEGER NOT NULL REFERENCES genres(id)   ON DELETE CASCADE,
    is_primary INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (release_id, genre_id)
);

-- Jointure release ↔ descripteurs
CREATE TABLE IF NOT EXISTS release_descriptors (
    release_id    INTEGER NOT NULL REFERENCES releases(id)    ON DELETE CASCADE,
    descriptor_id INTEGER NOT NULL REFERENCES descriptors(id) ON DELETE CASCADE,
    PRIMARY KEY (release_id, descriptor_id)
);

-- Jointure artiste ↔ genres
CREATE TABLE IF NOT EXISTS artist_genres (
    artist_id INTEGER NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
    genre_id  INTEGER NOT NULL REFERENCES genres(id)  ON DELETE CASCADE,
    PRIMARY KEY (artist_id, genre_id)
);

-- Index
CREATE INDEX IF NOT EXISTS idx_releases_artist   ON releases(artist_id);
CREATE INDEX IF NOT EXISTS idx_releases_year     ON releases(year);
CREATE INDEX IF NOT EXISTS idx_releases_rating   ON releases(avg_rating DESC);
CREATE INDEX IF NOT EXISTS idx_chart_entries      ON chart_entries(chart_type, chart_year, position);
CREATE INDEX IF NOT EXISTS idx_tracks_release    ON tracks(release_id);
CREATE INDEX IF NOT EXISTS idx_rg_genre          ON release_genres(genre_id);
CREATE INDEX IF NOT EXISTS idx_rd_descriptor     ON release_descriptors(descriptor_id);
CREATE INDEX IF NOT EXISTS idx_ag_genre          ON artist_genres(genre_id);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Storage:
    """Interface SQLite normalisée."""

    def __init__(self):
        self.conn = sqlite3.connect(str(DB_FILE))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        logger.info("Base de données ouverte : %s", DB_FILE)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_or_create_genre(self, name: str) -> int:
        name = name.strip()
        row = self.conn.execute("SELECT id FROM genres WHERE name = ?", (name,)).fetchone()
        if row:
            return row[0]
        cur = self.conn.execute("INSERT INTO genres (name) VALUES (?)", (name,))
        return cur.lastrowid

    def _get_or_create_descriptor(self, name: str) -> int:
        name = name.strip()
        row = self.conn.execute("SELECT id FROM descriptors WHERE name = ?", (name,)).fetchone()
        if row:
            return row[0]
        cur = self.conn.execute("INSERT INTO descriptors (name) VALUES (?)", (name,))
        return cur.lastrowid

    # ------------------------------------------------------------------
    # Artistes
    # ------------------------------------------------------------------

    def upsert_artist(self, data: dict) -> int:
        try:
            now = _now()
            self.conn.execute("""
                INSERT INTO artists (name, url, location, scraped_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    name = excluded.name, location = excluded.location, scraped_at = excluded.scraped_at
            """, (data["name"], data["url"], data.get("country", ""), now))

            artist_id = self.conn.execute("SELECT id FROM artists WHERE url = ?", (data["url"],)).fetchone()[0]

            genres_str = data.get("genres", "")
            if genres_str:
                self.conn.execute("DELETE FROM artist_genres WHERE artist_id = ?", (artist_id,))
                for g in genres_str.split(","):
                    g = g.strip()
                    if g:
                        gid = self._get_or_create_genre(g)
                        self.conn.execute("INSERT OR IGNORE INTO artist_genres (artist_id, genre_id) VALUES (?, ?)", (artist_id, gid))

            self.conn.commit()
            return artist_id
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error("Erreur SQLite (artiste) : %s", e)
            return -1

    # ------------------------------------------------------------------
    # Releases
    # ------------------------------------------------------------------

    def upsert_release(self, data: dict) -> int:
        try:
            now = _now()
            artist_url = data.get("artist_url", "")
            artist_name = data.get("artist_name", "Unknown")
            artist_id = self._ensure_artist(artist_name, artist_url)

            self.conn.execute("""
                INSERT INTO releases (title, artist_id, year, release_type, label,
                    avg_rating, num_ratings, cover_url, url, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    title = excluded.title, artist_id = excluded.artist_id,
                    year = excluded.year, release_type = excluded.release_type,
                    label = excluded.label, avg_rating = excluded.avg_rating,
                    num_ratings = excluded.num_ratings, cover_url = excluded.cover_url,
                    scraped_at = excluded.scraped_at
            """, (
                data["title"], artist_id, data.get("year"),
                data.get("release_type"), data.get("label"),
                data.get("avg_rating"), data.get("num_ratings"),
                data.get("cover_url"), data["url"], now,
            ))

            release_id = self.conn.execute("SELECT id FROM releases WHERE url = ?", (data["url"],)).fetchone()[0]

            # Genres primaires
            self.conn.execute("DELETE FROM release_genres WHERE release_id = ?", (release_id,))
            for g in (data.get("pri_genres") or "").split(","):
                g = g.strip()
                if g:
                    gid = self._get_or_create_genre(g)
                    self.conn.execute("INSERT OR IGNORE INTO release_genres (release_id, genre_id, is_primary) VALUES (?, ?, 1)", (release_id, gid))
            # Genres secondaires
            for g in (data.get("sec_genres") or "").split(","):
                g = g.strip()
                if g:
                    gid = self._get_or_create_genre(g)
                    self.conn.execute("INSERT OR IGNORE INTO release_genres (release_id, genre_id, is_primary) VALUES (?, ?, 0)", (release_id, gid))

            # Descripteurs
            self.conn.execute("DELETE FROM release_descriptors WHERE release_id = ?", (release_id,))
            for d in (data.get("descriptors") or "").split(","):
                d = d.strip()
                if d:
                    did = self._get_or_create_descriptor(d)
                    self.conn.execute("INSERT OR IGNORE INTO release_descriptors (release_id, descriptor_id) VALUES (?, ?)", (release_id, did))

            # Tracks
            self.conn.execute("DELETE FROM tracks WHERE release_id = ?", (release_id,))
            for track in data.get("tracks", []):
                self.conn.execute("""
                    INSERT INTO tracks (release_id, track_num, title, duration)
                    VALUES (?, ?, ?, ?)
                """, (release_id, track.get("num"), track["title"], track.get("duration")))

            # Chart entry
            if data.get("chart_position"):
                self.add_chart_entry(
                    release_id,
                    data.get("chart_type", "year"),
                    data.get("chart_year"),
                    data["chart_position"],
                )

            # Télécharger la pochette
            cover_url = data.get("cover_url", "")
            if cover_url:
                self._download_cover(cover_url, data["url"])

            self.conn.commit()
            logger.debug("Release sauvegardé : %s (id=%d)", data.get("title"), release_id)
            return release_id
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error("Erreur SQLite (release) : %s — data=%s", e, data)
            return -1

    def _ensure_artist(self, name: str, url: str) -> int:
        if url:
            row = self.conn.execute("SELECT id FROM artists WHERE url = ?", (url,)).fetchone()
            if row:
                return row[0]
        now = _now()
        effective_url = url or f"__unknown_{name}"
        self.conn.execute(
            "INSERT OR IGNORE INTO artists (name, url, scraped_at) VALUES (?, ?, ?)",
            (name, effective_url, now),
        )
        return self.conn.execute("SELECT id FROM artists WHERE url = ?", (effective_url,)).fetchone()[0]

    # ------------------------------------------------------------------
    # Covers
    # ------------------------------------------------------------------

    @staticmethod
    def _download_cover(cover_url: str, release_url: str):
        """Télécharge la pochette dans covers/ si pas déjà présente."""
        COVERS_DIR.mkdir(parents=True, exist_ok=True)
        # Nom de fichier basé sur le hash de l'URL du release
        ext = ".jpg"
        filename = hashlib.sha256(release_url.encode()).hexdigest()[:16] + ext
        filepath = COVERS_DIR / filename
        if filepath.exists():
            return
        try:
            if cover_url.startswith("//"):
                cover_url = "https:" + cover_url
            urllib.request.urlretrieve(cover_url, str(filepath))
            logger.debug("Cover téléchargée : %s", filename)
        except Exception as e:
            logger.warning("Échec téléchargement cover %s : %s", cover_url, e)

    # ------------------------------------------------------------------
    # Chart entries
    # ------------------------------------------------------------------

    def add_chart_entry(self, release_id: int, chart_type: str, chart_year: int | None, position: int):
        """Ajoute ou met à jour une entrée de classement."""
        self.conn.execute("""
            INSERT INTO chart_entries (release_id, chart_type, chart_year, position)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(release_id, chart_type, chart_year) DO UPDATE SET
                position = excluded.position
        """, (release_id, chart_type, chart_year, position))

    def add_chart_entry_by_url(self, url: str, chart_type: str, chart_year: int | None, position: int):
        """Ajoute un chart entry pour un album déjà en base (identifié par URL)."""
        row = self.conn.execute("SELECT id FROM releases WHERE url = ?", (url,)).fetchone()
        if row:
            self.add_chart_entry(row[0], chart_type, chart_year, position)
            self.conn.commit()

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self) -> dict:
        counts = {}
        for table in ("artists", "releases", "genres", "descriptors", "tracks", "chart_entries"):
            counts[table] = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return counts

    def close(self):
        self.conn.close()
        logger.info("Base de données fermée")
