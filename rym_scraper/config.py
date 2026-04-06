import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# === Credentials ===
RYM_USERNAME = os.getenv("RYM_USERNAME", "")
RYM_PASSWORD = os.getenv("RYM_PASSWORD", "")

# === URLs ===
BASE_URL = "https://rateyourmusic.com"
LOGIN_URL = f"{BASE_URL}/account/login"

# === Plage d'années à scraper ===
YEAR_START = 1960
YEAR_END = 2026

# === Chemins ===
PROJECT_DIR = Path(__file__).parent
CACHE_DIR = PROJECT_DIR / "cache"
COVERS_DIR = PROJECT_DIR / "covers"
COOKIES_FILE = PROJECT_DIR / "cookies.json"
PROGRESS_FILE = PROJECT_DIR / "progress.json"
DB_FILE = PROJECT_DIR / "rym.db"
LOG_FILE = PROJECT_DIR / "scraper.log"

# === Délais (secondes) ===
MIN_DELAY = 5
MAX_DELAY = 12
RETRY_MIN_DELAY = 60
RETRY_MAX_DELAY = 120
MAX_RETRIES = 3

# === CAPTCHA ===
CAPTCHA_INDICATORS = [
    "are you a bot",
    "captcha",
    "verify you are human",
    "access denied",
    "please complete the security check",
]
