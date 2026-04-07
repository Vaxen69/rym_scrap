"""Gestion du navigateur avec undetected-chromedriver pour bypass Cloudflare."""

import json
import time
import logging
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config import (
    RYM_USERNAME, RYM_PASSWORD, LOGIN_URL, BASE_URL,
    COOKIES_FILE, CAPTCHA_INDICATORS, PROJECT_DIR,
)

logger = logging.getLogger(__name__)

BROWSER_PROFILE_DIR = PROJECT_DIR / "browser_profile"


class BrowserManager:
    """Navigateur Chrome indétectable pour scraper RYM."""

    def __init__(self):
        self._driver: uc.Chrome | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        """Lance Chrome avec undetected-chromedriver."""
        options = uc.ChromeOptions()
        options.add_argument(f"--user-data-dir={BROWSER_PROFILE_DIR}")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--lang=en-US")

        self._driver = uc.Chrome(options=options, version_main=146)
        logger.info("Chrome lancé (undetected) avec profil : %s", BROWSER_PROFILE_DIR)

    def stop(self):
        """Ferme le navigateur."""
        if self._driver:
            try:
                self._save_cookies()
                self._driver.quit()
            except Exception:
                pass
        logger.info("Navigateur fermé")

    # ------------------------------------------------------------------
    # Authentification
    # ------------------------------------------------------------------

    def login(self) -> bool:
        """Se connecte à RYM."""
        if not RYM_USERNAME or not RYM_PASSWORD:
            logger.error("Credentials manquants dans .env")
            return False

        driver = self._driver
        logger.info("Navigation vers la page de login...")
        driver.get(LOGIN_URL)
        self._wait_for_cloudflare()

        if self._is_captcha():
            logger.critical("CAPTCHA RYM détecté sur la page de login — arrêt")
            return False

        # Attendre le formulaire
        try:
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.ID, "username"))
            )
        except Exception:
            logger.error("Formulaire de login introuvable après 60s")
            return False

        # Remplir et soumettre
        driver.find_element(By.ID, "username").clear()
        driver.find_element(By.ID, "username").send_keys(RYM_USERNAME)
        driver.find_element(By.ID, "password").clear()
        driver.find_element(By.ID, "password").send_keys(RYM_PASSWORD)

        # Cocher "Remember me"
        try:
            remember = driver.find_element(By.ID, "remember")
            if not remember.is_selected():
                remember.click()
        except Exception:
            pass

        driver.find_element(By.ID, "login_submit").click()

        # Attendre la redirection
        time.sleep(3)
        self._wait_for_cloudflare()

        if self._is_captcha():
            logger.critical("CAPTCHA RYM détecté après login — arrêt")
            return False

        if "/account/login" in driver.current_url:
            logger.error("Échec du login — toujours sur la page de connexion")
            return False

        self._save_cookies()
        logger.info("Login réussi pour %s", RYM_USERNAME)
        return True

    def is_logged_in(self) -> bool:
        """Vérifie si la session est active."""
        driver = self._driver
        driver.get(BASE_URL)
        self._wait_for_cloudflare()
        self._dismiss_popups()
        try:
            driver.find_element(By.CSS_SELECTOR, 'a[href*="/~"]')
            return True
        except Exception:
            return False

    def ensure_logged_in(self) -> bool:
        """S'assure qu'on est connecté, sinon login."""
        if self.is_logged_in():
            logger.info("Session active")
            return True
        logger.info("Session expirée, tentative de login...")
        return self.login()

    def set_items_per_page(self, value: int = 100) -> bool:
        """
        Configure la préférence 'items per page' à 100 (max).
        Clique d'abord sur l'icône engrenage pour ouvrir le panneau settings,
        puis sélectionne la valeur dans le dropdown.
        Persiste server-side pour toute la session.
        """
        driver = self._driver
        chart_url = f"{BASE_URL}/charts/top/album/all-time/"
        logger.info("Configuration items_per_page=%d...", value)
        try:
            driver.get(chart_url)
            self._wait_for_cloudflare()
            self._dismiss_popups()

            # Cliquer sur l'icône engrenage pour ouvrir le panneau settings
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "i.fa-cog"))
                )
                cog = driver.find_element(By.CSS_SELECTOR, "i.fa-cog")
                # Cliquer le parent (le bouton englobant) si possible
                try:
                    parent = cog.find_element(By.XPATH, "./..")
                    parent.click()
                except Exception:
                    cog.click()
                time.sleep(1)
            except Exception as e:
                logger.warning("Icône engrenage introuvable : %s", e)
                return False

            # Attendre que le select apparaisse après clic
            from selenium.webdriver.support.ui import Select
            try:
                WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.ID, "page_charts_settings_items_per_page"))
                )
            except Exception:
                logger.warning("Select items_per_page introuvable après clic engrenage")
                return False

            sel_el = driver.find_element(By.ID, "page_charts_settings_items_per_page")
            Select(sel_el).select_by_value(str(value))
            # Déclencher le change event au cas où
            driver.execute_script(
                "arguments[0].dispatchEvent(new Event('change', {bubbles: true}));",
                sel_el,
            )
            time.sleep(3)  # Laisse RYM enregistrer la préférence
            logger.info("items_per_page configuré à %d", value)
            return True
        except Exception as e:
            logger.warning("Échec configuration items_per_page : %s", e)
            return False

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def fetch_page(self, url: str) -> str | None:
        """Navigue vers l'URL et retourne le HTML. None si CAPTCHA non résolu."""
        driver = self._driver
        driver.get(url)
        self._wait_for_cloudflare()
        self._dismiss_popups()

        if self._is_captcha():
            # Attendre que l'utilisateur résolve le CAPTCHA manuellement
            if not self._wait_for_captcha_resolution():
                logger.critical("CAPTCHA non résolu sur %s — arrêt", url)
                return None
            # Après résolution, recharger la page originale
            driver.get(url)
            self._wait_for_cloudflare()
            self._dismiss_popups()

        return driver.page_source

    # ------------------------------------------------------------------
    # Popups (cookies consent + pubs)
    # ------------------------------------------------------------------

    def _dismiss_popups(self):
        """Ferme la popup de consentement cookies et les pubs overlay."""
        driver = self._driver
        # Bouton "Do not consent" ou "Consent" (on refuse par défaut)
        for selector in [
            'button.fc-cta-do-not-consent',       # "Do not consent"
            'button[aria-label="Do not consent"]',
            '.fc-dialog-container button.fc-cta-consent',  # fallback "Consent"
            'button.fc-cta-consent',
        ]:
            try:
                btn = driver.find_element(By.CSS_SELECTOR, selector)
                if btn.is_displayed():
                    btn.click()
                    logger.debug("Popup cookies fermée via %s", selector)
                    time.sleep(1)
                    return
            except Exception:
                continue

        # Fermer les pubs overlay / modals
        for selector in [
            'button.modal-close', 'button.close', '.ad-close',
            '[class*="dismiss"]', '[class*="close-btn"]',
        ]:
            try:
                btn = driver.find_element(By.CSS_SELECTOR, selector)
                if btn.is_displayed():
                    btn.click()
                    logger.debug("Popup pub fermée via %s", selector)
            except Exception:
                continue

    # ------------------------------------------------------------------
    # Cookies
    # ------------------------------------------------------------------

    def _save_cookies(self):
        cookies = self._driver.get_cookies()
        COOKIES_FILE.write_text(json.dumps(cookies, indent=2), encoding="utf-8")
        logger.debug("Cookies sauvegardés (%d)", len(cookies))

    def _load_cookies(self):
        if not COOKIES_FILE.exists():
            return
        cookies = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
        for cookie in cookies:
            # Selenium n'accepte pas certains champs
            cookie.pop("sameSite", None)
            cookie.pop("httpOnly", None)
            cookie.pop("expiry", None)
            try:
                self._driver.add_cookie(cookie)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Cloudflare
    # ------------------------------------------------------------------

    def _wait_for_cloudflare(self, timeout: int = 120):
        """Attend que Cloudflare soit passé (max timeout secondes)."""
        waited = 0
        logged = False

        while waited < timeout:
            try:
                title = (self._driver.title or "").lower()
            except Exception:
                time.sleep(2)
                waited += 2
                continue

            is_cf = (
                "just a moment" in title
                or "security verification" in title
                or "verify you are human" in title
                or "attention required" in title
            )

            if not is_cf:
                return

            if not logged:
                logger.warning(
                    "Cloudflare détecté — résous le CAPTCHA dans la fenêtre si nécessaire"
                )
                logged = True

            time.sleep(2)
            waited += 2

        logger.critical("Timeout Cloudflare après %ds", timeout)

    # ------------------------------------------------------------------
    # CAPTCHA RYM (attente résolution manuelle)
    # ------------------------------------------------------------------

    def _wait_for_captcha_resolution(self, timeout: int = 300) -> bool:
        """
        Attend que l'utilisateur résolve le CAPTCHA RYM manuellement.
        Vérifie toutes les 3 secondes pendant max 5 minutes.
        Retourne True si résolu, False si timeout.
        """
        logger.warning("CAPTCHA RYM détecté — résous-le dans la fenêtre du navigateur (5 min max)")
        waited = 0
        while waited < timeout:
            time.sleep(3)
            waited += 3
            if not self._is_captcha():
                logger.info("CAPTCHA résolu, reprise du scraping")
                return True
        return False

    # ------------------------------------------------------------------
    # Détection CAPTCHA RYM
    # ------------------------------------------------------------------

    def _is_captcha(self) -> bool:
        """Vérifie si la page est un CAPTCHA interne RYM."""
        try:
            title = (self._driver.title or "").lower()
            body = self._driver.find_element(By.TAG_NAME, "body").text[:2000].lower()
            combined = title + " " + body
            for indicator in CAPTCHA_INDICATORS:
                if indicator in combined:
                    return True
        except Exception:
            pass
        return False
