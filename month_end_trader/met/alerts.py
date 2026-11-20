"""Alerting: always logs; optionally Telegram and/or email, configured only through environment variables.

  MET_TELEGRAM_TOKEN, MET_TELEGRAM_CHAT_ID
  MET_SMTP_HOST, MET_SMTP_PORT (587), MET_SMTP_USER, MET_SMTP_PASSWORD, MET_ALERT_EMAIL_TO
  MET_HEARTBEAT_URL   (e.g. a healthchecks.io ping URL; pinged by the daemon loop)
"""
from __future__ import annotations

import logging
import os
import smtplib
import threading
import time
from email.message import EmailMessage

import requests

log = logging.getLogger("met.alerts")
LEVELS = {"INFO": logging.INFO, "WARN": logging.WARNING, "CRITICAL": logging.CRITICAL}


class Alerter:
    def __init__(self, mode: str = "paper", sink: list | None = None):
        self.mode = mode
        self.sink = sink            # tests capture alerts here
        self._last_hb = 0.0
        self._recent = {}            # title -> last sent time (identical titles at most every 30 min)

    def send(self, level: str, title: str, body: str = ""):
        text = f"[MET {self.mode.upper()}] {level}: {title}" + (f"\n{body}" if body else "")
        log.log(LEVELS.get(level, logging.INFO), text.replace("\n", " | "))
        if self.sink is not None:
            self.sink.append((level, title, body))
            return
        now = time.time()
        if now - self._recent.get(title, 0) < 1800:
            return
        self._recent[title] = now
        # network delivery never blocks the trading loop
        threading.Thread(target=self._deliver, args=(level, title, text), daemon=True).start()

    def _deliver(self, level, title, text) -> bool:
        ok = False
        for attempt in range(3):
            ok = self._telegram(text)
            if ok is not False:
                break
            time.sleep(2 * (attempt + 1))
        if level in ("WARN", "CRITICAL"):
            ok = self._email(f"[MET {self.mode}] {level}: {title}", text) or ok
        if ok is False:
            self._recent.pop(title, None)        # not delivered: let the next identical alert through
        return bool(ok)

    def self_test(self) -> bool:
        if not (os.environ.get("MET_TELEGRAM_TOKEN") or os.environ.get("MET_SMTP_HOST")):
            log.warning("no alert channel configured (MET_TELEGRAM_* / MET_SMTP_*)")
            return False
        return self._deliver("WARN", "alert self-test", f"[MET {self.mode.upper()}] alert self-test")

    def info(self, t, b=""): self.send("INFO", t, b)
    def warn(self, t, b=""): self.send("WARN", t, b)
    def critical(self, t, b=""): self.send("CRITICAL", t, b)

    def heartbeat(self, min_interval_s: int = 300):
        url = os.environ.get("MET_HEARTBEAT_URL")
        if not url or self.sink is not None or time.time() - self._last_hb < min_interval_s:
            return
        self._last_hb = time.time()

        def ping():
            try:
                requests.get(url, timeout=10)
            except Exception as e:
                log.warning("heartbeat ping failed: %r", e)
        threading.Thread(target=ping, daemon=True).start()

    def _telegram(self, text):
        tok, chat = os.environ.get("MET_TELEGRAM_TOKEN"), os.environ.get("MET_TELEGRAM_CHAT_ID")
        if not (tok and chat):
            return None
        try:
            r = requests.post(f"https://api.telegram.org/bot{tok}/sendMessage", timeout=10,
                              data={"chat_id": chat, "text": text[:4000]})
            ok = r.status_code == 200 and r.json().get("ok", False)
            if not ok:
                log.warning("telegram alert rejected: HTTP %s %s", r.status_code, r.text[:200])
            return ok
        except Exception as e:
            log.warning("telegram alert failed: %r", e)
            return False

    def _email(self, subject, text):
        host, to = os.environ.get("MET_SMTP_HOST"), os.environ.get("MET_ALERT_EMAIL_TO")
        if not (host and to):
            return None
        try:
            msg = EmailMessage(); msg["Subject"] = subject; msg["To"] = to
            msg["From"] = os.environ.get("MET_SMTP_USER", to); msg.set_content(text)
            with smtplib.SMTP(host, int(os.environ.get("MET_SMTP_PORT", "587")), timeout=20) as s:
                s.starttls()
                if os.environ.get("MET_SMTP_USER"):
                    s.login(os.environ["MET_SMTP_USER"], os.environ.get("MET_SMTP_PASSWORD", ""))
                s.send_message(msg)
            return True
        except Exception as e:
            log.warning("email alert failed: %r", e)
            return False
