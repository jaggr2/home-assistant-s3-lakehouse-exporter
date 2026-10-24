"""Secret redaction for logs, health output and notifications.

Anything that could leak credentials — access keys, session tokens, webhook
URLs, catalog URIs (which may embed database passwords) — is replaced with
``***`` before it is logged or exposed.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Iterable

    from exporter.config import Options

MASK = "***"


class Redactor:
    def __init__(self, secrets: Iterable[str] = ()) -> None:
        self._secrets = tuple(
            sorted((secret for secret in secrets if secret), key=len, reverse=True)
        )

    @classmethod
    def from_options(cls, options: Options) -> Redactor:
        return cls(
            secret
            for secret in (
                options.auth.secret_access_key,
                options.auth.session_token,
                options.auth.access_key_id,
                options.runtime.notify_webhook,
                options.format.catalog_uri,
            )
            if secret
        )

    def redact(self, text: str) -> str:
        for secret in self._secrets:
            text = text.replace(secret, MASK)
        return text


class RedactingFilter(logging.Filter):
    """Logging filter that masks secrets in every record."""

    def __init__(self, redactor: Redactor) -> None:
        super().__init__()
        self._redactor = redactor

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        redacted = self._redactor.redact(message)
        if redacted != message:
            record.msg = redacted
            record.args = ()
        return True
