"""Add-on entry point: wire config, spool, capture, batching, health together.

Lifecycle (see AGENTS.md):

1. load and validate options, configure logging + secret redaction,
2. open the durable spool and the configured writer,
3. start the health endpoint (Supervisor watchdog),
4. stream from Home Assistant + backfill history / repair gaps,
5. drain the spool to storage in micro-batches,
6. snapshot registries daily (optional), notify webhook on status changes.

Shutdown is graceful: on SIGTERM/SIGINT the stream stops, an in-flight flush
finishes and the spool (with everything not yet written) stays on disk.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

from exporter import __version__
from exporter.batcher import Drainer, Pressure
from exporter.capture import AttributeFilter, CaptureService, build_client
from exporter.config import ConfigError, Options, load_options
from exporter.health import HealthServer, HealthState
from exporter.notify import Monitor, Notifier
from exporter.redact import RedactingFilter, Redactor
from exporter.registry import RegistryService
from exporter.spool import Spool
from exporter.writers import create_writer
from exporter.writers.base import S3ObjectStore

LOGGER = logging.getLogger("exporter")

DEFAULT_SPOOL_PATH = "/data/spool.db"
DEFAULT_HEALTH_PORT = 8099


def setup_logging(level: str, redactor: Redactor) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    handler.addFilter(RedactingFilter(redactor))
    root = logging.getLogger()
    root.setLevel(level.upper())
    root.addHandler(handler)
    for noisy in ("aiohttp", "botocore", "boto3", "s3transfer", "urllib3", "pyiceberg"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def apply_aws_env(options: Options) -> None:
    """Populate the standard AWS SDK environment from the add-on config.

    Config wins over environment (config -> SDK chain), which is the add-on's
    documented credential behaviour.
    """
    auth = options.auth
    if auth.access_key_id:
        os.environ["AWS_ACCESS_KEY_ID"] = auth.access_key_id
    if auth.secret_access_key:
        os.environ["AWS_SECRET_ACCESS_KEY"] = auth.secret_access_key
    if auth.session_token:
        os.environ["AWS_SESSION_TOKEN"] = auth.session_token
    os.environ["AWS_DEFAULT_REGION"] = options.s3.region
    os.environ["AWS_REGION"] = options.s3.region


def supervisor_token() -> str:
    token = os.environ.get("HA_TOKEN") or os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        raise ConfigError("no Home Assistant token: set SUPERVISOR_TOKEN (or HA_TOKEN)")
    return token


async def async_main() -> int:
    options = load_options()
    redactor = Redactor.from_options(options)
    setup_logging(options.runtime.log_level, redactor)
    apply_aws_env(options)
    token = supervisor_token()
    LOGGER.info("Home Assistant S3 Lakehouse Exporter %s starting", __version__)

    spool = Spool(
        os.environ.get("SPOOL_PATH", DEFAULT_SPOOL_PATH),
        options.runtime.spool_max_bytes,
    )
    writer = create_writer(S3ObjectStore(options), options)

    attributes = AttributeFilter(
        options.capture.attributes, options.capture.attribute_allowlist
    )
    pressure = Pressure(spool, float(options.capture.flush_interval_seconds), attributes)
    status = HealthState(
        spool,
        pressure=pressure,
        flush_interval=float(options.capture.flush_interval_seconds),
        redactor=redactor,
    )
    health = HealthServer(status, port=int(os.environ.get("HEALTH_PORT", DEFAULT_HEALTH_PORT)))
    notifier = Notifier(options.runtime.notify_webhook)
    client = build_client(options, token)
    capture = CaptureService(options, spool, client, attribute_filter=attributes)
    monitor = Monitor(status, notifier, capture=capture)
    drainer = Drainer(spool, writer, options, pressure=pressure, status=status)
    registry = RegistryService(options, spool, client) if options.capture.registries else None

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    tasks = [
        asyncio.create_task(health.run(stop_event), name="health"),
        asyncio.create_task(drainer.run(stop_event), name="drainer"),
        asyncio.create_task(capture.run(stop_event), name="capture"),
        asyncio.create_task(monitor.run(stop_event), name="monitor"),
    ]
    if registry is not None:
        tasks.append(asyncio.create_task(registry.run(stop_event), name="registry"))

    await stop_event.wait()
    LOGGER.info("shutdown requested")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await client.close()
    spool.close()
    LOGGER.info("shutdown complete")
    return 0


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, request_stop)
        except (NotImplementedError, RuntimeError):  # e.g. Windows
            signal.signal(sig, lambda *_args: request_stop())


def main() -> None:
    try:
        exit_code = asyncio.run(async_main())
    except ConfigError as err:
        logging.getLogger("exporter").critical("invalid configuration: %s", err)
        raise SystemExit(1) from err
    except KeyboardInterrupt:
        exit_code = 130
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
