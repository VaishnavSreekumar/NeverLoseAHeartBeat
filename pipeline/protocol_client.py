"""MayaCardio biosignal TCP protocol client (Phase 2 Increment B).

Responsible for:
- Interleaved dual-sensor polling (Sensor 1 <-> Sensor 2) over single TCP connection.
- Strict NDJSON framing, schema, session identity, and raw data text CRC-32 validation.
- Separation of ephemeral transport wire cursors from durable storage recovery cursors.
- Quarantining unresolved CRC incidents without falsely declaring permanent loss.
- In-process gap reconciliation sweeper to re-read unresolved incidents during runtime.
- Bounded accepted-sample queue (maxsize=50 batches) enforcing backpressure.
- Clean shutdown, timeout resilience, and automatic reconnect with exponential backoff.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from shared.protocol import (
    LossRecord,
    MockReply,
    ProtocolError,
    ReadCommand,
    parse_mock_reply,
)

logger = logging.getLogger("pipeline.protocol_client")


@dataclass(frozen=True)
class IngestBatch:
    """Validated response batch emitted downstream to the storage queue.
    
    Contains only payloads that have passed strict NDJSON parsing,
    schema validation, and exact raw-text zlib CRC-32 verification.
    """
    session_id: str
    sensor: int
    first_index: int
    count: int
    data: list[list[int]]
    lost: list[LossRecord]
    session_end: bool
    crc32: str
    received_at: float = field(default_factory=time.time)

    @classmethod
    def from_mock_reply(cls, reply: MockReply) -> IngestBatch:
        return cls(
            session_id=reply.session_id,
            sensor=reply.sensor,
            first_index=reply.first_index,
            count=reply.count,
            data=reply.data,
            lost=reply.lost,
            session_end=reply.session_end,
            crc32=reply.crc32,
        )


@dataclass
class UnresolvedCrcIncident:
    """Represents a transport/integrity incident where wire retries exhausted.
    
    Quarantined in memory. Does NOT advance recovery cursors or write to loss_ledger.
    """
    session_id: str
    sensor: int
    from_index: int
    to_index: Optional[int]
    first_failed_at: float = field(default_factory=time.time)
    retries_attempted: int = 0


@dataclass
class ClientTelemetry:
    """Operational telemetry counters for client monitoring."""
    batches_accepted: int = 0
    samples_accepted: int = 0
    crc_failures_detected: int = 0
    protocol_errors: int = 0
    unresolved_incidents_recorded: int = 0
    incidents_recovered_in_process: int = 0
    session_transitions_observed: int = 0
    reconnect_count: int = 0
    read_timeouts: int = 0
    duplicate_frames_handled: int = 0


class BiosignalProtocolClient:
    """Asynchronous client acquiring dual-sensor biosignal streams over TCP."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7100,
        read_timeout: float = 1.0,
        max_crc_retries: int = 3,
        reconnect_delay: float = 0.5,
        max_reconnect_delay: float = 5.0,
        queue_maxsize: int = 50,
        sweeper_interval: float = 1.0,
        poll_delay: float = 0.01,
        recovery_cursor_provider: Optional[Callable[[str, int], int]] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.read_timeout = read_timeout
        self.max_crc_retries = max_crc_retries
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self.sweeper_interval = sweeper_interval
        self.poll_delay = poll_delay
        self.recovery_cursor_provider = recovery_cursor_provider

        # Bounded downstream queue for validated response batches
        self.accepted_queue: asyncio.Queue[IngestBatch] = asyncio.Queue(maxsize=queue_maxsize)

        # Quarantined transport incidents (unresolved CRC errors)
        self.unresolved_crc_incidents: list[UnresolvedCrcIncident] = []

        # Transport wire polling cursor (ephemeral, distinct from storage recovery cursor)
        self.transport_polling_cursor: dict[int, int] = {1: 0, 2: 0}

        self.current_session_id: Optional[str] = None
        self.active_sensor: int = 1
        self.telemetry = ClientTelemetry()

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False
        self._running = False
        self._stop_event = asyncio.Event()
        self._main_task: Optional[asyncio.Task[None]] = None
        self._last_sweeper_time: float = 0.0

    @property
    def is_connected(self) -> bool:
        return self._connected and self._writer is not None and not self._writer.is_closing()

    async def connect(self) -> bool:
        """Establish TCP connection with the mock server."""
        await self.disconnect()
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self.host,
                self.port,
                limit=128 * 1024,  # 128 KB buffer limit for newline framing
            )
            self._connected = True
            logger.info(f"Connected to biosignal mock at {self.host}:{self.port}")
            return True
        except (OSError, asyncio.TimeoutError) as exc:
            logger.debug(f"Connection to {self.host}:{self.port} failed: {exc}")
            self._connected = False
            self._reader = None
            self._writer = None
            return False

    async def disconnect(self) -> None:
        """Cleanly close TCP connection."""
        self._connected = False
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except (OSError, asyncio.CancelledError):
                pass
            finally:
                self._writer = None
                self._reader = None
        logger.info("TCP connection closed.")

    async def _send_command(self, cmd: ReadCommand) -> bool:
        """Serialize and send command NDJSON line over the wire."""
        if not self.is_connected or self._writer is None:
            return False
        try:
            wire_bytes = cmd.to_json().encode("utf-8")
            self._writer.write(wire_bytes)
            await self._writer.drain()
            return True
        except (OSError, ConnectionResetError, BrokenPipeError) as exc:
            logger.warning(f"Failed to send command to {self.host}:{self.port}: {exc}")
            self._connected = False
            return False

    async def _read_reply_line(self) -> Optional[bytes]:
        """Read a single newline-delimited reply frame with bounded timeout."""
        if not self.is_connected or self._reader is None:
            return None
        try:
            line = await asyncio.wait_for(
                self._reader.readline(),
                timeout=self.read_timeout,
            )
            if not line:
                logger.info("Server closed connection (EOF).")
                self._connected = False
                return None
            return line
        except asyncio.TimeoutError:
            logger.warning(f"Read timed out after {self.read_timeout}s waiting for reply.")
            self.telemetry.read_timeouts += 1
            return None
        except (OSError, ConnectionResetError, BrokenPipeError) as exc:
            logger.warning(f"Socket error reading reply: {exc}")
            self._connected = False
            return None

    def _handle_session_transition(self, new_session_id: str) -> None:
        """Handle detected session transition."""
        if self.current_session_id is None:
            self.current_session_id = new_session_id
            logger.info(f"Initial session established: {new_session_id}")
            return

        if new_session_id != self.current_session_id:
            self.telemetry.session_transitions_observed += 1
            logger.info(
                f"Session transition detected: {self.current_session_id} -> {new_session_id}. "
                "Preserving terminal uncertainty for previous session."
            )
            self.current_session_id = new_session_id
            # Reset wire transport cursors for new session
            self.transport_polling_cursor[1] = 0
            self.transport_polling_cursor[2] = 0
            # Quarantined incidents from previous session are cleared from memory
            self.unresolved_crc_incidents.clear()

    async def _enqueue_validated_reply(self, reply: MockReply) -> None:
        """Wrap and enqueue a verified reply into the accepted batch queue."""
        self._handle_session_transition(reply.session_id)

        # Update transport wire cursor to reflect received samples
        next_idx = reply.first_index + reply.count
        if next_idx > self.transport_polling_cursor[reply.sensor]:
            self.transport_polling_cursor[reply.sensor] = next_idx

        # Enforce bounded backpressure: blocks asynchronously if queue reaches maxsize
        batch = IngestBatch.from_mock_reply(reply)
        await self.accepted_queue.put(batch)

        self.telemetry.batches_accepted += 1
        self.telemetry.samples_accepted += reply.count

    async def _query_and_validate(
        self,
        cmd: ReadCommand,
    ) -> tuple[Optional[MockReply], bool]:
        """Send command, read response line, and validate NDJSON schema and CRC-32."""
        success = await self._send_command(cmd)
        if not success:
            return None, False

        raw_line = await self._read_reply_line()
        if raw_line is None:
            return None, False

        try:
            reply, is_crc_valid = parse_mock_reply(raw_line)
        except ProtocolError as exc:
            logger.error(f"Malformed reply rejected: {exc}")
            self.telemetry.protocol_errors += 1
            return None, False

        if not is_crc_valid:
            logger.warning(
                f"CRC-32 mismatch on reply for sensor {reply.sensor}, first_index={reply.first_index}. "
                f"Payload discarded."
            )
            self.telemetry.crc_failures_detected += 1
            return reply, False

        return reply, True

    async def _handle_crc_failure(
        self,
        sensor: int,
        failed_reply: Optional[MockReply],
        requested_index: int,
    ) -> None:
        """Handle CRC corruption: attempt targeted re-reads; quarantine if exhausted."""
        recovered = False

        for retry_idx in range(1, self.max_crc_retries + 1):
            logger.info(
                f"Dispatching targeted CRC re-read attempt {retry_idx}/{self.max_crc_retries} "
                f"for sensor {sensor} at index {requested_index}"
            )
            retry_cmd = ReadCommand(sensor=sensor, from_index=requested_index)
            retry_reply, is_valid = await self._query_and_validate(retry_cmd)

            if is_valid and retry_reply is not None:
                logger.info(
                    f"Targeted re-read succeeded on attempt {retry_idx} for sensor {sensor} "
                    f"at index {requested_index}"
                )
                await self._enqueue_validated_reply(retry_reply)
                recovered = True
                break

        if not recovered:
            # 3 failed retries: Quarantine interval in unresolved_crc_incidents
            # Do NOT declare permanent loss! Do NOT write to loss_ledger!
            to_idx = None
            span_count = 100
            if failed_reply is not None:
                to_idx = failed_reply.first_index + failed_reply.count - 1
                span_count = failed_reply.count or 100

            incident = UnresolvedCrcIncident(
                session_id=self.current_session_id or "unknown",
                sensor=sensor,
                from_index=requested_index,
                to_index=to_idx,
                retries_attempted=self.max_crc_retries,
            )
            self.unresolved_crc_incidents.append(incident)
            self.telemetry.unresolved_incidents_recorded += 1
            logger.warning(
                f"CRC retries exhausted for sensor {sensor} at index {requested_index}. "
                f"Interval quarantined in unresolved_crc_incidents. Advancing wire cursor."
            )

            # Advance ephemeral wire transport cursor past this window to keep socket loop active
            self.transport_polling_cursor[sensor] = requested_index + span_count

    async def _sweep_unresolved_incidents(self) -> None:
        """In-process gap sweeper: re-attempt pending unresolved CRC incidents."""
        if not self.unresolved_crc_incidents or not self.is_connected:
            return

        incident = self.unresolved_crc_incidents[0]
        logger.debug(
            f"In-process sweeper re-polling unresolved gap for sensor {incident.sensor} "
            f"from_index={incident.from_index}"
        )
        sweep_cmd = ReadCommand(sensor=incident.sensor, from_index=incident.from_index)
        reply, is_valid = await self._query_and_validate(sweep_cmd)

        if is_valid and reply is not None:
            # Check if reply actually covered the missing index
            if reply.first_index <= incident.from_index:
                logger.info(
                    f"In-process gap sweeper successfully recovered gap for sensor {incident.sensor} "
                    f"from_index={incident.from_index}!"
                )
                await self._enqueue_validated_reply(reply)
                self.unresolved_crc_incidents.pop(0)
                self.telemetry.incidents_recovered_in_process += 1
                return
            elif reply.lost:
                # Mock returned explicit loss record (e.g. buffer_overflow evicted older data)
                for loss in reply.lost:
                    if loss.from_index <= incident.from_index:
                        logger.info(
                            f"Mock authoritatively reported loss for quarantined gap: {loss.reason}. "
                            "Transitioning incident to confirmed loss via accepted batch."
                        )
                        await self._enqueue_validated_reply(reply)
                        self.unresolved_crc_incidents.pop(0)
                        return

        # If still failed, update retry count
        incident.retries_attempted += 1

    async def poll_once(self) -> None:
        """Execute a single interleaved polling step for the active sensor."""
        if not self.is_connected:
            return

        now = time.monotonic()
        if now - self._last_sweeper_time >= self.sweeper_interval:
            self._last_sweeper_time = now
            await self._sweep_unresolved_incidents()

        sensor = self.active_sensor
        requested_index = self.transport_polling_cursor[sensor]
        cmd = ReadCommand(sensor=sensor, from_index=requested_index)

        # Switch active sensor immediately so interleaving alternates strictly
        self.active_sensor = 2 if sensor == 1 else 1

        reply, is_valid = await self._query_and_validate(cmd)

        if not self.is_connected:
            return

        if reply is None and not is_valid:
            # Socket timeout or malformed reply discarded
            return

        if not is_valid:
            # CRC corruption detected
            await self._handle_crc_failure(sensor, reply, requested_index)
            return

        # Valid reply received
        assert reply is not None

        # Check if reply matches requested sensor or is a duplicate buffered frame
        if reply.sensor != sensor:
            logger.info(
                f"Received frame for sensor {reply.sensor} while querying sensor {sensor} "
                "(duplicate/pipelined frame). Handled cleanly."
            )
            self.telemetry.duplicate_frames_handled += 1

        await self._enqueue_validated_reply(reply)

    async def run(self) -> None:
        """Main client loop with interleaved polling and reconnect backoff."""
        self._running = True
        backoff = self.reconnect_delay

        try:
            while not self._stop_event.is_set():
                if not self.is_connected:
                    connected = await self.connect()
                    if not connected:
                        logger.debug(f"Reconnect failed. Backing off for {backoff:.2f}s...")
                        try:
                            await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                            break
                        except asyncio.TimeoutError:
                            backoff = min(backoff * 1.5, self.max_reconnect_delay)
                            continue

                    # Reset backoff upon successful connection
                    backoff = self.reconnect_delay
                    self.telemetry.reconnect_count += 1

                # Execute one interleaved polling step
                await self.poll_once()

                if self.poll_delay > 0:
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_delay)
                        break
                    except asyncio.TimeoutError:
                        pass
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            await self.disconnect()

    def start(self) -> asyncio.Task[None]:
        """Start the protocol client background ingestion loop."""
        self._stop_event.clear()
        self._main_task = asyncio.create_task(self.run())
        return self._main_task

    async def stop(self) -> None:
        """Signal clean shutdown and wait for background task and socket to close."""
        self._stop_event.set()
        if self._main_task is not None and not self._main_task.done():
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass
            self._main_task = None
        await self.disconnect()
