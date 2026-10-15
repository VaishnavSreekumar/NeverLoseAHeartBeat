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
from collections import deque
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


class IntegrityConflictError(RuntimeError):
    """Raised when an incoming sample or reply has identical (session_id, sensor, sample_index)
    but conflicting channel values."""
    pass


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
    terminal: bool = False  # Set to True when source session ends without resolution


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
        """Initialize the protocol client.
        
        Args:
            host: IP address or hostname of mock server.
            port: TCP port of mock server.
            read_timeout: Bounded socket read timeout in seconds.
            max_crc_retries: Number of targeted re-read attempts after an initial CRC failure
                (1 initial attempt + max_crc_retries re-reads = 1 + max_crc_retries total reads).
            reconnect_delay: Initial reconnect backoff delay in seconds.
            max_reconnect_delay: Maximum reconnect backoff delay cap.
            queue_maxsize: Maximum number of accepted IngestBatch items in downstream queue.
            sweeper_interval: Seconds between in-process gap reconciliation sweeps.
            poll_delay: Delay between poll steps.
            recovery_cursor_provider: Optional callback providing persisted recovery cursor.
        """
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

        # Quarantined transport incidents for the current session (eligible for in-process sweep)
        self.unresolved_crc_incidents: list[UnresolvedCrcIncident] = []

        # Archived terminal incidents from previous sessions preserved as permanent uncertainty
        # (Preserves session_id and affected range; not eligible for sweep by new session)
        self.terminal_unresolved_incidents: list[UnresolvedCrcIncident] = []

        # Transport wire polling cursor (ephemeral, distinct from storage recovery cursor)
        self.transport_polling_cursor: dict[int, int] = {1: 0, 2: 0}

        # Bounded cache of recent samples for duplicate integrity validation:
        # Key: (session_id, sensor, sample_index) -> Value: list[int] channel values
        # NOTE: This cache is bounded to 10,000 samples (~12.5s dual-sensor window) to satisfy
        # the ARM64 memory bound (REQ-H-02). Durable, full-history duplicate-conflict detection
        # across arbitrary time horizons MUST be enforced by Increment C at the SQLite storage layer
        # via PRIMARY KEY (session_id, sensor, sample_index).
        self._recent_samples: dict[tuple[str, int, int], list[int]] = {}
        self._recent_sample_keys: deque[tuple[str, int, int]] = deque()
        self._max_recent_samples: int = 10000

        self.current_session_id: Optional[str] = None
        self.active_sensor: int = 1
        self.telemetry = ClientTelemetry()

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False
        self._running = False
        self._stop_event = asyncio.Event()
        self.storage_error_event: asyncio.Event = asyncio.Event()
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
        except ValueError as exc:
            logger.error(f"Frame line exceeded buffer limit (framing violation): {exc}")
            self.telemetry.protocol_errors += 1
            self._connected = False
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
            # If a recovery cursor provider exists, check if this session has a committed recovery checkpoint
            if self.recovery_cursor_provider:
                s1_rec = self.recovery_cursor_provider(new_session_id, 1)
                if s1_rec >= 0:
                    self.transport_polling_cursor[1] = s1_rec + 1
                s2_rec = self.recovery_cursor_provider(new_session_id, 2)
                if s2_rec >= 0:
                    self.transport_polling_cursor[2] = s2_rec + 1
            return

        if new_session_id != self.current_session_id:
            self.telemetry.session_transitions_observed += 1
            logger.info(
                f"Session transition detected: {self.current_session_id} -> {new_session_id}. "
                "Preserving terminal uncertainty for previous session."
            )
            # Archive unrecovered incidents from previous session as terminal uncertainty
            # Prevents silent erasure; preserves session_id and affected range untouched
            for inc in self.unresolved_crc_incidents:
                inc.terminal = True
                self.terminal_unresolved_incidents.append(inc)
            self.unresolved_crc_incidents.clear()

            self.current_session_id = new_session_id
            # Reset wire transport cursors for new session or initialize from recovery provider
            if self.recovery_cursor_provider:
                s1_rec = self.recovery_cursor_provider(new_session_id, 1)
                self.transport_polling_cursor[1] = s1_rec + 1 if s1_rec >= 0 else 0
                s2_rec = self.recovery_cursor_provider(new_session_id, 2)
                self.transport_polling_cursor[2] = s2_rec + 1 if s2_rec >= 0 else 0
            else:
                self.transport_polling_cursor[1] = 0
                self.transport_polling_cursor[2] = 0

            # Clear recent sample cache for new session
            self._recent_samples.clear()
            self._recent_sample_keys.clear()

    async def _enqueue_validated_reply(self, reply: MockReply) -> None:
        """Wrap and enqueue a verified reply into the accepted batch queue.
        
        Validates duplicate integrity:
        - Identical duplicates are safely ignored with duplicate telemetry incremented.
        - Conflicting duplicates (same index, different channel values) raise IntegrityConflictError.
        """
        self._handle_session_transition(reply.session_id)

        # Check for sample-level duplicate conflicts
        all_duplicate = True if (reply.data or not reply.lost) else False
        new_samples_to_record: list[tuple[tuple[str, int, int], list[int]]] = []

        for offset, sample_vals in enumerate(reply.data):
            idx = reply.first_index + offset
            key = (reply.session_id, reply.sensor, idx)
            if key in self._recent_samples:
                existing_vals = self._recent_samples[key]
                if existing_vals != sample_vals:
                    msg = (
                        f"Integrity conflict on session {reply.session_id}, sensor {reply.sensor}, "
                        f"index {idx}: existing values {existing_vals} != incoming values {sample_vals}"
                    )
                    logger.error(msg)
                    raise IntegrityConflictError(msg)
            else:
                all_duplicate = False
                new_samples_to_record.append((key, sample_vals))

        if reply.data and all_duplicate and not reply.lost:
            # Completely redundant identical duplicate frame
            logger.info(
                f"Identical duplicate reply frame safely discarded for sensor {reply.sensor}, "
                f"first_index={reply.first_index}, count={reply.count}."
            )
            self.telemetry.duplicate_frames_handled += 1
            return

        # Record new sample values in bounded recent cache
        for key, vals in new_samples_to_record:
            self._recent_samples[key] = vals
            self._recent_sample_keys.append(key)
            if len(self._recent_sample_keys) > self._max_recent_samples:
                old_key = self._recent_sample_keys.popleft()
                self._recent_samples.pop(old_key, None)

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
                # If during retries the session changed, handle session transition cleanly
                if self.current_session_id is not None and retry_reply.session_id != self.current_session_id:
                    logger.info(
                        f"Session transition detected during CRC retry: {self.current_session_id} -> {retry_reply.session_id}."
                    )
                    self._handle_session_transition(retry_reply.session_id)
                    await self._enqueue_validated_reply(retry_reply)
                    recovered = True
                    break

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

            incident_session = self.current_session_id or (failed_reply.session_id if failed_reply and failed_reply.session_id else "unknown")
            incident = UnresolvedCrcIncident(
                session_id=incident_session,
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

        # Guard: If incident belongs to an old session, archive it to terminal_unresolved_incidents
        if self.current_session_id is not None and incident.session_id != self.current_session_id:
            logger.info(
                f"Archiving quarantined incident from prior session {incident.session_id} as terminal uncertainty "
                f"(current session is {self.current_session_id})."
            )
            old_inc = self.unresolved_crc_incidents.pop(0)
            old_inc.terminal = True
            self.terminal_unresolved_incidents.append(old_inc)
            return

        logger.debug(
            f"In-process sweeper re-polling unresolved gap for sensor {incident.sensor} "
            f"from_index={incident.from_index}"
        )
        sweep_cmd = ReadCommand(sensor=incident.sensor, from_index=incident.from_index)
        reply, is_valid = await self._query_and_validate(sweep_cmd)

        if is_valid and reply is not None:
            # Check for session transition detected during sweep
            if self.current_session_id is not None and reply.session_id != self.current_session_id:
                logger.info(
                    f"Session transition detected during sweep: {self.current_session_id} -> {reply.session_id}. "
                    "Preserving terminal uncertainty for previous session."
                )
                self._handle_session_transition(reply.session_id)
                await self._enqueue_validated_reply(reply)
                return

            if reply.session_id != incident.session_id:
                # Reply is from a different session than the incident
                logger.warning(
                    f"Reply session {reply.session_id} does not match incident session {incident.session_id}. "
                    "Cannot attribute new session data to old incident."
                )
                self._handle_session_transition(reply.session_id)
                await self._enqueue_validated_reply(reply)
                return

            # Check if reply actually covered the missing index
            if reply.first_index <= incident.from_index and (reply.first_index + reply.count) > incident.from_index:
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
                    if loss.from_index <= incident.from_index and (loss.to_index is None or loss.to_index >= incident.from_index):
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
        if not self.is_connected or self.storage_error_event.is_set():
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

        # Check if reply matches requested sensor or is a pipelined frame
        if reply.sensor != sensor:
            logger.info(
                f"Received frame for sensor {reply.sensor} while querying sensor {sensor} "
                "(pipelined frame). Handled cleanly."
            )

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
