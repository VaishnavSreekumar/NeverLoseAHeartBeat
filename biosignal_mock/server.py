"""Asynchronous TCP server for MayaCardio biosignal-mock.

Strictly follows Section 6.2 and 6.3:
- NDJSON over TCP on configured port.
- Accepts one client at a time.
- Emits at most 1 second of data per reply with exact zlib CRC-32.
- Handles faults: stalls, corrupt CRC-32, duplicate replies, and session restarts.
- Continuous 400 Hz dual-sensor generation loop.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any, Callable, Optional

from biosignal_mock.faults import FaultConfig
from biosignal_mock.generator import DualSensorGenerator, SAMPLING_RATE
from shared.protocol import ProtocolError, ReadCommand, format_mock_reply

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("biosignal_mock")


def compute_deadline_delay(
    start_time: float,
    batches_generated: int,
    batch_interval: float,
    now: float,
) -> float:
    """Compute sleep duration anchored to absolute schedule to prevent drift accumulation."""
    target_time = start_time + (batches_generated * batch_interval)
    return target_time - now


class MockServer:
    """TCP server serving biosignal mock data over NDJSON."""

    def __init__(
        self,
        mode: str = "signal",
        seed: int = 7,
        duration: int = 3600,
        port: int = 7100,
        host: str = "127.0.0.1",
        fault_config: Optional[FaultConfig] = None,
        ground_truth_tap: Optional[Callable[[dict[str, Any]], None]] = None,
        realtime: bool = True,
        buffer_capacity: int = 4000,
    ) -> None:
        self.host = host
        self.port = port
        self.realtime = realtime
        self.fault_config = fault_config or FaultConfig()
        
        self.generator = DualSensorGenerator(
            mode=mode,
            seed=seed,
            duration=duration,
            buffer_capacity=buffer_capacity,
            ground_truth_tap=ground_truth_tap,
        )

        self._server: Optional[asyncio.Server] = None
        self._gen_task: Optional[asyncio.Task[None]] = None
        self._client_lock = asyncio.Lock()  # Restrict to one active client
        self._stop_event = asyncio.Event()

    async def _generation_loop(self) -> None:
        """Continuously generate samples at 400 Hz into ring buffers with absolute deadline scheduling."""
        batch_size = 10  # Emit 10 samples every 25ms (400 samples/sec)
        batch_interval = batch_size / SAMPLING_RATE  # 0.025s = 25ms
        loop = asyncio.get_running_loop()
        start_time = loop.time()
        batches_generated = 0
        
        try:
            while not self._stop_event.is_set() and not self.generator.is_session_ended:
                self.generator.advance_samples(batch_size)
                batches_generated += 1
                
                # Absolute deadline calculation anchored to session timeline
                delay = compute_deadline_delay(start_time, batches_generated, batch_interval, loop.time())
                if delay > 0:
                    await asyncio.sleep(delay)
                elif delay < -1.0:
                    # Severe lag fallback: re-anchor to prevent runaway burst loop
                    start_time = loop.time() - (batches_generated * batch_interval)
        except asyncio.CancelledError:
            pass

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle incoming single-client TCP connection."""
        peer = writer.get_extra_info("peername")
        logger.info(f"Connection attempt from {peer}")

        if self._client_lock.locked():
            logger.warning(f"Rejecting second client {peer}; mock serves one client at a time.")
            writer.close()
            await writer.wait_closed()
            return

        async with self._client_lock:
            logger.info(f"Client {peer} connected successfully.")
            try:
                while not self._stop_event.is_set():
                    line = await reader.readline()
                    if not line:
                        logger.info(f"Client {peer} disconnected.")
                        break

                    try:
                        cmd = ReadCommand.from_json(line)
                    except ProtocolError as exc:
                        logger.error(f"Invalid command from client: {exc}")
                        continue

                    # Apply fault injection: Socket Stall
                    if self.fault_config.stall_seconds > 0:
                        logger.info(f"Injecting socket stall of {self.fault_config.stall_seconds}s")
                        await asyncio.sleep(self.fault_config.stall_seconds)

                    # Apply fault injection: Session Restart
                    if self.fault_config.trigger_restart:
                        logger.info("Injecting session restart fault.")
                        self.generator.restart_session()
                        self.fault_config.trigger_restart = False

                    # Apply fault injection: Selective Sensor Stall
                    if self.fault_config.sensor_stall is not None:
                        stall_sensor = self.fault_config.sensor_stall
                        logger.info(f"Injecting sensor stall fault on sensor {stall_sensor}")
                        self.generator.trigger_sensor_stall(
                            sensor=stall_sensor,
                            duration_sec=self.fault_config.sensor_stall_duration,
                            samples=self.fault_config.sensor_stall_samples,
                        )
                        self.fault_config.sensor_stall = None

                    # Read sensor samples from generator ring buffer
                    first_index, data, lost, session_end = self.generator.read_sensor(
                        sensor=cmd.sensor,
                        from_index=cmd.from_index,
                    )

                    corrupt_crc = self.fault_config.should_corrupt_crc()
                    reply_str = format_mock_reply(
                        session_id=self.generator.session_id,
                        sensor=cmd.sensor,
                        first_index=first_index,
                        data=data,
                        lost=lost,
                        session_end=session_end,
                        corrupt_crc=corrupt_crc,
                    )

                    # Transmit reply frame over wire
                    writer.write(reply_str.encode("utf-8"))
                    await writer.drain()

                    # Apply fault injection: Duplicate Reply
                    if self.fault_config.should_duplicate():
                        logger.info("Injecting duplicate reply frame.")
                        writer.write(reply_str.encode("utf-8"))
                        await writer.drain()

            except (ConnectionResetError, BrokenPipeError):
                logger.info(f"Connection reset by client {peer}.")
            finally:
                writer.close()
                await writer.wait_closed()
                logger.info(f"Connection with {peer} closed.")

    async def start(self) -> None:
        """Start the background generation loop and TCP listening socket."""
        self._server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            reuse_address=True,
        )
        if self.realtime:
            self._gen_task = asyncio.create_task(self._generation_loop())
        logger.info(f"biosignal-mock listening on {self.host}:{self.port} (mode={self.generator.mode})")

    async def stop(self) -> None:
        """Gracefully stop server and background generator."""
        self._stop_event.set()
        if self._gen_task:
            self._gen_task.cancel()
            try:
                await self._gen_task
            except asyncio.CancelledError:
                pass
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        logger.info("biosignal-mock server stopped.")


def main() -> None:
    """CLI entrypoint conforming to Section 6: biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100."""
    parser = argparse.ArgumentParser(description="MayaCardio biosignal-mock sensor service")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal", help="Generation mode")
    parser.add_argument("--seed", type=int, default=7, help="Random seed for reproducibility")
    parser.add_argument("--duration", type=int, default=3600, help="Session duration in seconds")
    parser.add_argument("--port", type=int, default=7100, help="TCP port to listen on")
    parser.add_argument("--host", default="127.0.0.1", help="Host address to bind")
    args = parser.parse_args()

    server = MockServer(
        mode=args.mode,
        seed=args.seed,
        duration=args.duration,
        port=args.port,
        host=args.host,
        realtime=True,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(server.start())
        print(f"biosignal-mock running on {args.host}:{args.port}. Press Ctrl+C to stop.")
        loop.run_forever()
    except KeyboardInterrupt:
        print("\nStopping biosignal-mock...")
    finally:
        loop.run_until_complete(server.stop())
        loop.close()


if __name__ == "__main__":
    main()
