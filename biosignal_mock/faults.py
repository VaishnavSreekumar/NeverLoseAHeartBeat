"""Fault injection configurations and hooks for MayaCardio biosignal-mock.

Strictly follows Section 6.3:
1. Stop answering for a while (socket stall)
2. Send a corrupt reply (wrong crc32)
3. Send the same reply twice (duplicate reply)
4. Stop sending one sensor for a while (sensor stall / explicit lost)
5. Restart session (new session_id, index resets to 0, unread data lost)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FaultConfig:
    """Configurable fault injection parameters for testing resilience."""
    # Probability (0.0 to 1.0) or count-based flags
    corrupt_crc: bool = False
    corrupt_crc_prob: float = 0.0
    
    duplicate_reply: bool = False
    duplicate_reply_prob: float = 0.0
    
    stall_seconds: float = 0.0
    
    # Selective sensor suppression
    sensor_stall: Optional[int] = None
    sensor_stall_duration: float = 0.0
    sensor_stall_samples: int = 0
    
    # Session restart simulation
    trigger_restart: bool = False

    def should_corrupt_crc(self) -> bool:
        """Evaluate if the current reply should have a corrupted CRC."""
        if self.corrupt_crc:
            return True
        if self.corrupt_crc_prob > 0.0:
            import random
            return random.random() < self.corrupt_crc_prob
        return False

    def should_duplicate(self) -> bool:
        """Evaluate if the current reply should be transmitted twice."""
        if self.duplicate_reply:
            return True
        if self.duplicate_reply_prob > 0.0:
            import random
            return random.random() < self.duplicate_reply_prob
        return False
