"""Inject the anomalies of the specification into the simulated events (M2, step 5: written by Quentin)."""

from random import Random

from generator.config import Config
from generator.events import Record


def inject(records: list[Record], cfg: Config, rng: Random) -> list[Record]:
    """Step 5. For now, return the events unchanged."""
    return records
