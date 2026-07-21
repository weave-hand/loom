"""Wire DTOs for loom_sdk's write acknowledgements.

Plain dataclasses, no pydantic (the pydantic-based ontology layer is a
separate opt-in module). Extended by later tasks (e.g. object-write acks).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class LandAck:
    """200 response from `POST /datasets/{schema}/{table}`."""

    snapshot_id: int
    dataset: str


@dataclass
class ModelLandAck:
    """200 response from `POST /models/{type}` (wire key is `type`)."""

    snapshot_id: int
    type_name: str
