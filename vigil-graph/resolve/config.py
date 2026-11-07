"""Versioned Stage 2 policy. Changes require a fresh evaluation and review."""
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class ResolutionConfig:
    threshold: float = 0.85
    max_cluster_records: int = 50
    max_block_pairs: int = 2_000_000
    memory_limit: str = '3GB'
    batch_size: int = 50_000

    def __post_init__(self):
        if not 0 < self.threshold <= 1:
            raise ValueError('threshold must be in (0, 1]')
        if self.max_cluster_records < 1 or self.max_block_pairs < 1 or self.batch_size < 1:
            raise ValueError('size limits must be positive')

    def as_dict(self):
        return asdict(self)
