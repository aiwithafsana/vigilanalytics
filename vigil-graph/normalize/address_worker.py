"""Pure, process-safe address batches; workers never access files or databases."""
from common.normalization import normalize_address


def normalize_batch(rows):
    return [(ordinal, normalize_address(dict(line1=line1,line2=line2,city=city,state=state,zip=postal)))
            for ordinal,line1,line2,city,state,postal in rows]
