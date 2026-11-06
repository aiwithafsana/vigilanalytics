from ingest.acquisition import fetch_manual


def fetch(vintage: str) -> None:
    fetch_manual('shared_patients', vintage)
