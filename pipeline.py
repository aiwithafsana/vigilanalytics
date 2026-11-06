"""DAG runner entry point; pipeline stages arrive in subsequent phases."""
import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through", choices=[f"stage{i}" for i in range(6)])
    parser.parse_args()
    parser.error("Only session setup is implemented. Acquisition and DAG execution require subsequent phases.")


if __name__ == "__main__":
    main()
