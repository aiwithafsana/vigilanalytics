"""Deterministic batch DAG entry point for acquisition and guarded normalization."""
import argparse
import importlib
from pathlib import Path
import sys

from common.manifest import ImmutabilityError
from ingest.acquisition import AcquisitionError, import_files
from ingest.registry import print_status, statuses
from ingest.sources import SOURCES
from ingest.validation import ValidationError


def run_stage0() -> None:
    failures = []
    for row in statuses():
        if row.state.endswith('MISSING'):
            if SOURCES[row.source].required:
                failures.append(row.source)
                print(f'{row.source}: REQUIRED INPUT MISSING\nDownload: {SOURCES[row.source].url}\n{SOURCES[row.source].instructions}')
            else:
                print(f'{row.source}: optional source absent')
            continue
        importlib.import_module(f'ingest.{row.source}').fetch(row.vintage)
    if failures:
        raise AcquisitionError(f'Stage 0 gate incomplete; missing required sources: {", ".join(failures)}')
    print('Stage 0 complete: all four required sources verified and sealed.')


def _assignments(values: list[str]) -> dict[str, str]:
    result = {}
    for value in values:
        if '=' not in value:
            raise AcquisitionError('Expected NAME=VALUE')
        name, content = value.split('=', 1)
        if name in result:
            raise AcquisitionError(f'Duplicate assignment: {name}')
        result[name] = content
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--through', choices=[f'stage{i}' for i in range(6)])
    parser.add_argument('--review', type=Path, help='Stage 2 human review attestation JSON')
    sub = parser.add_subparsers(dest='command')
    status = sub.add_parser('status', help='Print latest vintage and days since acquisition')
    status.add_argument('--verify', action='store_true', help='Rehash complete vintages (may read many GB)')
    for command in ['fetch', 'import']:
        action = sub.add_parser(command)
        action.add_argument('source', choices=SOURCES)
        action.add_argument('--vintage', required=True)
        if command == 'import':
            action.add_argument('--file', action='append', required=True, metavar='NAME=PATH')
            action.add_argument('--source-url', action='append', default=[], metavar='NAME=HTTPS_URL')
    args = parser.parse_args(argv)
    if args.through and args.command:
        parser.error('Choose either --through or a subcommand')
    try:
        if args.command == 'status':
            print_status(verify=args.verify)
        elif args.command == 'fetch':
            importlib.import_module(f'ingest.{args.source}').fetch(args.vintage)
        elif args.command == 'import':
            import_files(args.source, args.vintage,
                         {k: Path(v) for k, v in _assignments(args.file).items()},
                         source_urls=_assignments(args.source_url))
        elif args.through == 'stage2':
            from resolve.run import run
            run(review_path=args.review)
        elif args.through == 'stage1':
            from normalize.run import run
            run()
        elif args.through == 'stage0':
            run_stage0()
        elif args.through:
            raise AcquisitionError(f'{args.through} is not implemented; stop after Phase C. No later stage was run.')
        else:
            parser.print_help()
            return 2
    except (AcquisitionError, ValidationError, ImmutabilityError, OSError, ValueError) as error:
        print(f'ERROR: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
