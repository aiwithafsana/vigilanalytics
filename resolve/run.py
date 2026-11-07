"""Canonical records -> guarded, auditable entity resolution. No raw access."""
import shutil
import tempfile
from pathlib import Path

from resolve import blocking, clustering, scoring, survivorship
from resolve.config import ResolutionConfig
from resolve.storage import PROJECT_ROOT, connect, data_root, write_json
from resolve import eval as evaluation
from resolve.storage import load


def run(canonical=None, output=None, config=None, review_path=None, evaluate_sample=True):
    config = config or ResolutionConfig()
    canonical = Path(canonical or data_root()/'canonical')
    output = Path(output or data_root()/'resolved')
    output.mkdir(parents=True,exist_ok=True)
    write_json(output/'gate.json',{'status':'building','policy':config.as_dict()})
    scratch = PROJECT_ROOT/'work'/'resolve'
    scratch.mkdir(parents=True,exist_ok=True)
    # Publish resolved identities only after ALL gates pass. Review artifacts
    # stay available on failure; a prior successful build is never overwritten.
    with tempfile.TemporaryDirectory(dir=scratch) as tmp:
        build = Path(tmp)/'build'
        build.mkdir()
        with connect(Path(tmp)/'stage2.duckdb',config) as c:
            print('Stage 2: loading canonical records',flush=True)
            load(c,canonical)
            try:
                if evaluate_sample:
                    evaluation.sample(c,build)
                    for name in ('labeled_pairs.parquet','labeled_pairs.csv','human_review.md','review_request.json'):
                        shutil.copy2(build/name,output/name)
                clustering.preflight(c,build,config)
                blocking.generate(c,build,config)
                scoring.score(c,build)
                clustering.cluster(c,build,config)
                survivorship.publish(c,build)
                if evaluate_sample:
                    evaluation.evaluate(c,build,config)
                    evaluation.require_review(build,review_path)
            except clustering.ResolutionGateError as error:
                if not (build/'gate.json').exists():
                    write_json(build/'gate.json',{'status':'blocked','reason':str(error),'policy':config.as_dict(),
                                                 'national_clustering_completed':False})
                for path in build.iterdir():
                    # Never expose unaccepted golden tables as successful output.
                    if path.name not in {'entities.parquet','entity_addresses.parquet','entity_npis.parquet','record_to_entity.parquet'}:
                        shutil.copy2(path,output/path.name)
                raise clustering.ResolutionGateError(f'{error}. Artifacts: {output}') from error
            write_json(build/'gate.json',{'status':'complete','policy':config.as_dict(),
                'human_review_required':evaluate_sample,
                'records':c.execute('SELECT count(*) FROM nodes').fetchall()[0][0],
                'entities':c.execute('SELECT count(*) FROM entity_rows').fetchall()[0][0]})
        # gate.json is copied last, so interrupted publication cannot claim success.
        files=sorted(build.iterdir(),key=lambda p:p.name=='gate.json')
        for path in files:
            shutil.copy2(path,output/path.name)
    print('Stage 2 complete; stop before edge construction.',flush=True)
    return [output/p.name for p in files]
