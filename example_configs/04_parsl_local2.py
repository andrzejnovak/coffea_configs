import parsl
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor
from coffea import processor

slurm_htex = Config(
    executors=[
        ThreadPoolExecutor(max_threads=3, label='jobs'),
        ThreadPoolExecutor(max_threads=3, label='merges')
    ],
    retries=2,
)
dfk = parsl.load(slurm_htex)

run_instance = processor.Runner(
    executor=processor.ParslExecutor(
        merging=True,
        merges_executors=['merges'],
        jobs_executors=['jobs'],
    ),
    schema=processor.NanoAODSchema,
    chunksize=100,
    maxchunks=1,
    savemetrics=True,
    skipbadfiles=True,
)