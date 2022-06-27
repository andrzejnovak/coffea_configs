from coffea import processor

from distributed import Client

client = Client()

run_instance = processor.Runner(
        metadata_cache={},
        executor=processor.DaskExecutor(client=client, worker_affinity=True),
        schema=processor.NanoAODSchema,
    chunksize=100,
    maxchunks=1,
    savemetrics=True,
    skipbadfiles=True,
)