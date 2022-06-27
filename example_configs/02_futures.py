from coffea import processor

run_instance = processor.Runner(
    metadata_cache={},
    executor=processor.FuturesExecutor(
        workers=6, 
        recoverable=True, 
        merging=True, 
        mergepool=2,
    ),
    schema=processor.NanoAODSchema,
    chunksize=100,
    maxchunks=1,
    savemetrics=True,
    skipbadfiles=True,
)