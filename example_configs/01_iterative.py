from coffea import processor

run_instance = processor.Runner(
    metadata_cache={},
    executor=processor.IterativeExecutor(),
    schema=processor.NanoAODSchema,
    chunksize=100,
    maxchunks=1,
    savemetrics=True,
    skipbadfiles=True,
)