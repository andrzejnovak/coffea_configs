import os
from coffea import processor

import parsl
from parsl.providers import SlurmProvider
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.addresses import address_by_hostname

try:
    _x509_localpath = (
        [
            l
            for l in os.popen("voms-proxy-info").read().split("\n")
            if l.startswith("path")
        ][0]
        .split(":")[-1]
        .strip()
    )
except:
    raise RuntimeError(
        "x509 proxy could not be parsed, try creating it with 'voms-proxy-init'"
    )
_x509_path = os.environ["HOME"] + f'/.{_x509_localpath.split("/")[-1]}'
os.system(f"cp {_x509_localpath} {_x509_path}")

env_extra = [
    "export XRD_RUNFORKHANDLER=1",
    f"export X509_USER_PROXY={_x509_path}",
    f'export X509_CERT_DIR={os.environ["X509_CERT_DIR"]}',
    f"export PYTHONPATH=$PYTHONPATH:{os.getcwd()}",
]

slurm_htex = Config(
    executors=[
        HighThroughputExecutor(
                    label="jobs",
                    address=address_by_hostname(),
                    prefetch_capacity=0,
                    worker_debug=True,
                    provider=SlurmProvider(
                        channel=LocalChannel(script_dir='logs_parsl'),
                        launcher=SrunLauncher(),
                        max_blocks=1,
                        init_blocks=1,
                        partition='all',
                        worker_init="\n".join(env_extra), 
                        walltime='03:00:00'
                    ),
                ),
                HighThroughputExecutor(
                    label="merges",
                    address=address_by_hostname(),
                    prefetch_capacity=0,
                    worker_debug=True,
                    provider=SlurmProvider(
                        channel=LocalChannel(script_dir='logs_parsl'),
                        launcher=SrunLauncher(),
                        max_blocks=1,
                        init_blocks=1,
                        partition='all',
                        worker_init="\n".join(env_extra), 
                        walltime='03:00:00'
                    ),
                ),
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