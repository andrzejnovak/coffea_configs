

import argparse
from rich.pretty import pprint
import os

# Find available run instances
path = os.path.join(os.path.dirname(__file__), 'example_configs')
configs = sorted([os.path.realpath(os.path.join(path, fn)) for fn in os.listdir(path) if fn.endswith(".py") and "init" not in fn])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a single test')
    parser.add_argument('-i', '--input', dest='input', default=r'inputs/test.root', help='Input files to test on (.root, .json).')
    parser.add_argument('-c', '--config', dest='config', default=r'example_run_configs/iterative.py', help='Config file with ``run_instance``.')
    args = parser.parse_args()

for config in configs:

    to_run = f"python test_config.py -i {args.input} -c {config}"
    os.system(to_run)