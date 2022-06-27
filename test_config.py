

import json
import argparse
from rich.pretty import pprint
from pydoc import importfile
import warnings
import os

from test_processor import TestProcessor

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a single test')
    parser.add_argument('-i', '--input', dest='input', default=r'inputs/test.root', help='Input files to test on (.root, .json).')
    parser.add_argument('-c', '--config', dest='config', default=r'example_run_configs/iterative.py', help='Config file with ``run_instance``.')
    args = parser.parse_args()

    # Parse input
    if args.input.endswith('.root'):
        input_files = {"test": [args.input]}
    elif args.input.endswith('.json'):
        with open(args.input) as f:
            input_files = json.load(f)
    
    # Import 
    tested_confifg = importfile(args.config)
    run_instance = tested_confifg.run_instance
    
    # Run test
    print("X"*100)
    pprint(f"Testing: {args.config}")
    print("X"*100)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        filemeta = run_instance.preprocess(input_files, treename="Events")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        output = run_instance.run(filemeta, processor_instance=TestProcessor(), treename="Events")

    hists = output['out']
    metrics = output['metrics']

    pprint("Outputs:")
    pprint(hists)
    print()
    pprint("Metrics:")
    pprint(metrics)

    print("X"*100)
    print()