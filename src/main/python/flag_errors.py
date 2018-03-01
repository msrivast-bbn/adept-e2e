#! /bin/env python

# Takes a scoring directory.
# If the "errors" file has "ERROR"s (not WARNINGS), write an "error_count" file

import argparse, os, sys

parser = argparse.ArgumentParser()
parser.add_argument('score_dir', help='scoring directory')
args = parser.parse_args()

error_file_path = os.path.join(args.score_dir, 'errors')

# Check that there is an errors file to check
if not os.path.isfile(error_file_path):
    raise Exception("No errors file to test in score_dir: " + args.score_dir)

# Count the errors
count = 0
for line in open(error_file_path, 'r'):
    if line.startswith('ERROR: '):
        count += 1

# If there are errors, write the count
if count > 0:
    with open(os.path.join(args.score_dir, 'error_count'), 'w') as out_file:
        out_file.write('{} errors found.\n'.format(count))
