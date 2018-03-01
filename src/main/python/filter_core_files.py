#! /bin/env python

# Filter a 2017-format EDL-only KB file down
# to entities and mentions from the files in the "core" EDL eval set.

from argparse import ArgumentParser
import glob, sys, os, os.path, codecs
from collections import defaultdict

parser = ArgumentParser()
parser.add_argument('language', help="EN or CH")
parser.add_argument('in_file')
parser.add_argument('out_file')
args = parser.parse_args()

if not args.language in ('EN', 'CH'):
    raise Exception("Language must be EN or CH.")

if args.language == 'EN':
    df_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata_orig/2016/LDC2016E64_TAC_KBP_2016_Evaluation_Core_Source_Corpus/data/eng/df/'
    nw_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata_orig/2016/LDC2016E64_TAC_KBP_2016_Evaluation_Core_Source_Corpus/data/eng/nw/'
elif args.language == 'CH':
    df_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata/chi/core_edl_df/'
    nw_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata/chi/core_edl_nw/'

df_paths = glob.glob(df_dir + '*.xml')
df_files = [os.path.basename(path)[:-4] for path in df_paths]
df_files_set = set(df_files)

nw_paths = glob.glob(nw_dir + '*.xml')
nw_files = [os.path.basename(path)[:-4] for path in nw_paths]
nw_files_set = set(nw_files)

print("Found both newswire and DF files:", nw_files[0], df_files[0])

files_set = df_files_set | nw_files_set

# Collect set of core docs for each entity
entity_docs = defaultdict(set)
for line in open(args.in_file, 'r'):
    fields = line.rstrip().split('\t')
    if len(fields) < 3:
        continue
    (id, pred) = fields[:2]
    if pred in ('mention', 'canonical_mention', 'nominal_mention'):
        #######(str, prov, conf) = fields[2:5]
        (str, prov) = fields[2:4]
        ## We're assuming only a single provenance for each mention
        (doc, span) = prov.split(':')
        if doc in files_set:
            entity_docs[id].add(doc)

# Process the input file, writing the filtered output
with codecs.open(args.out_file, 'w', 'utf-8') as o:
    for line in codecs.open(args.in_file, 'r', 'utf-8'):
        fields = line.rstrip().split(u'\t')
        if len(fields) < 3:
            o.write(line)
            continue
        (id, pred) = fields[:2]
        if id in entity_docs:
            if pred in ('mention', 'canonical_mention', 'nominal_mention'):
                #######(str, prov, conf) = fields[2:5]
                (str, prov) = fields[2:4]
                (doc, span) = prov.split(':')
                if doc in entity_docs[id]:
                    o.write(line)
            else:
                o.write(line)
