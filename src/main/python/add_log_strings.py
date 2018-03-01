#! /bin/env python
# Add the strings to an EDL "analysis_log" file

from argparse import ArgumentParser
import glob, sys, os, os.path, codecs

def add_string(ref, file_name, contents):
    source = ref[0]
    fields = ref[2:-1].split(u'|')
    assert len(fields) == 3
    (types, offsets, ext_kbid) = fields
    (start_str, end_str) = offsets.split(':')
    start = int(start_str)
    end = int(end_str)
    span = contents[file_name][start:end+1]
    span = span.replace(u'\n', ' ')
    return u'{}[{}|{}|"{}"|{}]'.format(source, types, offsets, span, ext_kbid)
        
parser = ArgumentParser()
parser.add_argument('language', help='EN or CH')
parser.add_argument('in_file')
parser.add_argument('out_file')
parser.add_argument('-y', '--year', default="2016", help="either 2015 or 2016")
args = parser.parse_args()

if args.year == "2016":
    # corpus for 2016
    if args.language == 'EN':
        df_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata_orig/2016/LDC2016E64_TAC_KBP_2016_Evaluation_Core_Source_Corpus/data/eng/df/'
        nw_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata_orig/2016/LDC2016E64_TAC_KBP_2016_Evaluation_Core_Source_Corpus/data/eng/nw/'
    elif args.language == 'CH':
        df_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata/chi/core_edl_df/'
        nw_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata/chi/core_edl_nw/'
    else:
        raise Exception("Language arg must be EN or CH.")
else:
    raise Exception("2015 not handled yet")

# load all the file contents
df_paths = glob.glob(df_dir + '*.xml')
nw_paths = glob.glob(nw_dir + '*.xml')
paths = df_paths + nw_paths

contents = {}
for path in paths:
    file_name = os.path.basename(path)[:-4]
    contents[file_name] = codecs.open(path, 'r', 'utf8').read()

with codecs.open(args.out_file, 'w', 'utf8') as out_file:
    for (i, line) in enumerate(codecs.open(args.in_file, 'r', 'utf8')):
        line = line.rstrip()
        fields = line.split(u'\t')
        assert len(fields) == 4
        file_name, score_class, gold, system  = fields
        if gold != u'g""':
            gold = add_string(gold, file_name, contents)
        if system != u's""':
            system = add_string(system, file_name, contents)
        out_file.write(u'\t'.join((file_name, score_class, gold, system)) + '\n')

