#! /bin/env python

from argparse import ArgumentParser
from collections import defaultdict
import glob, sys, os, os.path, codecs

parser = ArgumentParser()
parser.add_argument('kb_file_in')
parser.add_argument('kb_file_out')
parser.add_argument('filtered_rels')
parser.add_argument('rel_arg_types_file')
args = parser.parse_args()

##rel_arg_types_file = '/nfs/mercury-04/u39/DEFT/users/hprovenz/adept-e2e/src/main/resources/adept/utilities/rel_arg_types.tsv'
rel_obj_types = defaultdict(set)
for line in open(args.rel_arg_types_file, 'r'):
    (rel, ent_type) = line.rstrip().split('\t')
    rel_obj_types[rel].add(ent_type)

# Process the file once to grab all the types
id_types = {}
for line in codecs.open(args.kb_file_in, 'r', 'utf-8'):
    fields = line.rstrip().split('\t')
    if len(fields) >= 3:
        (id, op, val) = fields[:3]
        if op == 'type':
            # if id in id_types:
            #     raise Exception('id has multiple types: ' + id)
            id_types[id] = val

# Process again, copying the file, but skipping bad relation type lines
good_rel_count = 0
bad_rel_count = 0
error_count = 0
with codecs.open(args.kb_file_out, 'w', 'utf-8') as out_file:
    with codecs.open(args.filtered_rels, 'w', 'utf-8') as filtered_file:
        for i, line in enumerate(codecs.open(args.kb_file_in, 'r', 'utf-8')):
            fields = line.rstrip().split('\t')
            if len(fields) == 1:
                # run name
                out_file.write(line)
            else:
                try:
                    (id, op, val) = fields[:3]
                except(ValueError):
                    print "ERROR: Line (after the first) with less than 3 fields!!"
                    print "On line:", i+1
                    print line
                    # continue
                    raise Exception("Found problem line")
                tail = fields[3:]
                if op in ('type', 'canonical_mention', 'mention', 'nominal_mention', 'normalized_mention'):
                    out_file.write(line)
                elif op in rel_obj_types:
                    subj_spec_type = op[:3].upper()
                    try:
                        subj_type = id_types[id]
                    except(KeyError):
                        print '..missing type for id:', id
                        error_count += 1
                        subj_type = 'undefined'
                    obj_spec_types = rel_obj_types[op]
                    try:
                        obj_type = id_types[val]
                    except(KeyError):
                        if val[0] == '"' and val[len(val)-1] == '"':
                            obj_type = "STRING"
                        else:
                            obj_type = "BAD STRING"
                            print '..missing type for val:', val
                            error_count += 1
                    if (subj_type == subj_spec_type) and (obj_type in obj_spec_types):
                        out_file.write(line)
                        good_rel_count += 1
                    else:
                        filtered_file.write(line)
                        bad_rel_count += 1
                
print "In filter_rel_args, good_rels:", good_rel_count, ", rejected:", bad_rel_count, ", errors:", error_count
