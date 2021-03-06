#! /bin/env python

# Takes prettyprinted htlcc containers, extracts entity typing data
# aggregates it across documents, and outputs it in a tab-delimited
# format suitable for opening in excel.

# WARNING: input directory is hardcoded below. Change accordingly.
# NOTE: output files are created in your current directory.

# WARNING: Code assumes that the input directory is a directory full of 
# subdirectories which contain the part files. Comment out the first 
# loop if you are just using a single directory full of part files.

# This was developed as part of the entity typing check effort. The full
# envisioned code path is as follows:
#
# 1 Documents run through e2e, resulting in part-* files in htlcc format
# 2 entity_type_data_generator.py deserializes part files and
#   produces prettyprinted versions of the file with the same filenames in
#   the specified output directory
# 3 entity_type_pprint_tab_delimited.py (this file) runs over the files generated by this 
#   version, producing one output file per algorithm type (where algorithm types are
#   broken down by directory) in a tab-delimited format suitable for opening
#   in Excel
# 4 entity_type_find_mismatch.py processes the output of pprint_tab_delimited.py
#   and produces statistics over the data  

import sys, os

# Reset this to your input path (the output path from entity_type_data_generator.py)
# Note that this expects a directory of directories; if this is not the case
# comment out the first loop (for directory in directories) below
input_path = "/nfs/mercury-04/u60/DEFT/users/ldavis/entity_typing/output"

directories = os.listdir(input_path)
print "Found", len(directories), "directories."
j = 0;
all_entities = {}

for directory in directories:
    j += 1;
    #if j > 1:			# exits after one directory for quicker debugging
	#sys.exit();
    #if "test" not in directory:		# runs on specific directory only
    #    continue			# for targeted debugging

    print "Processing", directory, "(directory", j, "of", len(directories), ")"
    directory_path = input_path + "/" + directory
    output_file_path = input_path + directory + ".txt"
    output_file = open(output_file_path, 'w')

    entities = {}

    files = os.listdir(directory_path) # this is more fragile than the below - may pick up directories - but considerably faster
    #files = [f for f in listdir(directory_path) if isfile(join(directory_path, f))]
    print "Found", len(files), "files in", directory_path
    i = 0;
    for file in files:
        i += 1
        #if i > 2:              # exits after two files for quicker debugging
        #    sys.exit();
        #print "\t", file
        # check that file is a part file - should begin with "part -"
        if "part-" not in file:
            print "\tSkipping ", file, "(", i, "of", len(files), " in directory", j, "of", len(directories), ")"
            continue
        print "\tProcessing", file, "(", i, "of", len(files), " in directory", j, "of", len(directories), ")"

	input_file = directory_path + "/" + file
	open_file = open(input_file, 'r')
	flush = False
	output_line = ""
	for line in open_file:
	    #if flush:
		#print output_line
		#output_file.write(output_line)
		#output_file.write("\n")
		#flush = False
		#output_line = ""
	    if "hltcc->coreferences" in line:
		flush = True
	    elif "Coreferential entity" in line:
		entity_type = line.split("type ",1)[1]
		entity_type = entity_type.split(",",1)[0]
		entity_type = entity_type.split(".",1)[0]
		entity_value = line.split("value '",1)[1]
		entity_value = entity_value.split("\'",1)[0]
		#output_line = entity_value + "\t" + entity_type
		key = entity_value + "\t" + entity_type
		if key in entities:
		    entities[key] += 1
		    #print "Incrementing \t Key: " + key + "\t" + "Value: " 
		    #print entities[key]
		else:
		    entities[key] = 1
		    #print "Adding \t Key: " + key + "\t" + "Value: "
		    #print entities[key]

		#if "\"" in entity_value:
		#	print "Found quotative in file " + str(i)
		if entity_type in all_entities:
		    all_entities[entity_type] += 1
		else:
		    all_entities[entity_type] = 1

    #print "\n\n"

    # print out all entities counts
    output_file.write("ALL_ENTITIES\t")
    for key, value in all_entities.items():
	output_pair = key + ":" + str(value) + "\t"
	output_file.write(output_pair)

    output_file.write("\n")

    unify_entities = {}
    num_unified = 0
    # unify entities that have different types
    for key, value in entities.items():
	output_line = key + ":" + str(value)
	#print "Unifying " + output_line
	new_key = output_line.split("\t",1)[0]
	new_value = output_line.split("\t",1)[1]
	#print "New key:" + new_key
	#print "New value: " + new_value

	#print "Current dict:"
	#for dict_key, dict_value in unify_entities.items():
	#	print dict_key + "\t" + dict_value
	#print "End of dict."

	if new_key in unify_entities:
	    #print "Found existing value: " + unify_entities[new_key] + " for key: " + new_key
	    #print "Adding to existing value " + new_value
	    new_new_value = unify_entities[new_key] + "\t" + new_value
	    #print "Unified value: " + new_new_value
	    unify_entities[new_key] = new_new_value
	    num_unified += 1
	else:
	    #print "Adding " + new_key + "\t" + new_value
	    unify_entities[new_key] = new_value

    print "Unified " + str(num_unified) + " entities."
    
    # print out individual (unified) mentions
    for key, value in unify_entities.items():
	total = 0
	subtypes = value.split("\t")
	for subtype in subtypes:
	    total += int(subtype.split(":",1)[1])	

	output_line = str(total) + "\t" + key + "\t" + value + "\n"
	output_file.write(output_line)
