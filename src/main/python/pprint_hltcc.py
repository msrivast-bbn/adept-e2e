#! /bin/env python

# Produce a pretty-printed view of an HLTCC file,
# either a freestanding file,
# or by searching in a partition "part-NNNNN" file
# given an unambivuous substring of the file name.

# The Java class PprintHltcc that this script calls
# takes a file as input, and writes to a file
# (It de-shades the classes in the file appropriately.)
# For input extracted from a larger part-NNNNN file,
# or for output to the terminal, temporarty files are used.

# Maven exec is used to call the Java, but its stdout
# and stderr streams are by default thrown away as uninteresting.
# If there is an error, running the script again
# with -d will show all output.

import sys, os, argparse, codecs, re, tempfile

parser = argparse.ArgumentParser()
parser.add_argument("in_file", help = "HLTCC file or Part-N partition file containing the HTLCC")
parser.add_argument("-s", "--search_doc_name_substring", help = "unique doc name substring for HLTCC if in_file is a part file")
parser.add_argument("-o", "--out_file", help="output file to use in place of stdout")
parser.add_argument("-d", "--debug", help="do not suppress stdout and stderr", action='store_true')
parser.add_argument("-r", "--retain", help="retain temporary files for debugging as /tmp/pprint_htlcc_in and /tmp/pprint_htlcc_out", action='store_true')
args = parser.parse_args()

# Creating the temporary files, but closing them immeidately,
# so that we can reopen them later using codecs.
# (In Python 3, this will not be necessary.)
temp_in_file = tempfile.NamedTemporaryFile(delete=False)
temp_in_file_path = temp_in_file.name
temp_in_file.close()
temp_out_file  = tempfile.NamedTemporaryFile(delete=False)
temp_out_file_path = temp_out_file.name
temp_out_file.close()

if args.retain:
    temp_in_file_path = "/tmp/pprint_hltcc_in"
    temp_out_file_path = "/tmp/pprint_hltcc_out"
    print "temp_in_file_path:", temp_in_file_path
    print "temp_out_file_path:", temp_out_file_path


if args.search_doc_name_substring == None:
    # Input file is a sincle HLTCC file
    in_file_path = args.in_file
else:
    # Input file is a part-N file, so try to find the right piece of it
    past_start = False
    past_end = False
    pre_start_count = 0
    region_count = 0
    post_end_count = 0
    region_lines = []
    for line in  codecs.open(args.in_file, 'r', 'utf-8'):
        if not past_start:
            pre_start_count += 1
            if line.find(args.search_doc_name_substring) >= 0:
                past_start = True
                region_lines.append(line)
        elif not past_end:
            if line.find(r'(hdfs://hadoop') == 0 or line.find(r'(file:') == 0: #(file: is the prefix present when e2e is run locally
                past_end = True
            else:
                region_count += 1
                region_lines.append(line)
        else:
            post_end_count += 1
            if line.find(args.search_doc_name_substring) >= 0:
                raise Exception("More than one region match that substring")

    if not past_start:
        raise Exception("Did not find that substring in the part file")

    # print "pre_start_count:", pre_start_count
    # print "region_count:", region_count
    # print "post_end_count:", post_end_count
    
    # remove the HDFS wrapper stuff
    region_lines[0] = re.sub(r'\(hdfs.*,', '', region_lines[0])
    region_lines[0] = re.sub(r'\(file.*,', '', region_lines[0])

    region_text = ''.join(region_lines)
    region_text = region_text[:-1] #stripping the trailing ")"

    ## This now happens on the Java side
    # remove the shading
    #region_text = re.sub(r'shadedgoogle.com.google', 'com.google', region_text)
    #region_text = re.sub(r'shadedgoogle.com.fasterxml', 'com.fasterxml', region_text)

    # write the text as a temp file
    with codecs.open(temp_in_file_path, 'w', 'utf-8') as TMP_FILE:
        TMP_FILE.write(region_text)
    in_file_path = temp_in_file_path

if args.out_file:
    out_file_path = args.out_file
else:
    out_file_path = temp_out_file_path
    
if args.debug:
    redirect = ''
else:
    redirect = ' >/dev/null 2>/dev/null'

# Used to specify a project (-pl pprint_hltcc) but that doesn't seem to be necessary
command = 'mvn exec:exec ' + \
          '-Dexec.executable="${JAVA_HOME}/bin/java" ' + \
          '-Dexec.args="-Xms24g -Xms6g -XX:NewRatio=3 -cp %classpath adept.e2e.utilities.PprintHltcc ' + \
          in_file_path + ' ' + out_file_path + '" ' + \
          '-Dexec.workingdir="$(readlink -f .)" ' + redirect

print "Running command:\n", command
os.system(command)

if args.out_file:
    pass
elif os.path.isfile(temp_out_file_path) and os.stat(temp_out_file_path).st_size > 0:
    #print "Printing output:"
    for line in codecs.open(temp_out_file_path, 'r', 'utf-8'):
        print line,
else:
    print "There was an error. Rerun with --debug to see what it was."

if not args.retain:
    os.remove(temp_in_file_path)
    os.remove(temp_out_file_path)
