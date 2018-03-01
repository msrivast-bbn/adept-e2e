#! /bin/env python

import argparse, os, sys
from shutil import copyfile

def do_command(step, command, debug):
    if debug:
        print '  ', step, 'Command:', command
    else:
        os.system(command)

description = '\n'.join((
    'Computes TAC EDL or SF scores given a KB of E2E output.',
    ' - Inputs the parameters for a KB of E2E output on the 2016_eval dev set',
    ' - Calls TacFormatConverter to produce a KB file (with -edl_only flag if scoring EDL)',
    ' - Does some minor fixes to that file',
    ' - Calls the validation script to produce an EDL or TAC/SF output file',
    ' - Calls the scorer (but this does not work yet for SF)',
    ' - Output "eval" file as well as working files including validation errors will be in the score_dir you supply'
    ))

parser = argparse.ArgumentParser(description = description,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('task',
                    help='EDL or SF or EAL')
parser.add_argument('scoring_language',
                    help='Which gold data to use for scoring: EN or CH or NONE. (NONE for a multi-language KB,'
                    ' or if you only want to validate and not score.)')
parser.add_argument('scoring_year',
                    help='Which year to use for the eval data file lengths file for validation')
parser.add_argument('KB_param_file',
                    help='XML file with parameters for accessing the system output KB'
                    'See examples in your DEFT repo: adept/adept-kb/src/main/resources/adept/kbapi/KBParaemeters*.xml')
parser.add_argument('score_dir',
                    help='a writeable dir for working files and results')
parser.add_argument('-r', '--run_name',
                    help='name for the run (no spaces). If no name is supplied, the score_dir will be used.')
parser.add_argument('-p', '--poster_language',
                    help='For fmt16 EDL runs, you must specify EN, CH, or BOTH for which DF files to get poster names from.\nFor 2017 eval runs, you must specify EN_2017, CH_2017,or BOTH_2017 to get poster names from the 2017 eval files.')
parser.add_argument('-o', '--old_2016_syntax',
                    help='use old, 2016 format when writing the output KB file',
                    action='store_true')
parser.add_argument('-f', '--filter_to_core',
                    help='filter output to just the core files annotated for EDL. This is automatic for EDL runs.',
                    action='store_true')
parser.add_argument('-b', '--bad_rel_args_filter',
                    help='Run filter_rel_args before validating to strip our relations with bad arg types',
                    action='store_true')
parser.add_argument('-d', '--debug',
                    help='print command lines but do not do anything',
                    action='store_true')
parser.add_argument('-s', '--skip_format_convert',
                    help='use existing TacFormatCoverter output file',
                    action='store_true')
parser.add_argument('-c', '--do_nil_clustering',
                    help='do nil clustering as part of TacFormatConverterRun',
                    action='store_true')
parser.add_argument('-w', '--whole_run_with_scoring',
                    help='whole run, do not just validate, also try to score',
                    action = 'store_true')

args = parser.parse_args()
if not args.run_name:
    args.run_name = os.path.basename(args.score_dir.rstrip('/'))

if not args.task in ('EDL', 'SF', 'EAL'):
    raise Exception("first parameter, task, must be EDL, SF, or EAL.")

if args.task in ['EDL', 'EAL'] and args.old_2016_syntax:
    raise Exception("No need to use the old 2016 syntax for EDL or EAL scoring, so that path is not supported.")

if not args.scoring_language in ['EN', 'CH', 'NONE']:
    raise Exception("Scoring language argument must be EN, CH, or NONE.")

if not args.scoring_year in ('2016', '2017'):
    raise Exception("Scoring year must be 2016 or 2017")

if args.poster_language and args.poster_language not in ['EN', 'CH', 'BOTH', 'EN_2017', 'CH_2017', 'BOTH_2017']:
    raise Exception("The poster_language arg, if specifed, must be EN, CH, or BOTH, or EN_2017, CH_2017, BOTH_2017.")

if args.task == 'EDL' and not args.poster_language:
    raise Exception("For EDL scoring runs, you must specify poster_language (-p) as EN, CH. or BOTH.")

score_tac_script_path = os.path.realpath(__file__)
# get to the repo dir by stripping off the finel "src/main/python/score_tac.py"
e2e_repo_dir = score_tac_script_path[:-28]
scoring_scripts_dir = score_tac_script_path[:-12]
#print "e2e_repo_dir:", e2e_repo_dir
#print "scoring_scripts_dir:", scoring_scripts_dir
resources_dir = os.path.join(e2e_repo_dir, 'src', 'main', 'resources', 'adept', 'utilities')

### Scoring code directories ###
coldstart_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/scoring/2017/ColdStart-2017.06/'
coldstart_dir_2016 = '/nfs/mercury-04/u60/DEFT/users/lramshaw/scoring/2016/official_scorer/ColdStart/'
neleval_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/scoring/2017/neleval/'

### 2016 English EDL ###
#scoring_2016_gold = '/nfs/mercury-04/u60/DEFT/users/lramshaw/scoring/2016/neleval/bmin.gold.combined.tsv'
#scoring_2016_gold = '/nfs/mercury-04/u14/ldc_releases/LDC2016E68_TAC_KBP_2016_Entity_Discovery_and_Linking_Evaluation_Gold_Standard_Entity_Mentions_and_Knowledge_Base_Links/data/tac_kbp_2016_edl_evaluation_gold_standard_entity_mentions.tab'
scoring_2016_eng_gold = '/nfs/mercury-04/u60/DEFT/users/lramshaw/scoring/resources/2016/english.gold.tab'
eng_full_2016_eval_docs_dir = None
eng_sf_2016_eval_docs_dir = None  # used in any way in SF
eng_df_2016_core_docs_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata/eng/dev01_core_df/'
eng_nw_2016_core_docs_dir = '/nfs/mercury-04/u60/DEFT/users/msrivast/devdata/eng/dev01_core_nw/'

### 2016 Chinese EDL ###
scoring_2016_chi_gold = '/nfs/mercury-04/u60/DEFT/users/lramshaw/scoring/resources/2016/chinese.gold.tab'
chi_full_2016_eval_docs_dir = None
chi_sf_2016_eval_docs_dir = '/nfs/mercury-04/u60/DEFT/users/msrivast/chinese_core/' # used in any way in SF
chi_df_2016_core_docs_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata/chi/core_edl_df/'
chi_nw_2016_core_docs_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata/chi/core_edl_nw/'

### 2016 English SF ###
query_file_2016 = '/nfs/mercury-04/u42/bmin/projects/coldstart/coldstart2016/official_results/BBN.KBP2016_CSSF_scores/aux_files/tac_kbp_2016_english_cold_start_slot_filling_evaluation_queries.xml'
assessment_file_2016 = '/nfs/mercury-04/u42/bmin/projects/coldstart/coldstart2016/official_results/BBN.KBP2016_CSSF_scores/aux_files/batch_03.cssf.assessed.fqec.eng'
query_ids_2016 = '/nfs/mercury-04/u42/bmin/projects/coldstart/coldstart2016/official_results/BBN.KBP2016_CSSF_scores/aux_files/batch_03.queryids.eng'

### 2017 Eval data
eng_df_2017_docs_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/evaldata/LDC2017E25_TAC_KBP_2017_Evaluation_Source_Corpus_V1.1/data/eng/df/'
eng_nw_2017_docs_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/evaldata/LDC2017E25_TAC_KBP_2017_Evaluation_Source_Corpus_V1.1/data/eng/nw/'
chi_df_2017_docs_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/evaldata/LDC2017E25_TAC_KBP_2017_Evaluation_Source_Corpus_V1.1/data/cmn/df/'
chi_nw_2017_docs_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/evaldata/LDC2017E25_TAC_KBP_2017_Evaluation_Source_Corpus_V1.1/data/cmn/nw/'

### Eval data file lengths file ###
eval_doc_lengths_2016 = '/nfs/mercury-04/u42/bmin/projects/coldstart/coldstart2016/submission/LDC2016E63_TAC_KBP_2016_Evaluation_Source_Corpus_V1.1/data/character_counts.tsv'
eval_doc_lengths_2017 = '/nfs/mercury-04/u60/DEFT/users/lramshaw/evaldata/LDC2017E25_TAC_KBP_2017_Evaluation_Source_Corpus_V1.1/docs/character_counts.tsv'
if args.scoring_year == '2016':
    eval_doc_lengths_file = eval_doc_lengths_2016
else:
    eval_doc_lengths_file = eval_doc_lengths_2017

eval_in_dir = os.path.join(args.score_dir, 'eval_in')
if not (os.path.isdir(eval_in_dir)):
    print "Making dir", eval_in_dir
    os.makedirs(eval_in_dir) 
do_command('DELETE_EVAL_DIRS','rm -rf '+eval_in_dir+'/*',args.debug)

eval_out_dir = os.path.join(args.score_dir, 'eval_out')
if not (os.path.isdir(eval_out_dir)):
    print "Making dir", eval_out_dir
    os.makedirs(eval_out_dir)
do_command('DELETE_EVAL_DIRS','rm -rf '+eval_out_dir+'/*',args.debug)

print "run_name:", args.run_name
    
print "Beginning scoring...."

###############################################
# Run TacFormatConverter
###############################################

if args.skip_format_convert:
    print "Skipping TacFormatConverter"
else:
    print "Running TacFormatConverter..."

    command_repo_dir_cd = 'cd ' + e2e_repo_dir 
    command_converter = ' '.join(('mvn exec:java -Dexec.mainClass="adept.e2e.utilities.TacFormatConverter"',
				'-Dexec.args=\"'+args.score_dir,
                                args.run_name,
                                '--params',
                                args.KB_param_file,))
    if args.task == 'EDL':
        command_converter += ' -edl_only'
    elif args.task == 'EAL': # or args.task == 'SF':
        command_converter += ' -events'
    if args.old_2016_syntax:
        command_converter += ' -old'

    if args.do_nil_clustering:
        command_converter += ' -nil_clustering'
    # This used to depend on -e convert_events flag, but we're making it always happen    
    command_converter += ' -events'
        
    command_converter+='\"'
    combined_command = command_repo_dir_cd + '; ' + command_converter
    print combined_command

    do_command('TacFormatConverter', combined_command, args.debug)
    # check that we actually got a KB file
    if not (os.path.isfile(os.path.join(args.score_dir, args.run_name, args.run_name + '.ColdStart.tsv')) or
            os.path.isfile(os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'))):
        raise Exception('No TacFormatConverter output file was written. Quitting.')

    # If the output file is still named runname.ColdStart.tsv, copy it as "runname.kb"
    if os.path.isfile(os.path.join(args.score_dir, args.run_name, args.run_name + '.ColdStart.tsv')):
        copyfile(os.path.join(args.score_dir, args.run_name, args.run_name + '.ColdStart.tsv'),
                 os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'))

    input_dirs=[eng_df_2016_core_docs_dir,eng_nw_2016_core_docs_dir]
    kb_file_suffix='preengposternames'
    mode='English (scoring)'
    if args.poster_language=='CH':
        input_dirs=[chi_df_2016_core_docs_dir,chi_nw_2016_core_docs_dir]
        kb_file_suffix='prechiposternames'
        mode='Chinese (scoring)'
    elif args.poster_language=='BOTH':
        input_dirs=[eng_df_2016_core_docs_dir,eng_nw_2016_core_docs_dir,chi_df_2016_core_docs_dir,chi_nw_2016_core_docs_dir]
        kb_file_suffix='preposternames'
        mode='Bilingual (scoring)'
    elif args.poster_language=='EN_2017':
        input_dirs=[eng_df_2017_docs_dir,eng_nw_2017_docs_dir]
        kb_file_suffix='preengposternames'
        mode='English (2017 Eval)'
    elif args.poster_language=='CH_2017':
        input_dirs=[chi_df_2017_docs_dir,chi_nw_2017_docs_dir]
        kb_file_suffix='prechiposternames'
        mode='Chinese (2017 Eval)'
    elif args.poster_language=='BOTH_2017':
        input_dirs=[eng_df_2017_docs_dir,eng_nw_2017_docs_dir,chi_df_2017_docs_dir,chi_nw_2017_docs_dir]
        kb_file_suffix='preposternames'
        mode='Bilingual (2017 Eval)'
    if args.poster_language:
        print "Adding "+mode+" poster mentions..."
        os.rename(os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
                  os.path.join(args.score_dir, args.run_name, args.run_name + '.kb.'+kb_file_suffix))
        command_add_poster_mentions = ' '.join(('mvn exec:java -Dexec.mainClass="adept.e2e.utilities.GeneratePosterMentions"',
                                                '-Dexec.args=\"'+os.path.join(args.score_dir, args.run_name, args.run_name+'.kb.'+kb_file_suffix),
                                                os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
                                                " ".join(input_dirs),
                                            ))
        command_add_poster_mentions += '\"'
        combined_command = command_repo_dir_cd + '; ' + command_add_poster_mentions
        do_command('GeneratePosterMentions', combined_command, args.debug)
        if not os.path.isfile(os.path.join(args.score_dir, args.run_name, args.run_name + '.kb')):
            raise Exception('GeneratePosterMentions did not generate a .kb file. Quitting.')

        
###############################################
# Do minor fixes on the KB text file
###############################################

if (args.task == 'EDL' or args.filter_to_core) and (args.scoring_language in ('EN', 'CH')):
    # Filter the output down to the files in the EDL subset
    print "Filtering down to files in the core EDL-annotated subset..."
    os.rename(os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
              os.path.join(args.score_dir, args.run_name, args.run_name + '.unfiltered.kb'))
    filter_command = ' '.join((os.path.join(scoring_scripts_dir, 'filter_core_files.py'),
                               args.scoring_language,
                               os.path.join(args.score_dir, args.run_name, args.run_name + '.unfiltered.kb'),
                               os.path.join(args.score_dir, args.run_name,  args.run_name + '.kb'),
                               ))
    do_command('Filter', filter_command, args.debug)

# Remove duplicate lines, if any
### This used to be necessary when we were outputting combined.tsv files, but I don't think we need it now.
### If we do need it, it will have to get fancier, since we'll still want all relation lines to follow all entity lines
if False:
    print "Removing any duplicates..."
    dedup_command = 'sort ' + os.path.join(args.score_dir, args.run_name, args.run_name + '.rejects.tsv') + ' | uniq > ' + os.path.join(args.score_dir, args.run_name, args.run_name + '.uniq.tsv')
    do_command('Dedup', dedup_command, args.debug)

# If requested, run filter_bad_args.py to filter out relations with bad argument types.
if args.bad_rel_args_filter:
    print "Replacing NextLine chars..."
    os.rename(os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
              os.path.join(args.score_dir, args.run_name, args.run_name + '.prenextline.kb'))
    replace_nextline_command = ' '.join((os.path.join(scoring_scripts_dir, 'replace_nextline_chars.py'),
                                         os.path.join(args.score_dir, args.run_name, args.run_name + '.prenextline.kb'),
                                         os.path.join(args.score_dir, args.run_name, args.run_name + '.kb')))
    do_command('Replacing_nextline_chars', replace_nextline_command, args.debug)
    print "Running filter_rel_arg to remove relations with bad arg types..."
    os.rename(os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
              os.path.join(args.score_dir, args.run_name, args.run_name + '.prerelargs.kb'))
    rel_args_command = ' '.join((os.path.join(scoring_scripts_dir, 'filter_rel_args.py'),
                                 os.path.join(args.score_dir, args.run_name, args.run_name + '.prerelargs.kb'),
                                 os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
                                 os.path.join(args.score_dir, args.run_name, args.run_name + '.rejectedrels'),
                                 os.path.join(resources_dir, 'rel_arg_types.tsv')))
    do_command('Running_filter_rel_args', rel_args_command, args.debug)

#######################################################################
# Run validate over the KB file to get scoreable EDL or SF ("tac") file
#######################################################################

print "Running validate to get scoreable EDL, SF, or EAL format files..."

if args.task == 'EDL':
    task_arg = '-output edl'
elif args.task == 'SF':
    task_arg = '-output tac'
elif args.task == 'EAL':
    task_arg = '-output eal'

if args.old_2016_syntax:
    # using 2016 syntax
    validate_command = ' '.join(('/opt/perl-5.14.1-x86_64/bin/perl',
                                 (os.path.join(coldstart_dir_2016, 'CS-ValidateKB-MASTER.pl')),
                                 '-docs',
                                 eval_doc_lengths_file,
                                 '-task CSKB',
                                 task_arg,
                                 '-error_file',
                                 (os.path.join(args.score_dir, 'errors')),
                                 '-output_file',
                                 os.path.join(args.score_dir, args.run_name, args.run_name + '.kb.valid'),
                                 os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
                                 ))
    coldstart_cd_command = 'cd ' + coldstart_dir_2016
else:
    # using 2017 syntax
    validate_command = ' '.join(('/opt/perl-5.14.1-x86_64/bin/perl',
                                 (os.path.join(coldstart_dir, 'CS-ValidateKB-MASTER.pl')),
                                 '-docs',
                                 eval_doc_lengths_file,
                                 task_arg,
                                 '-output_dir',
                                 eval_in_dir,
                                 '-error_file',
                                 (os.path.join(args.score_dir, 'errors')),
                                 os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
                                 ))
    coldstart_cd_command = 'cd ' + coldstart_dir
    
validate_full_command = coldstart_cd_command + '; ' + validate_command

do_command('Validate', validate_full_command, args.debug)

# Quit if we're not going to do scoring
if args.whole_run_with_scoring:
    print "Continuing to try to do scoring"
else:
    print 'Stopping after validation.'
    sys.exit(-1)                    
                    
###################################################
# Bail out if validation has errors.
# User can fix the KB file by hand and rerun with -s
####################################################

# remove old error flag file, if one exists
error_flag_file_path = os.path.join(args.score_dir, 'error_count')
if os.path.exists(error_flag_file_path):
    os.remove(error_flag_file_path)

flag_errors_command = ' '.join((os.path.join(scoring_scripts_dir, 'flag_errors.py'),
                                args.score_dir))
do_command('Count and flag errors if any', flag_errors_command, args.debug)

if os.path.isfile(os.path.join(args.score_dir, 'error_count')):
    print('The validator found errors in the KB file, preventing scoring.')
    print('If you want, you can fix the KB file by hand,')
    print('  and then rerun score_tac.py with the -s flag.')
    sys.exit(-1)

###############################################
# Run the scorer
###############################################

if args.task=='EDL':
    if args.scoring_language == 'EN':
        gold_file = scoring_2016_eng_gold
    elif args.scoring_language == 'CH':
        gold_file = scoring_2016_chi_gold
    else:
        print "Not scoring, since scoring_language is neither EN nor CH"
        sys.exit(-1)
    print "Scoring EDL..."
    score_command_cd = 'cd ' + neleval_dir
    score_command_score = ' '.join(('scripts/run_tac16_evaluation.sh',
                                    gold_file,
                                    #os.path.join(args.score_dir, args.run_name + '.combined.tsv'),  ## Should be the EDL file from validation
                                    eval_in_dir,  # SYSTEMS_DIR, where to find the files
                                    eval_out_dir,  # OUT_DIR, where to write the results
                                    '1' # number of processors
                                    ))
    score_command = score_command_cd + '; ' + score_command_score
    do_command('Score', score_command, args.debug)
    add_strings_output_command = ' '.join((os.path.join(scoring_scripts_dir, 'add_strings.py'),
                                           args.scoring_language,
                                           os.path.join(args.score_dir, 'eval_out', args.run_name + '.kb.edl.valid.combined.tsv'),
                                           os.path.join(args.score_dir, 'eval_out', args.run_name + '.kb.edl.valid.combined.tsv.strings'),
                                       ))
    print "Adding strings to the output..."
    do_command('Adding strings output', add_strings_output_command, args.debug)
    add_strings_gold_command = ' '.join((os.path.join(scoring_scripts_dir, 'add_strings.py'),
                                         args.scoring_language,
                                         os.path.join(args.score_dir, 'eval_out','gold.combined.tsv'),
                                         os.path.join(args.score_dir, 'eval_out','gold.combined.tsv.strings'),
                                         ))
    print "Adding strings to the gold file..."
    do_command('Adding strings gold', add_strings_gold_command, args.debug)
    analysis_command_analysis = ' '.join(('scripts/run_analysis_log.sh',
                                          os.path.join(args.score_dir, 'eval_out', 'gold.combined.tsv'),
                                          os.path.join(args.score_dir, 'eval_out', args.run_name + '.kb.edl.valid.combined.tsv'),
                                      ))
    analysis_command = score_command_cd + '; ' + analysis_command_analysis
    print "Running the analysis..."
    do_command('Running analysis', analysis_command, args.debug)
    add_string_log_command = ' '.join((os.path.join(scoring_scripts_dir, 'add_log_strings.py'),
                                       args.scoring_language,
                                       os.path.join(args.score_dir, 'eval_out', args.run_name + '.kb.edl.valid.analysis'),
                                       os.path.join(args.score_dir, 'eval_out', args.run_name + '.kb.edl.valid.analysis.strings'),
                                       ))
    print "Adding strings to the analysis file..."
    do_command('Add strings to analysis', add_string_log_command, args.debug)
                                
    
elif args.task=='SF':
    if args.old_2016_syntax:
        print "Scoring SF using the old, 2016 formats"
    else:
        print "Sorry, but can't score SF using the new, 2017 format. Bailing out."
        sys.exit(-1)

    print "  Resolving queries..."
    resolve_query_command = ' '.join(('/opt/perl-5.14.1-x86_64/bin/perl',
                                      os.path.join(coldstart_dir_2016, 'CS-ResolveQueries-MASTER.pl'),
                                      '-error_file',
                                      os.path.join(args.score_dir, 'eval_in', 'resolve_queries_errors'),
                                      query_file_2016,
                                      os.path.join(args.score_dir, args.run_name, args.run_name + '.kb.valid'),
                                      os.path.join(args.score_dir, 'eval_in', args.run_name + '.sf'),
                                  ))
    do_command('ResolveQuery', coldstart_cd_command + '; ' + resolve_query_command, args.debug)

    print "  Validating the SF file..."
    validate_sf_command = ' '.join(('/opt/perl-5.14.1-x86_64/bin/perl',
                                    os.path.join(coldstart_dir_2016, 'CS-ValidateSF-MASTER.pl'),
                                    '-docs /nfs/mercury-04/u42/bmin/projects/coldstart/coldstart2016/submission/LDC2016E63_TAC_KBP_2016_Evaluation_Source_Corpus_V1.1/data/character_counts.tsv',                                    
                                    '-error_file',
                                    os.path.join(args.score_dir, 'eval_in', 'validate_sf_errors'),
                                    '-output_file',
                                    os.path.join(args.score_dir, 'eval_in', args.run_name + '.sf.valid'),
                                    query_file_2016,
                                    os.path.join(args.score_dir, 'eval_in', args.run_name + '.sf'),
                                ))
    do_command('ValidateSF', coldstart_cd_command + '; ' + validate_sf_command, args.debug)

    print "  Scoring..."
    score_command = ' '.join(('/opt/perl-5.14.1-x86_64/bin/perl',
                              os.path.join(coldstart_dir_2016, 'CS-Score-MASTER.pl'),
                              '-discipline STRING_CASE',
                              '-output_file',
                              os.path.join(args.score_dir, 'eval_out', args.run_name + '.tab.txt.string_case'),
                              '-queries',
                              query_ids_2016,
                              '-error_file',
                              os.path.join(args.score_dir, 'eval_out', 'scorer_errors'),
                              query_file_2016,
                              os.path.join(args.score_dir, 'eval_in', args.run_name + '.sf.valid'),
                              assessment_file_2016,
                              ))
    do_command('ScoringSF', coldstart_cd_command + '; ' + score_command, args.debug)                             

print "Done scoring."
