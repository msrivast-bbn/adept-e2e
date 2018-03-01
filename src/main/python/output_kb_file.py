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
parser.add_argument('KB_param_file',
                    help='XML file with parameters for accessing the system output KB'
                    'See examples in your DEFT repo: adept/adept-kb/src/main/resources/adept/kbapi/KBParaemeters*.xml')
parser.add_argument('score_dir',
                    help='a writeable dir for working files and results')
parser.add_argument('-r', '--run_name',
                    help='name for the run (no spaces). If no name is supplied, the score_dir will be used.')
parser.add_argument('-p', '--poster_language',
                    help='For EDL runs, you must specify EN, CH, or BOTH for which DF files to get poster names from.')
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

args = parser.parse_args()
if not args.run_name:
    args.run_name = os.path.basename(args.score_dir.rstrip('/'))

if not args.task in ('EDL', 'SF', 'EAL'):
    raise Exception("first parameter, task, must be EDL, SF, or EAL.")

if args.task in ['EDL', 'EAL'] and args.old_2016_syntax:
    raise Exception("No need to use the old 2016 syntax for EDL or EAL scoring, so that path is not supported.")

if args.poster_language and args.poster_language not in ['EN', 'CH', 'BOTH']:
    raise Exception("The poster_language arg, if specifed, must be EN, CH, or BOTH.")

score_tac_script_path = os.path.realpath(__file__)
# get to the repo dir by stripping off the final "src/main/python/score_tac.py"
e2e_repo_dir = score_tac_script_path[:-33]
scoring_scripts_dir = score_tac_script_path[:-17]
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
eng_all_2016_core_docs_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata/eng/dev01_core/'

### 2016 Chinese EDL ###
scoring_2016_chi_gold = '/nfs/mercury-04/u60/DEFT/users/lramshaw/scoring/resources/2016/chinese.gold.tab'
chi_full_2016_eval_docs_dir = None
chi_sf_2016_eval_docs_dir = '/nfs/mercury-04/u60/DEFT/users/msrivast/chinese_core/' # used in any way in SF
chi_df_2016_core_docs_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata/chi/core_edl_df/'
chi_all_2016_core_docs_dir = '/nfs/mercury-04/u60/DEFT/users/lramshaw/devdata/chi/core_edl/'

### 2016 English SF ###
query_file_2016 = '/nfs/mercury-04/u42/bmin/projects/coldstart/coldstart2016/official_results/BBN.KBP2016_CSSF_scores/aux_files/tac_kbp_2016_english_cold_start_slot_filling_evaluation_queries.xml'
assessment_file_2016 = '/nfs/mercury-04/u42/bmin/projects/coldstart/coldstart2016/official_results/BBN.KBP2016_CSSF_scores/aux_files/batch_03.cssf.assessed.fqec.eng'
query_ids_2016 = '/nfs/mercury-04/u42/bmin/projects/coldstart/coldstart2016/official_results/BBN.KBP2016_CSSF_scores/aux_files/batch_03.queryids.eng'

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
    
print "Beginning output of KB file...."

###############################################
# Run TacFormatConverter
###############################################

if args.skip_format_convert:
    print "Skipping TacFormatCoverter"
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

    command_converter+='\"'
    combined_command = command_repo_dir_cd + '; ' + command_converter

    do_command('TacFormatConverter', combined_command, args.debug)
    # check that we actually got a KB file
    if not (os.path.isfile(os.path.join(args.score_dir, args.run_name, args.run_name + '.ColdStart.tsv')) or
            os.path.isfile(os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'))):
        raise Exception('No TacFormatConverter output file was written. Quitting.')

    # If the output file is still named runname.ColdStart.tsv, copy it as "runname.kb"
    if os.path.isfile(os.path.join(args.score_dir, args.run_name, args.run_name + '.ColdStart.tsv')):
        copyfile(os.path.join(args.score_dir, args.run_name, args.run_name + '.ColdStart.tsv'),
                 os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'))

    ### This needs to change before the Eval!
    ### For now, we're adding poster names for the DF files in the core EDL annotated set for the given language(s),
    ### but in the real runs, we'll need to process all the DF files that E2E processed.
    print "Adding poster mentions..."
    if args.poster_language in ('EN', 'BOTH'):
        print "Adding English poster mentions..."
        os.rename(os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
                  os.path.join(args.score_dir, args.run_name, args.run_name + '.kb.preengposternames'))
        command_add_poster_mentions = ' '.join(('mvn exec:java -Dexec.mainClass="adept.e2e.utilities.GeneratePosterMentions"',
                                                '-Dexec.args=\"'+os.path.join(args.score_dir, args.run_name, args.run_name + '.kb.preengposternames'),
                                                eng_df_2016_core_docs_dir,
                                                os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
                                            ))
        command_add_poster_mentions += '\"'
        combined_command = command_repo_dir_cd + '; ' + command_add_poster_mentions
        do_command('GeneratePosterMentions', combined_command, args.debug)
        if not os.path.isfile(os.path.join(args.score_dir, args.run_name, args.run_name + '.kb')):
            raise Exception('GeneratePosterMentions did not generate a .kb file. Quitting.')

    if args.poster_language in ('CH', 'BOTH'):
        print "Adding Chinese poster mentions..."
        os.rename(os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
                  os.path.join(args.score_dir, args.run_name, args.run_name + '.kb.prechiposternames'))
        command_add_poster_mentions = ' '.join(('mvn exec:java -Dexec.mainClass="adept.e2e.utilities.GeneratePosterMentions"',
                                                '-Dexec.args=\"'+os.path.join(args.score_dir, args.run_name, args.run_name + '.kb.prechiposternames'),
                                                chi_df_2016_core_docs_dir,
                                                os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
                                            ))
        command_add_poster_mentions += '\"'
        combined_command = command_repo_dir_cd + '; ' + command_add_poster_mentions
        do_command('GeneratePosterMentions', combined_command, args.debug)
        if not os.path.isfile(os.path.join(args.score_dir, args.run_name, args.run_name + '.kb')):
            raise Exception('GeneratePosterMentions did not generate a .kb file. Quitting.')

###############################################
# Do minor fixes on the KB text file
###############################################

# Remove duplicate lines, if any
### This used to be necessary when we were outputting combined.tsv files, but I don't think we need it now.
### If we do need it, it will have to get fancier, since we'll still want all relation lines to follow all entity lines
if False:
    print "Removing any duplicates..."
    dedup_command = 'sort ' + os.path.join(args.score_dir, args.run_name, args.run_name + '.rejects.tsv') + ' | uniq > ' + os.path.join(args.score_dir, args.run_name, args.run_name + '.uniq.tsv')
    do_command('Dedup', dedup_command, args.debug)

# If requested, run filter_bad_args.py to filter out relations with bad argument types.
if args.bad_rel_args_filter:
    print "Running filter_rel_arg to remove relations with bad arg types..."
    os.rename(os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
              os.path.join(args.score_dir, args.run_name, args.run_name + '.prerelargs.kb'))
    rel_args_command = ' '.join((os.path.join(scoring_scripts_dir, 'filter_rel_args.py'),
                                 os.path.join(args.score_dir, args.run_name, args.run_name + '.prerelargs.kb'),
                                 os.path.join(args.score_dir, args.run_name, args.run_name + '.kb'),
                                 os.path.join(args.score_dir, args.run_name, args.run_name + '.rejectedrels'),
                                 os.path.join(resources_dir, 'rel_arg_types.tsv')))
    do_command('Running_filter_rel_args', rel_args_command, args.debug)

print "Done with output KB file."
