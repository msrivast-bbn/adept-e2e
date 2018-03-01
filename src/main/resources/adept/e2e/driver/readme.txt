KBResolver parameters are stored in this package (not adept.e2e.kbresolver) because they are called by KBResolverSpark,
which is in the driver package, as one of the main functions of adept-e2e.

KBResolver requires two parameter files: one with the KB information (parliament, postgres, username/password, etc) and
a second with the parameters for the "filters" within KBResolver that are to be run. Here, the resolver parameters
are split into individual files for each filter. This is for clarity, as to what parameters each filter requires.

To run several filters, they may be listed in order in the "filterTypes" entry in the order they are to be run, and all
required parameters for those filters should be listed below (i.e., only one parameter file is needed). This is shown
in kbresolver_all_best_params.xml, which should be updated when new classes are implemented and tuning is complete.