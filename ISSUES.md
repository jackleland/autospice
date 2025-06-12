# Outstanding TODOs

The following TODO comments were found in the code base and should be tracked as issues:

- `autospice.py`
  - Line 139: Getting the autospice dir should be more rigorous
  - Line 202: The use of an input parser is SPICE specific
  - Line 207: Detecting dimensionality by parameter scan length could be improved
  - Line 299: Syntax change from example script needs addressing
  - Line 378-379: Slurm specific handling for Marconi time limits
  - Line 392: Expand to include n_jobs and param_scan_fl
  - Line 393: `api_json_filename` should be specified in a config option
  - Line 459: Verify string format
  - Line 480: Memory should be prioritised above maximising cpus per node

- `codes/spice.py`
  - Line 106: Replace parameter passing with kwargs or an object
  - Line 121: Read bash code from an external file
  - Line 156: Submission string could be modularised
  - Line 252: Consider moving logic to an input parser
  - Line 283: Validate `no_{section}` values

- `logger.py`
  - Line 140: Cast to required types before verification

- `utils.py`
  - Line 17: Git repo checking temporarily removed
  - Line 29-30: Check executable compiled since last commit and support other repos

These TODOs have been collected for future development and can be turned into tracked issues in your preferred issue tracker.

