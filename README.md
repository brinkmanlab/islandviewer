islandviewer
============

Perl library to run the IslandViwer backend.

# Installing

## Requirements

Perl modules:
- Array::Utils
- Bioperl
- Config::Simple
- Cwd
- Date::Manip
- DBI
- DBIx::Class::Core
- DBIx::Class::Schema
- Email::Valid
- Exporter
- Fcntl
- File::chdir
- File::Copy
- File::Copy::Recursive
- File::Path
- File::Spec
- File::Temp
- Getopt::Long
- IO::Select
- IO::Socket
- JSON
- List::Util
- Log::Log4perl
- Mail::Mailer
- MIME::Base64
- MIME::Base64::URLSafe
- Moose
- Moose::Util::TypeConstraints
- MooseX::Singleton
- Net::hostent
- Net::ZooKeeper::WatchdogQueue
- Scalar::Util
- Session::Token
- Set::IntervalTree
- Statistics::Descriptive
- Tie::RefHash
- URI::Escape
- utf8

## cvtree

A modified version of cvtree 4.2 is used which slightly changes the command line arguments.

```
126c126
< 	if (  (list==NULL || (idir==NULL && !parm.is_single_input) || k_len==0)
---
> 	if (  (list==NULL || k_len==0)
```

## MicrobeDB

Ensure MicrobeDB is loaded and the [MicrobeDB V2 API](https://github.com/lairdm/microbedbv2-perl) is available. The API defaults to the database name `microbedb`, this can be overridden using the environment variable `MicrobeDB`

  export MicrobeDB="microbedbv2

## Zookeeper

A Zookeeper server needs to be available for Islandviewer to use in coordinating jobs. Unfortunately the paths under Zookeeper aren't automatically recursively created, so you'll need to create the paths you configure for the backend on the Zookeeper server, something like:

  bin/zkCli.sh -server 127.0.0.1:7000
  create /islandviewer iv_jobs
  create /islandviewer/analysis iv_analysis

# Schedulers

There are a number of default schedulers available to submit jobs from the daemon. New schedulers can easily be written, they're simply perl modules that have a submit() parameters taking the name of the job, the command to execute, and the work directory to use, it's up to the scheduler module to submit this job to whereever appropriate. The submit() function should return 1 on success and 0 on failure.

In the Islandviewer configuration file the scheduler is set by simply giving the perl module.

  default_scheduler = Islandviewer::MetaScheduler

## Islandviewer::MetaScheduler

Scheduler to submit a job to [MetaScheduler](https://github.com/lairdm/metascheduler-ui), a proprietary scheduler used in the Brinkman Lab for managing complex pipelines.

## Islandviewer::Torque

Mainly used for the Distance module in update runs, but can also be used in place of Islandviewer::MetaScheduler to submit jobs directly to Torque via qsub.

## Islandviewer::NullScheduler

A simple fire-and-forget-it scheduler, takes the command given, and executes it as a detached system call. Always returns true.

## Islandviewer::DummyScheduler

Designed for testing the frontend or any other package that talks to a running backend. It doesn't actually execute the command given, it just prints the command given and returns 1.

# Updating pre-computed genomes

Once the prereqs are installed and configured running an update should be straight forward.

  bin/update_islandviewer.pl -c /data/Modules/iv-backend/islandviewer/etc/islandviewer.config --distance-only

There are a number of options for tweeking the run

--do-islandpick - If an update is being done and you want to have Islandviewer check if the available reference genomes may have changed, and rerun Islandpick if needed. This is for if a particular genome hasn't itself changed, but the genomes it might be compared against have changed through a MicrobeDB update.

-- skip-distance - The distance run can be intensive to initialize, if you interrupted a run and are restarting after the Distance step, this is a good way to skip straight to Islandviewer analysis.

--distance-only - Only run the Distance step for all genomes, useful for debugging or manual runs as the Distance step can be slow, sometimes you'll want to ensure it finished correctly before the actually Islandviewer runs are initialized.

--update-only - Don't look for new genomes in MicrobeDB, only scan existing records we've run through Islandviewer before, useful with --do-islandpick for restarting failed runs.

--distance-jobs - Defaults to 20, the number of pieces to split the Distance run in to, remember Distance is an all-against-all calculation.

--distance-scheduler - The default is to use the Islandviewer::Torque scheduler to submit jobs to Torque directly. This isn't always ideal, particularly when doing local development. And of the other Islandviewer schedulers should work for distance runs.

For example, if doing local development and you wanted to test the Distance phase only, and run the jobs linearly:

  bin/update_islandviewer.pl -c /data/Modules/iv-backend/islandviewer/etc/islandviewer.config --distance-only --distance-scheduler 'Islandviewer::NullScheduler' --distance-jobs 1
