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

## Islandviewer::NullScheduler

A simple fire-and-forget-it scheduler, takes the command given, and executes it as a detached system call. Always returns true.

## Islandviewer::DummyScheduler

Designed for testing the frontend or any other package that talks to a running backend. It doesn't actually execute the command given, it just prints the command given and returns 1.
