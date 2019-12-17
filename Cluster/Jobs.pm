package Engine::Cluster::Jobs;
use Moose::Role;
use Method::Signatures::Simple;

=head2

	PACKAGE		Engine::Cluster::Jobs
	
	PURPOSE
	
		RUN, MONITOR AND AUDIT CLUSTER JOBS
			
=cut

# Int
# has 'submit'		=> ( isa => 'Int|Undef', is => 'rw', default => 0 );
has 'starttime' 	=> ( isa => 'Int|Undef', is => 'rw', default => sub { time() });

# String
#has 'clustertype'=> ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'cluster'			=> ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'qsuboptions'	=> ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'walltime'		=> ( isa => 'Str|Undef', is => 'rw', default => 24 );
has 'cpus'				=> ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'qstat'				=> ( isa => 'Str|Undef', is => 'rw', default => '' );
# has 'qsub'		=> ( isa => 'Str|Undef', is => 'rw', default => '' );
# has 'maxjobs'	=> ( isa => 'Str|Undef', is => 'rw'	);
has 'sleep'				=> ( isa => 'Str|Undef', is => 'rw', default => 10 );
has 'cleanup'			=> ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'dot'					=> ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'verbose'			=> ( isa => 'Str|Undef', is => 'rw', default => '' );

# Hash/Array
# has 'envarsub'	=> ( isa => 'Maybe', is => 'rw', lazy => 1, builder => "setEnvarsub" );
has 'customvars'	=>	( isa => 'HashRef', is => 'rw', default => sub {
	return {
		cluster 			=> 	"CLUSTER",
		qmasterport 	=> 	"SGE_MASTER_PORT",
		execdport 		=> 	"SGE_EXECD_PORT",
		sgecell 			=> 	"SGE_CELL",
		sgeroot 			=> 	"SGE_ROOT",
		queue 				=> 	"QUEUE"
	};
});

# 	Objects
has 'batchstats'=> ( isa => 'ArrayRef|Undef', is => 'rw', default => sub { [] });
has 'command'	=> ( isa => 'ArrayRef|Undef', is => 'rw' );
# has 'conf'		=> ( isa => 'Conf::Yaml|Undef', is => 'rw' );
has 'monitor'	=> 	( isa => 'Maybe|Undef', is => 'rw', required => 0 );

# has 'envar'	=> ( 
# 	is => 'rw',
# 	isa => 'Envar',
# 	lazy => 1,
# 	builder => "setEnvar" 
# );

# method setEnvar {
# 	my $customvars	=	$self->can("customvars") ? $self->customvars() : undef;
# 	my $envarsub	=	$self->can("envarsub") ? $self->envarsub() : undef;
# 	$self->logDebug("customvars", $customvars);
# 	$self->logDebug("envarsub", $envarsub);
	
# 	my $envar = Envar->new({
# 		db			=>	$self->table()->db(),
# 		conf		=>	$self->conf(),
# 		customvars	=>	$customvars,
# 		envarsub	=>	$envarsub,
# 		parent		=>	$self
# 	});
	
# 	$self->envar($envar);
# }




use strict;
use warnings;
use Carp;

#### EXTERNAL MODULES
use Data::Dumper;
use File::Path;

method getIndex {
=head2

	SUBROUTINE		getIndex
	
	PURPOSE
	
		RETURN THE PSEUDO-ENVIRONMENT VARIABLE USED BY THE CLUSTER
		
		TO REPRESENT THE ARRAY JOB ID

=cut

	return '\$TASKNUM';

#	my $cluster = $self->cluster();	
#	return '\$LSB_JOBINDEX' if $cluster eq "LSF";
#	return '\$PBS_TASKNUM' if $cluster eq "PBS";
#    return '\$SGE_TASK_ID' if $cluster eq "SGE";
#	return;
}

method moveToDir ( $sourcedir, $targetdir, $label ) {
=head2

	SUBROUTINE		moveToDir
	
	PURPOSE
	
		1. MOVE THE CONTENTS OF A DIRECTORY TO ANOTHER DIRECTORY
		
		2. RUN ON THE MASTER NODE IF 'cluster' NOT SPECIFIED
			
			(E.G., BECAUSE TARGET DIRECTORY IS INVISIBLE TO
			
			CLUSTER EXECUTION HOSTS)
			
	INPUTS
	
		1. SOURCE DIRECTORY (MUST EXIST AND CANNOT BE A FILE)
		
		2. TARGET DIRECTORY (WILL BE CREATED IF DOES NOT EXIST)
		
		3. LABEL (IF SPECIFIED, JOB WILL BE SUBMITTED TO QUEUE)
		
	OUTPUTS
	
		1. ALL FILES IN SOURCE DIRECTORY WILL BE MOVED TO A
		
			NEWLY-CREATED TARGET DIRECTORY
			
=cut

	$self->logDebug("sourcedir", $sourcedir);
	$self->logDebug("targetdir", $targetdir);
	$self->logDebug("label", $label)  if defined $label;

	#### CHECK FOR SOURCE DIR
	$self->logDebug("sourcedir is a file", $sourcedir) and return if -f $sourcedir;
	$self->logDebug("targetdir is a file", $targetdir) and return if -f $targetdir;
	$self->logDebug("Can't find sourcedir", $sourcedir) and return if not -d $sourcedir;

	#### CREATE TARGET DIR IF NOT EXISTS
	File::Path::mkpath($targetdir) if not -d $targetdir;
	$self->logDebug("Skipping move because can't create targetdir", $targetdir) if not -d $targetdir;

	my $command = "mv $sourcedir/* $targetdir";
	
	if ( not defined $label )
	{
		$self->logDebug("command", $command);
		print `$command`;
	}
	else
	{
		#### SET JOB
		my $job = $self->setJob( [$command], $label, $targetdir);
		$self->runJobs( [$job], $label);
	}
}

method setMonitor {
	return $self->monitor() if $self->monitor();

	my $clustertype =  $self->conf()->getKey("core:CLUSTERTYPE");
	my $classfile = "Agua/Monitor/" . uc($clustertype) . ".pm";
	my $module = "Agua::Monitor::$clustertype";
	$self->logDebug("clustertype", $clustertype);
	$self->logDebug("classfile", $classfile);
	$self->logDebug("module", $module);

	$self->logDebug("Doing require $classfile");
	require $classfile;

	$self->logDebug("username: " . $self->username());
	$self->logDebug("cluster: " . $self->cluster());
	my $monitor = $module->new(
		{
			'pid'		=>	$$,
			'conf' 		=>	$self->conf(),
			username	=>	$self->username(),
			cluster		=>	$self->cluster()
			#,
			#'db'		=>	$self->table()->db()
		}
	);
	$self->monitor($monitor);

	return $monitor;
}

method runJobs ( $jobs, $label ) {
=head2

	SUBROUTINE		runJobs
	
	PURPOSE
	
		RUN A LIST OF JOBS CONCURRENTLY UP TO A MAX NUMBER
		
		OF CONCURRENT JOBS

=cut

	#### SET SLEEP
	my $sleep	= $self->sleep();
	$sleep = 5 if not defined $sleep;

	#### GET OUTPUTDIR
	my $outputdir = $self->outputdir();
	$self->logCritical("jobs not defined") and exit if not defined $jobs;
	$self->logCritical("label not defined") and exit if not defined $label;
	$self->logDebug("XXXXX no. jobs: " . scalar(@$jobs));
	my $jobids = [];
	my $scriptfiles = [];

	#### SET CURRENT TIME (START TIMER)
	my $current_time =  time();

	### EXECUTE JOBS	
	$self->execute($jobs, $label);
	
=head2

	#### CHECK JOBS HAVE COMPLETED
	my $status = "completed";
	my $sublabels 	= '';
	#my $maxchecks = maxchecks();
	#$maxchecks = 3 if not defined $maxchecks;
	my $counter = 0;
	#while ( not $status and $counter < $maxchecks )
	#{
		$self->logDebug("doing check $counter with checkStatus(jobs, label)");
		$counter++;
		($status, $sublabels) = $self->checkStatus($jobs, $label);
		$self->logDebug("status", $status);
		$self->logDebug("Sleeping for 10 seconds") if not $status;
	#	sleep(10) if not $status;
	#}
	$self->logDebug("Final value of status", $status);

	#### SEND JOB COMPLETION SIGNAL
	$self->logWarning("\n------------------------------------------------------------");
	$self->logWarning("---[status $label: $status $sublabels]---");
	$self->logWarning("\n------------------------------------------------------------");
	print "\n------------------------------------------------------------";
	print "---[status $label: $status $sublabels]---";
	print "\n------------------------------------------------------------";

	
	#### PRINT CHECKFILES
	my $checkfiles = $self->printCheckLog($jobs, $label, $outputdir);


	#### GET DURATION (STOP TIMER)
	my $duration = Util::Timer::runtime( $current_time, time() );

	#### PRINT DURATION
	my $datetime = Util::Timer::currentTime();
	$self->logDebug("Completed ", $label);
	$self->logDebug("duration", $duration);


	#### SET USAGE STATS FOR SINGLE OR BATCH JOB
	if ( defined $self->cluster()
		and $self->cluster() eq "LSF" )
	{
		$self->usageStats($jobs, $label, $duration);
		
		#### PRINT USAGE STATS TO usagefile
		$self->logDebug("Doing printUsage       " . Util::Timer::currentTime());
		$self->printUsage($jobs, $label);
		$self->logDebug("After printUsage       " . Util::Timer::currentTime());
	}

	##### CLEAN UP CLUSTER STDOUT AND STDERR FILES
	my $cleanup = $self->cleanup();
	if ( defined $cleanup and $cleanup )
	{
		$self->logDebug("Cleaning up scriptfiles.");
		foreach my $scriptfile ( @$scriptfiles )
		{
			$self->logDebug("Removing scriptfile", $scriptfile);
			`rm -fr $scriptfile*`;
		}
	}
	else
	{
		$self->logDebug("cleanup not defined. Leaving scriptfiles.");
	}

=cut

	$self->logDebug("END");

	return ("completed", "");	
	#return ($status, $sublabels);
}

method execute ( $jobs, $label ) {
=head2

	SUBROUTINE		execute
	
	PURPOSE
	
		execute A LIST OF JOBS CONCURRENTLY UP TO A MAX NUMBER
		
		OF CONCURRENT JOBS

=cut

	$self->logDebug("label", $label);
	$self->logDebug("no. jobs", scalar(@$jobs));

	my $cluster = $self->cluster();
	my $submit = $self->submit();
	$self->logDebug("cluster", $cluster);
	$self->logDebug("submit", $submit);
	my $scheduler = $self->scheduler();
	$self->logDebug("scheduler", $scheduler);

	my $username = $self->username();
	my $envar = $self->envar();
	$self->logDebug("envar: $envar");

	#### execute COMMANDS IN SERIES LOCALLY IF cluster NOT DEFINED
	if ( not defined $envar or not defined $submit or not $cluster or $scheduler ne "sge" )
	{
		$self->logDebug("Doing executeLocal(jobs, label)");
		$self->executeLocal($jobs, $label);
	}
	else
	{
		$self->logDebug("Doing executeCluster(jobs, label)");
		$self->executeCluster($jobs, $label);
	}
}

method executeLocal ( $jobs, $label ) {
=head2

	SUBROUTINE		executeLocal
	
	PURPOSE
	
		EXECUTE A LIST OF JOBS LOCALLY IN SERIES OR IN PARALLEL 

=cut

	$self->logCritical("jobs not defined") and exit if not defined $jobs;
	$self->logCritical("label not defined") and exit if not defined $label;
	$self->logDebug("XXXX no. jobs: " . scalar(@$jobs));

	#### INPUTS
	my $monitor = $self->monitor() if $self->can('db') and defined $self->table()->db();		#### ACCESSOR IS IMPLEMENTED
	my $cluster = $self->cluster();
	my $maxjobs = $self->maxjobs();
	my $qsub 	= $self->qsub();
	my $qstat 	= $self->qstat();
	my $sleep	= $self->sleep();

	#### SET DEFAULT SLEEP
	$sleep = 3 if not defined $sleep;
	
	#### QUIT IF maxjobs NOT DEFINED
	$self->logCritical("maxjobs not defined. Exiting") and exit if not defined $maxjobs;

	my $jobids = [];
	my $scriptfiles = [];

	my $counter = 0;
	$self->logDebug("executing " . scalar(@$jobs) . " jobs\n");
	foreach my $job ( @$jobs ) {	
		$counter++;
		$self->logDebug("counter", $counter);
		$self->logDebug("job", $job);
		
		#### CREATE OUTPUT DIRECTORY
		my $outputdir = $job->{outputdir};
		File::Path::mkpath($outputdir) if not -d $outputdir;
		$self->logDebug("Can't create outputdir", $outputdir) and die if not -d $outputdir;
	
		#### MOVE TO OUTPUT DIRECTORY
		$self->logDebug("Doing chdir($outputdir)");
		chdir($outputdir) or die "Can't move to output subdir: $outputdir/$counter\n";	
		
		my $commands = $job->{commands};
		my $lockfile = $job->{lockfile};

		#### execute COMMANDS
		print `date > $lockfile`;
		foreach my $command ( @$commands ) {
			$self->logDebug("command", $command);
			print `$command`;
			$self->logDebug("Completed command");
		}
		print `unlink $lockfile`;
		
		my $whoami = `whoami`;
		$self->logDebug("whoami", $whoami);
	}
	
	$self->logDebug("Completed");

	return $scriptfiles;
}

method executeCluster ( $jobs, $label ) {
=head2

	SUBROUTINE		executeCluster
	
	PURPOSE
	
		executeCluster A LIST OF JOBS CONCURRENTLY UP TO A MAX NUMBER
		
		OF CONCURRENT JOBS

=cut

	$self->logCritical("jobs not defined") and exit if not defined $jobs;
	$self->logCritical("label not defined") and exit if not defined $label;
	$self->logDebug("no. jobs: " . scalar(@$jobs));

	#### INSTANTIATE CLUSTER JOB MONITOR
	my $monitor = $self->setMonitor();
	
	#### INPUTS
	my $cluster = $self->cluster();
	my $maxjobs = $self->maxjobs();
	my $qsub 	= $self->qsub();
	my $qstat 	= $self->qstat();
	my $sleep	= $self->sleep();
	my $qsuboptions 	= $self->qsuboptions();	

	#### SET DEFAULT SLEEP
	$sleep = 3 if not defined $sleep;
	
	#### QUIT IF maxjobs NOT DEFINED
	$self->logCritical("maxjobs not defined. Exiting") and return if not defined $maxjobs;

	my $jobids = [];
	my $scriptfiles = [];

	#### DEBUG USAGE
	my $execute = 1;
	#$execute = 0;
	if ( $execute )
	{
		#### execute EVERY JOB
		my $counter = 0;
		$self->logDebug("executing " . scalar(@$jobs) . " jobs\n");
		foreach my $job ( @$jobs )
		{	
			$counter++;
			
			#### CREATE OUTPUT DIRECTORY
			my $outputdir = $job->{outputdir};
			File::Path::mkpath($outputdir) if not -d $outputdir;
			$self->logDebug("Can't create outputdir", $outputdir) and die if not -d $outputdir;
		
			#### MOVE TO OUTPUT DIRECTORY
			chdir($outputdir) or die "Can't move to output subdir: $outputdir/$counter\n";	
				#### GET FILES
				my $label = $job->{label};
				my $batch = $job->{batch};
				my $tasks = $job->{tasks};
				my $commands = $job->{commands};
				my $scriptfile = $job->{scriptfile};
				my $stdoutfile = $job->{stdoutfile};
				my $stderrfile = $job->{stderrfile};
				my $lockfile = $job->{lockfile};
				
				#### PRINT SHELL SCRIPT	
				$self->printScriptfile($scriptfile, $commands, $label, $stdoutfile, $stderrfile, $lockfile);
#				$self->logDebug("scriptfile", $scriptfile);
				
				#### SUBMIT AND GET THE JOB ID 
				#$self->logDebug("Doing scriptfile", $scriptfile);
				$self->logDebug("Doing monitor->submitJob()");
				my $jobid = $monitor->submitJob(
					{
						scriptfile  => $scriptfile,
						qsuboptions       => $qsuboptions,
						qmethod       	=> $qsub,
						qstat		=> $qstat,
						stdoutfile  => $stdoutfile,
						stderrfile  => $stderrfile,
						batch		=> $batch,
						tasks		=> $tasks
					}
				);
				$self->logCritical("jobid not defined. Exiting") and exit if not defined $jobid;
				$self->logDebug("jobid", $jobid);

				#### SAVE PID FOR CHECKING 
				push @$jobids, $jobid;
				$self->logDebug("Added jobid to list", $jobid );
				$self->logDebug("jobids ", scalar(@$jobids));
				$self->logDebug("maxjobs", $maxjobs);
			
				my $date = `date`;
				$date =~ s/\s+$//;
				$self->logDebug("date", $date);
				$self->logDebug("No. jobs: ", scalar(@$jobids));
				
				#### CHECK TO MAKE SURE WE HAVEN'T REACHED
				#### THE LIMIT OF MAX CONCURRENT JOBS
				while ( scalar(@$jobids) >= $maxjobs )
				{
					#$self->logDebug("Sleeping $sleep seconds...");
					sleep($sleep);
					
					
					$jobids = $monitor->remainingJobs($jobids);
				}
				
				#### CLEAN UP
				#`rm -fr $scriptfile`;
				push @$scriptfiles, $scriptfile;
			}	
		
		#}
		
		
		#### WAIT TIL ALL JOBS ARE FINISHED
		$self->logDebug("Waiting until the last jobs are finished (", scalar(@$jobids), " left)...");
		while ( defined $jobids and scalar(@$jobids) > 0 )
		{
			sleep($sleep);
			$jobids = $self->monitor()->remainingJobs($jobids);   
			$self->logDebug("", scalar(@$jobids), " jobs remaining: @$jobids");
		}
	}
	else
	{
		sleep(1);
	}
	$self->logDebug("Completed");

	return $scriptfiles;
}

method setJob ( $commands, $label, $outputdir, $scriptfile, $usagefile, $stdoutfile, $stderrfile, $lockfile) {
=head2

	SUBROUTINE		setJob
	
	PURPOSE
	
		GENERATE COMMANDS TO ALIGN SEQUENCES AGAINST REFERENCE
		
=cut
	#$self->logCaller("");
	#$self->logDebug("label", $label);
	#$self->logDebug("outputdir", $outputdir);
	##$self->logDebug("commands: @$commands");
	#$self->logDebug("stdoutfile", $stdoutfile);

	#### SANITY CHECK
	$self->logCritical("commands not defined") and exit if not defined $commands;
	$self->logCritical("label not defined") and exit if not defined $label;
	$self->logCritical("outputdir not defined") and exit if not defined $outputdir;

	#### SET DIRS
	my $scriptdir = "$outputdir/script";
	my $stdoutdir = "$outputdir/stdout";
	my $lockdir = "$outputdir/lock";

	#### CREATE DIRS	
	File::Path::mkpath($scriptdir) if not -d $scriptdir;
	File::Path::mkpath($stdoutdir) if not -d $stdoutdir;
	File::Path::mkpath($lockdir) if not -d $lockdir;
	$self->logError("Cannot create directory scriptdir: $scriptdir") and exit if not -d $scriptdir;
	$self->logError("Cannot create directory stdoutdir: $stdoutdir") and exit if not -d $stdoutdir;
	$self->logError("Cannot create directory lockdir: $lockdir") and exit if not -d $lockdir;	

	#### SET FILES IF NOT DEFINED
	$scriptfile = "$scriptdir/$label.sh" if not defined $scriptfile;
	$stdoutfile = "$stdoutdir/$label.out" if not defined $stdoutfile;
	$stderrfile = "$stdoutdir/$label.err" if not defined $stderrfile;
	$lockfile = "$lockdir/$label.lock" if not defined $lockfile;

	#### SET JOB LABEL, COMMANDS, ETC.
	my $job;
	$job->{label} 		= $label;
	$job->{commands} 	= $commands;
	$job->{outputdir} 	= $outputdir;
	$job->{scriptfile} 	= $scriptfile;
	$job->{stdoutfile} 	= $stdoutfile;
	$job->{stderrfile} 	= $stderrfile;
	$job->{lockfile} 	= $lockfile;
	
	#$self->logDebug("job", $job);	
	
	return $job;
}


method setBatchJob ( $commands, $label, $outputdir, $number) {
=head2

	SUBROUTINE		setBatchJob
	
	PURPOSE
	
		GENERATE COMMANDS TO ALIGN SEQUENCES AGAINST REFERENCE
		
=cut

	$self->logCritical("commands not defined. Exiting...") and exit if not defined $commands;
	$self->logCritical("label not defined. Exiting...") and exit if not defined $label;
	$self->logCritical("outputdir not defined. Exiting...") and exit if not defined $outputdir;
	$self->logCritical("number not defined. Exiting...") and exit if not defined $number;
	
	#### GET CLUSTER
	my $clustertype = $self->clustertype();
	$self->logDebug("outputdir", $outputdir);

	#### SET INDEX PATTERN FOR BATCH JOB
	my $index = $self->getIndex();
	$index =~ s/^\\//;

	#### CREATE TASK-RELATED DIRS
	my $scriptdir = "$outputdir/script";
	my $stdoutdir = "$outputdir/stdout/$index";
	my $lockdir = "$outputdir/lock/$index";
	my $taskdir = "$outputdir/$index";

	$self->createTaskDirs($taskdir, $number);
	$self->createTaskDirs($scriptdir, $number);
	$self->createTaskDirs($stdoutdir, $number);
	$self->createTaskDirs($lockdir, $number);	
	
	#### SET FILES IF NOT DEFINED
	my $scriptfile = "$scriptdir/$label.sh";
	my $stdoutfile = "$stdoutdir/$label-stdout.txt";
	my $stderrfile = "$stdoutdir/$label-stderr.txt";
	my $lockfile = "$lockdir/$label-lock.txt";

	#### SET FILE TO BE CHECKED TO FLAG COMPLETION
	#### NB: batchCheckfile CAN BE OVERRIDDEN BY THE INHERITING
	#### CLASS (E.G., Bowtie.pm) FOR NON-'out.sam' FILES
	my $checkfile = $self->batchCheckfile($label, $outputdir);

	#### SET JOB LABEL, COMMANDS, ETC.
	#### NB: FOR POSSIBLE FUTURE USE - ADD JOB ID TO FILENAME
	#$job->{usagefile} = "$outputdir/%I/$label-usage.%J.txt";
	#$job->{usagefile} = "$outputdir/\$PBS_TASKNUM/$label-usage.\$PBS_JOBID.txt";
	my $job;
	$job->{label} = $label;
	$job->{commands} = $commands;
	$job->{outputdir} = $outputdir;
	$job->{scriptfile} = $scriptfile;
	$job->{checkfile} = $checkfile;
	$job->{stdoutfile} = $stdoutfile;
	$job->{stderrfile} = $stderrfile;
	$job->{lockfile} = $lockfile;
	$job->{tasks} = $number;
	
	#### SET BATCH
	$job->{batch} = "$label\[1-$number\]" if $clustertype eq "LSF";
	$job->{batch} = "-t $number" if $clustertype eq "PBS";
	$job->{batch} = "-t 1-$number" if $clustertype eq "SGE";
	
	$self->logDebug("job", $job);
	
	return $job;
}


method createTaskDirs ( $directory, $number ) {
	$self->logDebug("directory: $directory ");
	$self->logDebug("number: $number ");

	my $index = $self->getIndex();
	$self->logDebug("index", $index);

	#### CREATE DIRS	
	my $dirs;
	for my $task ( 1..$number )
	{
		my $dir = $directory;
		$self->logDebug("BEFORE dir", $dir);
		
		use re 'eval';
		$dir =~ s/\\$index/$task/g;
		no re 'eval';
		$self->logDebug("AFTER dir", $dir);

		push @$dirs, $dir;
		File::Path::mkpath($dir) if not -d $dir;
	}

	return $dirs;
}

method printScriptfile {
=head2

	SUBROUTINE		printScriptfile
	
	PURPOSE
	
		PRINT SHELL SCRIPT CONFORMING TO CLUSTER TYPE
		
=cut

	$self->logNote("");

	my $scheduler =  $self->conf()->getKey("core:SCHEDULER");
	$self->logNote("scheduler", $scheduler);
	return $self->printPbsScriptfile(@_) if $scheduler eq "pbs";
	return $self->printLsfScriptfile(@_) if $scheduler eq "lsf";
	return $self->printSgeScriptfile(@_) if $scheduler eq "sge";
}

method printSgeScriptfile ( $scriptfile, $commands, $label, $stdoutfile, $stderrfile, $lockfile) {
=head2

	SUBROUTINE		printSgeScriptfile
	
	PURPOSE
	
		PRINT SHELL SCRIPT CONFORMING TO PBS FORMAT

	EXAMPLES
	
		#$ -pe mvapich 4 
		#$ -M [my email] 
		#$ -m ea 
		#$ -l h_rt=8:00:00 
		#$ -R y 
		#$ -j y 
		#$ -notify 
		#$ -cwd 

=cut

	$self->logDebug("scriptfile", $scriptfile);

	#### GET SLOTS
	my $slots		= $self->slots();
	$self->logDebug("slots", $slots);	

	# my $queue = $self->queue();
	my $cpus = $self->cpus();
	$cpus = 1 if not defined $cpus or not $cpus;
	$self->logNote("stdoutfile", $stdoutfile);
	$self->logNote("stderrfile", $stderrfile);
	$self->logNote("cpus", $cpus);

	my $contents = qq{#!/bin/bash\n\n};
	
	#### ! IMPORTANT !
	#### NEEDED BECAUSE EXEC NODES NOT FINDING $SGE_TASK_ID
	#### ADD LABEL
	$contents .= qq{#\$ -N $label\n};
	
	#### ADD CPUs
	$contents .= qq{#\$ -pe threaded $cpus\n} if defined $cpus and $cpus > 1;

	#### STDOUT AND STDERR
	$contents .= qq{#\$ -j y\n} if not defined $stderrfile;
	$contents .= qq{#\$ -o $stdoutfile\n} if defined $stdoutfile;
	$contents .= qq{#\$ -e $stderrfile\n} if defined $stderrfile;
	
	# #### ADD QUEUE
	# $contents .= qq{#\$ -q $queue\n};

	#### ADD SLOTS
	#$contents .= qq{#\$ -pe threaded $slots\n};
	
	##### ADD RESERVATIOIN
	#$contents .= qq{#\$ -R y\n};

	#### ADD RESERVATIOIN
	# $contents .= qq{#\$ -l h=annaisystems0*\n};

	#### ADD WALLTIME IF DEFINED
	my $walltime = $self->walltime();
	$contents .= qq{#\$ -l h_rt=$walltime:00:00\n} if defined $walltime and $walltime;

#### ADDITIONAL ENVARS
#echo COMMD_PORT: 	\$COMMD_PORT
#echo SGE_O_LOGNAME: \$SGE_O_LOGNAME
#echo SGE_O_MAIL: 	\$SGE_O_MAIL
#echo SGE_O_TZ: 		\$SGE_O_TZ
#echo SGE_CKPT_ENV: 	\$SGE_CKPT_ENV
#echo SGE_CKPT_DIR: 	\$SGE_CKPT_DIR

	$contents .= qq{
echo "-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*"
echo SGE_JOB_SPOOL_DIR: \$SGE_JOB_SPOOL_DIR
echo SGE_O_HOME: 		    \$SGE_O_HOME
echo SGE_O_HOST: 		    \$SGE_O_HOST
echo SGE_O_PATH: 		    \$SGE_O_PATH
echo SGE_O_SHELL: 		  \$SGE_O_SHELL
echo SGE_O_WORKDIR: 	  \$SGE_O_WORKDIR
echo SGE_STDERR_PATH:	  \$SGE_STDERR_PATH
echo SGE_STDOUT_PATH:	  \$SGE_STDOUT_PATH
echo SGE_TASK_ID: 		  \$SGE_TASK_ID
echo HOME: 				      \$HOME
echo HOSTNAME: 			    \$HOSTNAME
echo JOB_ID: 			      \$JOB_ID
echo JOB_NAME: 			    \$JOB_NAME
echo NQUEUES: 			    \$NQUEUES
echo NSLOTS: 			      \$NSLOTS

echo SGE_ROOT: 			    \$SGE_ROOT
echo SGE_CELL: 			    \$SGE_CELL
echo SGE_QMASTER_PORT: 	\$SGE_QMASTER_PORT
echo SGE_EXECD_PORT: 	  \$SGE_EXECD_PORT

echo USERNAME: 	     	  \$USERNAME
echo PROJECT:  	       	\$PROJECT
echo WORKFLOW: 	       	\$WORKFLOW
echo QUEUE:    	       	\$QUEUE

};

	#### ADD HOSTNAME
	$contents .= qq{hostname -f\n};

	#### PRINT LOCK FILE
	$contents .= qq{date > $lockfile\n};

	#### CREATE stdout DIR
	my ($stdoutdir)	=	$stdoutfile	=~	/^(.+?)\/[^\/]+$/;
	$contents .= qq{mkdir -p $stdoutdir\n};
	
	my $command = join "\n", @$commands;
	$contents .= "$command\n";

	#### REMOVE LOCK FILE
	$contents .= qq{unlink $lockfile;\n\nexit\n};

	$self->logDebug("contents", $contents);	
	
	open(OUT, ">$scriptfile") or die "Can't open script file: $scriptfile\n";
	print OUT $contents;
	close(OUT);
	chmod(0777, $scriptfile);
	#or die "Can't chmod 0777 script file: $scriptfile\n";
	$self->logNote("scriptfile printed", $scriptfile);
}

method printPbsScriptfile ( $scriptfile, $commands, $label, $stdoutfile, $stderrfile, $lockfile) {
=head2

	SUBROUTINE		printPbsScriptfile
	
	PURPOSE
	
		PRINT SHELL SCRIPT CONFORMING TO PBS FORMAT
		
=cut

	open(SHFILE, ">$scriptfile") or die "Can't open script file: $scriptfile\n";
	print SHFILE qq{#!/bin/bash\n\n};

	#### ! IMPORTANT !
	#### NEEDED BECAUSE EXEC NODES NOT FINDING $SGE_TASK_ID
	print SHFILE qq{export TASKNUM=\$(expr \$PBS_TASKNUM)\n\n};
	
	#### ADD LABEL
	print SHFILE qq{#PBS -N $label	                # The name of the job
};

	#### STDOUT AND STDERR
	print SHFILE qq{#PBS -j oe\n} if not defined $stderrfile;
	print SHFILE qq{#PBS -o $stdoutfile\n} if defined $stdoutfile;
	print SHFILE qq{#PBS -e $stderrfile\n} if defined $stderrfile;

	#### ADD WALLTIME IF DEFINED
	my $walltime = $self->walltime();
	print SHFILE qq{#PBS -W $walltime:00\n} if defined $walltime;

	print SHFILE qq{
echo running on PBS_O_HOST: 			\$PBS_O_HOST
echo originating queue is PBS_O_QUEUE: 	\$PBS_O_QUEUE
echo executing queue is PBS_QUEUE: 		\$PBS_QUEUE
echo working directory is PBS_O_WORKDIR:\$PBS_O_WORKDIR
echo execution mode is PBS_ENVIRONMENT: \$PBS_ENVIRONMENT
echo job identifier is PBS_JOBID: 		\$PBS_JOBID
echo job name is PBS_JOBNAME: 			\$PBS_JOBNAME
echo node file is PBS_NODEFILE: 		\$PBS_NODEFILE
echo current home directory PBS_O_HOME: \$PBS_O_HOME
echo PBS_O_PATH: 						\$PBS_O_PATH
echo PBS_JOBID: 						\$PBS_JOBID
};

	#### ADD HOSTNAME
	print SHFILE qq{hostname -f\n};

	#### PRINT LOCK FILE
	print SHFILE qq{date > $lockfile\n};

	#### PRINT ALL COMMANDS TO SHELL SCRIPT FILE
	foreach my $command ( @$commands )
	{
		print SHFILE "$command\n";
	}

	#### REMOVE LOCK FILE
	print SHFILE qq{unlink $lockfile;\n\nexit\n};

	close(SHFILE);
	chmod(0777, $scriptfile);
	#or die "Can't chmod 0777 script file: $scriptfile\n";
	$self->logDebug("scriptfile printed", $scriptfile);
}


method printLsfScriptfile ( $scriptfile, $commands, $label, $stdoutfile, $stderrfile, $lockfile) {
=head2

	SUBROUTINE		printLsfScriptfile
	
	PURPOSE
	
		PRINT SHELL SCRIPT CONFORMING TO LSF FORMAT
		
#BSUB -J�jobname	# assigns a name to job
#BSUB -B	        # Send email at job start
#BSUB -N	        # Send email at job end
#BSUB -e�errfile	# redirect stderr to specified file
#BSUB -o�out_file	# redirect stdout to specified file
#BSUB -a�application	# specify serial/parallel options
#BSUB -P�project_name	# charge job to specified project
#BSUB -W�runtime	# set wallclock time limit
#BSUB -q�queue_name	# specify queue to be used
#BSUB -n�num_procs	# specify number of processors
#BSUB -R    "span[ptile=num_procs_per_node]"	# specify MPI resource requirements

=cut

	#### SANITY CHECK
	$self->logCritical("scriptfile not defined") and exit if not defined $scriptfile;
	$self->logCritical("commands not defined") and exit if not defined $commands;
	$self->logCritical("label not defined") and exit if not defined $label;

	open(SHFILE, ">$scriptfile") or die "Can't open script file: $scriptfile\n";
	print SHFILE qq{#!/bin/bash
	
#BSUB -J $label             	# The name of the job
};

	#### ADD WALLTIME IF DEFINED
	my $walltime = $self->walltime();
	print SHFILE qq{#BSUB -W $walltime:00\n} if defined $walltime;

	print SHFILE qq{#BSUB -o $stdoutfile 			# print STDOUT to this file\n} if defined $stdoutfile;
	print SHFILE qq{#BSUB -e $stderrfile 			# print STDERR to this file\n} if defined $stderrfile;
	print SHFILE qq{

echo "LS_JOBID: " \$LS_JOBID
echo "LS_JOBPID: " \$LS_JOBPID
echo "LSB_JOBINDEX: " \$LSB_JOBINDEX
echo "LSB_JOBNAME: " \$LSB_JOBNAME
echo "LSB_QUEUE: " \$LSB_QUEUE
echo "LSFUSER: " \$LSFUSER
echo "LSB_JOB_EXECUSER: " \$LSB_JOB_EXECUSER
echo "HOSTNAME: " \$HOSTNAME
echo "LSB_HOSTS: " \$LSB_HOSTS
echo "LSB_ERRORFILE: " \$LSB_ERRORFILE
echo "LSB_JOBFILENAME: " \$LSB_JOBFILENAME
echo "LD_LIBRARY_PATH: " \$LD_LIBRARY_PATH

date > $lockfile

};

	#### PRINT ALL COMMANDS TO SHELL SCRIPT FILE
	foreach my $command ( @$commands )
	{
		print SHFILE "$command\n";
	}

	print SHFILE qq{
unlink $lockfile;

exit;
};

	close(SHFILE);
	chmod(0777, $scriptfile) or die "Can't chmod 0777 script file: $scriptfile\n";
	$self->logDebug("scriptfile printed", $scriptfile);

}








no Moose;


1;
