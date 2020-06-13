use MooseX::Declare;


=head2

  PACKAGE    Engine::Remote::Queue::Stage
  
  PURPOSE:
  
    A STAGE IS ONE STEP IN A WORKFLOW. EACH 
    
    STAGE DOES THE FOLLOWING:
    
    1.  RUNS AN ENTRY IN THE stage DATABASE TABLE

    2. LOGS ITS STATUS TO THE stage DATABASE TABLE
    
    3. DYNAMICALLY SETS STDOUT, STDERR, INPUT 

      AND OUTPUT FILES.
    
=cut 

use strict;
use warnings;

#### USE LIB FOR INHERITANCE
use FindBin qw($Bin);
use lib "$Bin/../";

class Engine::Remote::Queue::Stage extends (Engine::Local::Shell::Stage) {

#### EXTERNAL MODULES
use File::Path;

#### INTERNAL MODULES
use Util::Remote::Ssh;

#### ATTRIBUTES
# Bool
# Int
# Str
# HashRef/ArrayRef
# Class/Object


has 'ssh'  =>  (
  is     =>  'rw',
  isa    =>  'Util::Remote::Ssh',
);

method BUILD ($args) {
  # $self->logDebug( "args", $args );
  $self->profile( $args->{profile} ) if $args->{profile};
}

method setSsh( $profilehash ) {
  my $ssh  = Util::Remote::Ssh->new({
    conf          =>  $self->conf(),
    log           =>  $self->log(),
    printlog      =>  $self->printlog()
  });

  $ssh->setUp( $profilehash );

  $self->ssh( $ssh );  
}

method setSystemCall ( $profilehash, $runfiles ) {
  $self->logCaller();

  #### GET FILE ROOT
  my $username      =  $self->username();
  my $userhome      =  $self->userhome();
  my $envar         =  $self->envar();
  my $stagenumber   =  $self->appnumber();  
  my $basedir       =  $self->conf()->getKey("core:DIR");

  #### ADD PERL5LIB TO ENABLE EXTERNAL SCRIPTS TO USE OUR MODULES
  my $installdir = $self->conf()->getKey("core:INSTALLDIR");
  my $perl5lib = $ENV{"PERL5LIB"};
  $self->logDebug( "installdir: $installdir" );
  $self->logDebug( "perl5lib: $perl5lib" );

  #### SET REMOTE FILEROOT
  my $remoteusername = $profilehash->{ virtual }->{ username };
  my $fileroot = $self->getRemoteFileroot( $profilehash, $remoteusername );
  $self->logDebug( "fileroot", $fileroot );

  my $stageparameters =  $self->stageparameters();
  # $self->logDebug( "stageparameters", $stageparameters );
  $self->logError("stageparemeters not defined") and exit if not defined $stageparameters;
# $self->logDebug( "DeBUG EXIT" ) and exit;

  my $projectname   =  $$stageparameters[0]->{projectname};
  my $workflowname  =  $$stageparameters[0]->{workflowname};

  #### REPLACE <TAGS> IN PARAMETERS
  foreach my $stageparameter ( @$stageparameters ) {
    $stageparameter->{value} = $self->replaceTags( $stageparameter->{value}, $profilehash, $userhome, $fileroot, $projectname, $workflowname, $installdir, $basedir );
  }

  #### CONVERT ARGUMENTS INTO AN ARRAY IF ITS A NON-EMPTY STRING
  my $arguments = $self->setArguments( $stageparameters );
  $self->logDebug("arguments", $arguments);

  #### ADD USAGE COMMAND (HANDLE OSX VERSION OF time )
  my $usage  =  $self->setUsagefile( $runfiles->{usagefile} );

  #### SET EXPORTS
  my $exports     = "export STAGENUMBER=$stagenumber;";  
  $exports .=  "export PERL5LIB=$perl5lib; ";
  $exports .=  " cd $fileroot/$projectname/$workflowname;";
  $exports .= $self->getPrescript( $profilehash, $userhome, $fileroot, $projectname, $workflowname, $installdir, $basedir );
  $self->logDebug("FINAL exports", $exports);

  #### SET EXECUTOR 
  my $executor = $self->executor();
  $self->logDebug("executor", $executor);
  
  #### PREFIX APPLICATION PATH WITH PACKAGE INSTALLATION DIRECTORY
  my $application = $self->installdir() . "/" . $self->location();  
  $self->logDebug("$$ application", $application);
  $application  =  $self->replaceTags( $application, $userhome, $fileroot, $projectname, $workflowname, $installdir );

  #### SET SYSTEM CALL
  my $systemcall = [];
  push @$systemcall, $exports;
  push @$systemcall, $usage;
  push @$systemcall, $executor if defined $executor and $executor ne "";
  push @$systemcall, $application;
  @$systemcall = (@$systemcall, @$arguments);
  $self->logDebug( "systemcall", $systemcall );

  return $systemcall;  
}

# SUBROUTINE    run
#
# PURPOSE
#
#   1. AND UPDATE STATUS TO 'running'
#
#   2. RUN THE STAGE APPLICATION 
#  
#   3. WAIT TO COMPLETE
#
#   4. UPDATE STATUS TO 'complete' OR 'error'

method run ( $dryrun ) {
  my $profilehash = $self->profilehash();
  $self->logDebug("dryrun", $dryrun);
  # $self->logDebug( "profilehash", $profilehash );

  #### SET SSH
  $self->setSsh( $profilehash );
  
  #### SET RUN FILES
  my $localusername = $self->username();
  my $remoteusername = $profilehash->{ virtual }->{ username };
  my $localfileroot = $self->util()->getFileroot( $localusername );  
  my $remotefileroot = $self->getRemoteFileroot( $profilehash, $remoteusername );
  $self->logDebug( "localfileroot", $localfileroot );
  $self->logDebug( "remotefileroot", $remotefileroot );
  my $remote = $self->setRunFiles( $remotefileroot );
  my $local  = $self->setRunFiles( $localfileroot );

  #### REGISTER PROCESS IDS SO WE CAN MONITOR THEIR PROGRESS
  $self->registerRunInfo( $local->{ stdoutfile }, $local->{ stderrfile } );

  #### SET SYSTEM CALL TO POPULATE RUN SCRIPT
  my $systemcall = $self->setSystemCall( $profilehash, $remote );

  #### ADD STDOUT AND STDERR FILES TO SYSTEM CALL
  $systemcall = $self->addOutputFiles( $systemcall, $remote->{stdoutfile}, $remote->{stderrfile} );

  #### GET PID
  push @$systemcall, ' & echo $!';  

  #### CREATE REMOTE SCRIPTDIR
  $self->ssh()->command( "ls /" );

  my $projectname  = $self->projectname();
  my $workflowname = $self->workflowname();
  my $scriptdir = "$remotefileroot/$projectname/$workflowname/scripts";
  $self->logDebug( "CREATING REMOTE SCRIPTDIR", $scriptdir );
  $self->ssh()->makeDir( $scriptdir );
  
  #### PRINT COMMAND TO .sh FILE
  my $command = join " \\\n", @$systemcall;
  $self->printScriptFile( $command, $local->{scriptfile}, $remote->{exitfile}, $remote->{lockfile} );
  
  #### UPDATE STATUS TO 'running'
  $self->setRunningStatus();

  #### NO BUFFERING
  $| = 1;

  #### COPY SCRIPT TO REMOTE
  my ( $stdout, $stderr, $exit ) = $self->copyToRemote( $profilehash, $local->{scriptfile}, $remote->{scriptfile} );
  $self->logDebug( "stdout", $stdout );
  $self->logDebug( "stderr", $stderr );
  $self->logDebug( "exit", $exit );

  #### SET PERMISSIONS
  ( $stdout, $stderr ) = $self->ssh()->command( "chmod 755 $remote->{scriptfile}" );
  # $self->logDebug( "stdout", $stdout );
  # $self->logDebug( "stderr", $stderr );

  #### EXECUTE
  ( $stdout, $stderr ) = $self->ssh()->command( $remote->{scriptfile} );
  my $processid = $stdout;
  $processid =~ s/\s+//g;
  $self->logDebug( "PROCESS ID", $processid );
  # $self->logDebug( "stderr", $stderr );
  
  # my $processid = "117095";

  #### POLL FOR COMPLETION
  my $success = $self->pollForCompletion( $processid );

$self->logDebug( "success", $success );
$self->logDebug( 'DEBUG EXIT' ) and exit;

  #### RUN FILES
  $self->downloadRunFiles( $profilehash, $remote, $local );

  # #### EXIT CODE
  # my $exitcode = $self->getExitCode( $local->{exitfile} );
  # $self->logDebug( "exitcode", $exitcode );
 
  #### IF success IS ZERO, SET STATUS TO 'completed'
  #### OTHERWISE, SET STATUS TO 'error' 
  $self->setFinalStatus( $success );
  
  return $success;
}

method pollForCompletion ( $processid ) {

  $self->logDebug( "processid", $processid );

  my $limit = 999;
  my $sleep = 5;
  $self->logDebug( "sleep", $sleep );
  my $counter = 0;
  while ( $counter < $limit ) {
    $counter++;
    my ( $stdout, $stderr ) = $self->ssh()->command( "ps aux | grep $processid" );
    # $stdout =~ s/\s*$//g;

#     $processid = "117872";
#     $stdout = "117872  0.0  0.0   4356   648 ?        S    09:26   0:00 /usr/bin/time -o /home/ubuntu/.flow/abmod/5-40/scripts/1-download-pdb.usage -f %Uuser %Ssystem %Eelapsed %PCPU (%Xtext+%Ddata %Mmax)k /usr/bin/aws s3 cp s3://snugdock/benchmark/conf/ace2-vhh_complex_start.pdb /home/ubuntu/.flow/abmod/5-40
# ubuntu   117874  0.0  0.0  11240  3084 ?        Ss   09:26   0:00 bash -c ps aux | grep 117872 
# ubuntu   117876  0.0  0.0  12940   928 ?        S    09:26   0:00 grep 117872";

#     $self->logDebug( "BEFORE stdout", $stdout );
#     $stdout =~ s/[^\n]+grep\s+$processid//msg;
#     $self->logDebug( "AFTER stdout", $stdout );

#     $stdout = "ubuntu   117874  0.0  0.0  11240  3084 ?        Ss   09:26   0:00 bash -c ps aux | grep 117872
# ";


#     $stdout = "ubuntu   117874  0.0  0.0  11240  3084 ?        Ss   09:26   0:00 bash -c ps aux | grep 117872 
# ubuntu   117876  0.0  0.0  12940   928 ?        S    09:26   0:00 grep 117872";

    $self->logDebug( "BEFORE stdout", $stdout );
    $stdout =~ s/[^\n]+grep\s+$processid\s*//msgi;
    $self->logDebug( "AFTER stdout", $stdout );



    if ( defined $stdout and $stdout !~ /^\s*$/ ) {
      $self->logDebug( "WAITING FOR PROCESS TO END. processid", $processid );
      sleep( $sleep );
    }
    else {
      $self->logDebug( "RETURNING ZERO" );
      return 0;  #### OK
    }
  }

  return 1;  #### ERROR
}

# SUBROUTINE    printScriptFile
#
# PURPOSE
#
#   RETURN THE JOB HASH FOR THIS STAGE:
# 
#     command    :  Command line system call,
#     label    :  Unique name for job (e.g., to be used by SGE)
#     outputfile  :  Location of outputfile
#

method printScriptFile ( $command, $scriptfile, $exitfile, $lockfile ) {
  $self->logDebug("scriptfile", $scriptfile);

  #### CREATE DIR COMMANDS
  $self->mkdirCommand($scriptfile);

  my $contents  =  qq{#!/bin/bash

# OPEN LOCKFILE
date > $lockfile

$command

};
  $self->logDebug("contents", $contents);



# $self->logDebug( "DEBUG EXIT" ) and exit;




  open(OUT, ">$scriptfile") or die "Can't open script file: $scriptfile\n";
  print OUT $contents;
  close(OUT);
  chmod(0777, $scriptfile);
  $self->logNote("scriptfile printed", $scriptfile);
}

method remoteCommand ( $command, $scriptfile, $stdoutfile, $stderrfile ) {
  $self->logDebug( "command", $command );
  $self->logDebug( "scriptfile", $scriptfile );
  $self->logDebug( "stdoutfile", $stdoutfile );  
  $self->logDebug( "stderrfile", $stderrfile );

  my ( $scriptdir ) = $scriptfile =~ /^(.+?)\/[^\/]+$/;
  $self->logDebug( "scriptdir", $scriptdir );

  $self->makeRemoteDir( $scriptdir );
  $self->copyToRemote( $scriptfile, $scriptfile );

  my($stdout, $stderr, $exit) = $self->ssh()->cmd( $command );
  $self->logDebug( "stdout", $stdout );
  $self->logDebug( "stderr", $stderr );
  $self->logDebug( "exit", $exit );

  return ( $stdout, $stderr, $exit );
}

method downloadRunFiles ( $profilehash, $remote, $local ) {
  my $files = [ "stdoutfile", "stderrfile", "scriptfile", "exitfile", "lockfile", "usagefile" ];
    # $self->logDebug( "remote", $remote );
    # $self->logDebug( "local", $local );
  
  my $success = 1;
  foreach my $file ( @$files ) {
    # $self->logDebug( "file", $file );
    $success = 0 if not $self->copyFromRemote( $profilehash, $remote->{$file}, $local->{$file} );
    # $self->logDebug( "success", $success );
  }

  return $success;
}

method getRemoteFileroot ( $profilehash, $username ) {
  $self->logNote("username", $username);

  my $profiler = Util::Profiler->new();
  $profiler->profilehash( $profilehash );

  my $homedir = $self->getProfileValue( "host:homedir", $profilehash ) || "/home";
  my $basedir = $self->conf()->getKey("core:DIR");
  my $fileroot = "$homedir/$username/$basedir";
  
  return $fileroot;    
}

method copyToRemote ( $profilehash, $sourcefile, $targetfile ) {
  # $self->logDebug( "profilehash", $profilehash );
  # $self->logDebug( "sourcefile", $sourcefile );
  # $self->logDebug( "targetfile", $targetfile );

  my $remoteusername = $profilehash->{ virtual }->{ username };
  my $ipaddress = $profilehash->{ instance }->{ ipaddress };
  my $source = $sourcefile;
  my $target = "$remoteusername\@$ipaddress:$targetfile";
  # $self->logDebug( "source", $source ); 
  $self->logDebug( "target", $target );   
  
  my $result = $self->ssh()->copy( $source, $target );
  # $self->logDebug( "result", $result );

  return $result;
}

method copyFromRemote ( $profilehash, $sourcefile, $targetfile ) {
  # $self->logCaller();
  # $self->logDebug( "profilehash", $profilehash );
  # $self->logDebug( "sourcefile", $sourcefile );
  # $self->logDebug( "targetfile", $targetfile );

  my $remoteusername = $profilehash->{ virtual }->{ username };
  my $ipaddress = $profilehash->{ instance }->{ ipaddress };
  my $source = "$remoteusername\@$ipaddress:$sourcefile";
  my $target = $targetfile;
  # $self->logDebug( "remotehost", $remotehost );    
  # $self->logDebug( "source", $source ); 
  
  my $result = $self->ssh()->copy( $source, $target );
  # $self->logDebug( "result", $result );

$self->logDebug( "target $result ", $target );   
  
  return $result;
}

method containsRedirection ($arguments) {
  return if not defined $arguments or not @$arguments;
  
  foreach my $argument ( @$arguments ) {
    return "stdout" if $argument eq ">" or $argument eq "1>";
    return "stderr" if $argument eq "2>";
  }
  
  return 0;
}


# SUBROUTINE    setStageJob
#
# PURPOSE
#
#   RETURN THE JOB HASH FOR THIS STAGE:
#  
#     command    :  Command line system call,
#     label    :  Unique name for job (e.g., to be used by SGE)
#     outputfile  :  Location of outputfile





} #### Stage

