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

class Engine::Remote::Queue::Stage with (Util::Logger, Engine::Common::Stage, Exchange) {

#### EXTERNAL MODULES
use File::Path;

#### INTERNAL MODULES
use Util::Profile;
use Util::Remote::Ssh;

#### ATTRIBUTES
# Bool
# Int/Nums
# Str
# HashRef/ArrayRef
# Class/Object

has 'ssh'  =>  (
  is     =>  'rw',
  isa    =>  'Util::Remote::Ssh',
);

method BUILD ($args) {
  # $self->logDebug( "args", $args );
  # $self->profilehash( $args->{profilehash} ) if $args->{profilehash};
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


# SUBROUTINE    setStageJob
#
# PURPOSE
#
#   RETURN THE JOB HASH FOR THIS STAGE:
#  
#     command    :  Command line system call,
#     label    :  Unique name for job (e.g., to be used by SGE)
#     outputfile  :  Location of outputfile


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
  $self->logDebug( "profilehash", $profilehash );
  my $projectname  = $self->projectname();
  my $workflowname = $self->workflowname();
  my $username     = $self->username();

  my $remoteusername = undef;
  if ( defined $profilehash ) {
    my $profile = Util::Profile->new( {
      log         => $self->log(),
      printlog    => $self->printlog(),
      profilehash => $profilehash
    } );
    $remoteusername = $profile->getProfileValue( "virtual:username" );
  }
  # $self->logDebug( "remoteuser", $remoteuser );

  #### SEND TASK
  my $stagedata = $self->toData();
  $stagedata->{ remoteusername } = $remoteusername;
  $self->logDebug( "stagedata", $stagedata, 1 );

  my $queuename = "$username.$projectname.$workflowname.queue";
  $self->sendTask( $queuename, $stagedata );

  #### SET STATUS=queued
  my $now = $self->table()->db()->now();
  $self->setStageQueued( $now );
}


#   # #### UPDATE STATUS TO 'running'
#   # $self->setCompleted();


#   # #### SET SSH
#   # $self->setSsh( $profilehash );
  
#   # #### SET RUN FILES
#   # my $localusername = $self->username();
#   # my $remoteusername = $profilehash->{ virtual }->{ username };
#   # my $localfileroot = $self->util()->getFileroot( $localusername );  
#   # my $remotefileroot = $self->getRemoteFileroot( $profilehash, $remoteusername );
#   # $self->logDebug( "localfileroot", $localfileroot );
#   # $self->logDebug( "remotefileroot", $remotefileroot );
#   # my $remote = $self->setRunFiles( $remotefileroot );
#   # my $local  = $self->setRunFiles( $localfileroot );
  

# #   #### POLL FOR COMPLETION
# #   my $success = $self->pollForCompletion( $processid );

# # $self->logDebug( "success", $success );
# # $self->logDebug( 'DEBUG EXIT' ) and exit;

# #   #### RUN FILES
# #   $self->downloadRunFiles( $profilehash, $remote, $local );

# #   # #### EXIT CODE
# #   # my $exitcode = $self->getExitCode( $local->{exitfile} );
# #   # $self->logDebug( "exitcode", $exitcode );
 
# #   #### IF success IS ZERO, SET STATUS TO 'completed'
# #   #### OTHERWISE, SET STATUS TO 'error' 
# #   $self->setFinalStatus( $success );
  
# #   return $success;



# # SUBROUTINE    printScriptFile
# #
# # PURPOSE
# #
# #   RETURN THE JOB HASH FOR THIS STAGE:
# # 
# #     command    :  Command line system call,
# #     label    :  Unique name for job (e.g., to be used by SGE)
# #     outputfile  :  Location of outputfile
# #

# method printScriptFile ( $command, $scriptfile, $exitfile, $lockfile ) {
#   $self->logDebug("scriptfile", $scriptfile);

#   #### CREATE DIR COMMANDS
#   $self->mkdirCommand($scriptfile);

#   my $contents  =  qq{#!/bin/bash

# # OPEN LOCKFILE
# date > $lockfile

# $command

# };
#   $self->logDebug("contents", $contents);



# # $self->logDebug( "DEBUG EXIT" ) and exit;




#   open(OUT, ">$scriptfile") or die "Can't open script file: $scriptfile\n";
#   print OUT $contents;
#   close(OUT);
#   chmod(0777, $scriptfile);
#   $self->logNote("scriptfile printed", $scriptfile);
# }

# method remoteCommand ( $command, $scriptfile, $stdoutfile, $stderrfile ) {
#   $self->logDebug( "command", $command );
#   $self->logDebug( "scriptfile", $scriptfile );
#   $self->logDebug( "stdoutfile", $stdoutfile );  
#   $self->logDebug( "stderrfile", $stderrfile );

#   my ( $scriptdir ) = $scriptfile =~ /^(.+?)\/[^\/]+$/;
#   $self->logDebug( "scriptdir", $scriptdir );

#   $self->makeRemoteDir( $scriptdir );
#   $self->copyToRemote( $scriptfile, $scriptfile );

#   my($stdout, $stderr, $exit) = $self->ssh()->cmd( $command );
#   $self->logDebug( "stdout", $stdout );
#   $self->logDebug( "stderr", $stderr );
#   $self->logDebug( "exit", $exit );

#   return ( $stdout, $stderr, $exit );
# }

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




} #### Stage

