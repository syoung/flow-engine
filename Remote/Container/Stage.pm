use MooseX::Declare;

=head2

  PACKAGE    Engine::Remote::Container::Stage
  
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

class Engine::Remote::Container::Stage extends (Engine::Remote::Shell::Stage) {

#### ATTRIBUTES
# Bool
# Int
# Str
# HashRef/ArrayRef
# Class/Object


method BUILD ($args) {
  #### ssh AND profile ARE HANDLED BY INHERITED
  #### MODULE Engine::Remote::Shell::Stage.pm
}

method setSsh() {
  $self->logCaller( "DEBUG" );
  my $username = $self->username();
  $self->logDebug( "username", $username );

  my $ssh  = Engine::Remote::Ssh->new({
    conf          =>  $self->conf(),
    log           =>  $self->log(),
    printlog      =>  $self->printlog(),
    profilehash   =>  $self->profilehash(),
    username      =>  $self->username()
  });

  $self->ssh( $ssh );  
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
  $self->logDebug( "profilehash", $profilehash );
  
  #### GET FILE ROOTS
  my $username = $self->username();
  my $localfileroot = $self->util()->getFileroot( $username );  
  my $remotefileroot = $self->getRemoteFileroot( $profilehash, $username );
  $self->logDebug( "localfileroot", $localfileroot );
  $self->logDebug( "remotefileroot", $remotefileroot );
  my $remote = $self->setRunFiles( $remotefileroot );
  my $local  = $self->setRunFiles( $localfileroot );

  #### REGISTER PROCESS IDS SO WE CAN MONITOR THEIR PROGRESS
  $self->registerRunInfo( $local->{stdoutfile}, $local->{stderrfile} );

  #### SET SYSTEM CALL TO POPULATE RUN SCRIPT
  my $systemcall = $self->setSystemCall( $profilehash, $remote );

  $systemcall = $self->addOutputFiles( $systemcall, $remote->{stdoutfile}, $remote->{stderrfile} );

  #### SURROUND CALL WITH SINGLE QUOTES
  $$systemcall[ 0 ] = "'" . $$systemcall[ 0 ];
  $$systemcall[ scalar( @$systemcall ) - 1 ] = $$systemcall[ scalar( @$systemcall ) - 1 ] . "'"; 

  #### CREATE REMOTE SCRIPTDIR
  my $projectname  = $self->projectname();
  my $workflowname = $self->workflowname();
  my $scriptdir = "$remotefileroot/$projectname/$workflowname/scripts";
  $self->logDebug("scriptdir", $scriptdir);
  $self->ssh()->makeDir( $scriptdir );

  my $command = $self->setCommand( $profilehash, $systemcall );

  $self->printScriptFile( $command, $local->{scriptfile}, $remote->{exitfile}, $remote->{lockfile} );
  
  #### UPDATE STATUS TO 'running'
  my $now = $self->table()->db()->now();
  $self->setStageRunning( $now );

  #### NO BUFFERING
  $| = 1;

  #### COPY SCRIPT TO REMOTE
  my ( $stdout, $stderr, $exit ) = $self->copyToRemote( $profilehash, $local->{scriptfile}, $remote->{scriptfile} );
  $self->logDebug( "stdout", $stdout );
  $self->logDebug( "stderr", $stderr );
  $self->logDebug( "exit", $exit );

  #### SET PERMISSIONS
  ( $stdout, $stderr ) = $self->ssh()->command( "chmod 755 $remote->{scriptfile}" );
  $self->logDebug( "stdout", $stdout );
  $self->logDebug( "stderr", $stderr );

  #### EXECUTE
  ( $stdout, $stderr ) = $self->ssh()->command( $remote->{scriptfile} );
  
  #### RUN FILES
  $self->downloadRunFiles( $profilehash, $remote, $local );

  #### EXIT CODE
  my $exitcode = $self->getExitCode( $local->{exitfile} );
  $self->logDebug( "exitcode", $exitcode );
 
  #### SET STATUS TO 'error' IF exitcode IS NOT ZERO
  if ( defined $exitcode and $exitcode == 0 ) {
    $self->setStatus('completed') ;
  }
  else {
    $self->setStatus('error');
  }
  $exitcode  =~   s/\s+$// if defined $exitcode;
  $self->logDebug("FIRST exitcode", $exitcode);
  
  return $exitcode;
}

method setCommand ( $profilehash, $systemcall ) {
  my $platform = $self->getProfileValue( "run:platform", $profilehash );
  my $bindpoints = $self->getProfileValue( "run:bindpoints", $profilehash );
  my $imagefile = $self->getProfileValue( "run:imagefile", $profilehash );
  $self->logDebug( "platform", $platform );
  $self->logDebug( "bindpoints", $bindpoints );
  $self->logDebug( "imagefile", $imagefile );

  my $command = "";
  if ( $platform eq "singularity" ) {
    $command .= "/usr/bin/singularity exec \\\n";
    if ( $bindpoints ) {
      foreach my $bindpoint ( @$bindpoints ) {
        $self->logDebug( "bindpoint", $bindpoint );
        $command .= " -B $bindpoint \\\n";
      }
    }
  }

  $command .= " $imagefile \\\n";
  $command .= " /bin/bash -c \\\n";
  $command .= join " \\\n", @$systemcall;

  return $command;
}

method setSystemCall ( $profilehash, $runfiles ) {
  $self->logCaller();

  #### GET FILE ROOT
  my $username      =  $self->username();
  my $fileroot      =  $self->fileroot();
  my $userhome      =  $self->userhome();
  my $envar         =  $self->envar();
  my $stagenumber   =  $self->appnumber();  
  my $basedir       = $self->conf()->getKey("core:BASEDIR");
  my $installdir = $self->conf()->getKey("core:INSTALLDIR");
  $self->logDebug("fileroot", $fileroot);
  $self->logDebug( "installdir: $installdir" );
  
  my $stageparameters =  $self->stageparameters();
  $self->logDebug( "stageparameters", $stageparameters );
  $self->logError("stageparemeters not defined") and exit if not defined $stageparameters;

  my $projectname   =  $$stageparameters[0]->{projectname};
  my $workflowname  =  $$stageparameters[0]->{workflowname};

  #### REPLACE <TAGS> IN PARAMETERS
  foreach my $stageparameter ( @$stageparameters ) {
    $stageparameter->{value} = $self->replaceTags( $stageparameter->{value}, $profilehash, $userhome, $fileroot, $projectname, $workflowname, $installdir, $basedir );
  }

  #### CONVERT ARGUMENTS INTO AN ARRAY
  my $arguments = $self->setArguments( $stageparameters );
  $self->logDebug("arguments", $arguments);

  #### SET USAGE
  my $usage  =  $self->setUsagefile( $profilehash, $runfiles->{usagefile} );
  $self->logDebug( "usage", $usage );

  #### SET EXPORTS
  my $exports  = $self->getPrescript( $profilehash, $userhome, $fileroot, $projectname, $workflowname, $installdir, $basedir );
  $self->logDebug("exports", $exports);

  #### SET EXECUTOR 
  my $executor = $self->executor();
  $self->logDebug("executor", $executor);
  
  #### PREFIX APPLICATION PATH WITH PACKAGE INSTALLATION DIRECTORY
  my $application = $self->installdir() . "/" . $self->location();  
  $self->logDebug("$$ application", $application);
  $application  =  $self->replaceTags( $application, $userhome, $fileroot, $projectname, $workflowname, $installdir );

  #### SET SYSTEM CALL
  my $systemcall = [];
  push ( @$systemcall, $exports ) if $exports;
  push ( @$systemcall, $usage );
  push ( @$systemcall, $executor ) if defined $executor and $executor ne "";
  push @$systemcall, $application;
  @$systemcall = (@$systemcall, @$arguments);

  return $systemcall;  
}

method setUsagefile( $profilehash, $usagefile ) {
  my $containertime = $self->getProfileValue( "run:binary:time", $profilehash ); 

  my $time = "/usr/bin/time";
  if ( $containertime ) {
    $time = $containertime;
  }
  return qq{$time \\
 -o $usagefile \\
 -f "%Uuser %Ssystem %Eelapsed %PCPU (%Xtext+%Ddata %Mmax)k"};
}


} #### Engine::Stage

