use MooseX::Declare;


=head2

  PACKAGE    Engine::Remote::Shell::Stage
  
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

class Engine::Remote::Shell::Stage extends (Engine::Local::Shell::Stage) {

#### EXTERNAL MODULES
use File::Path;

#### INTERNAL MODULES
use Engine::Remote::Ssh;

#### ATTRIBUTES
# Bool
# Int
# Str
# HashRef/ArrayRef
# Class/Object


has 'ssh'  =>  (
  is     =>  'rw',
  isa    =>  'Engine::Remote::Ssh',
);

method BUILD ($args) {
  # $self->logDebug( "args", $args );

  $self->profile( $args->{profile} ) if $args->{profile};

}

method setSsh( $profilehash ) {
  my $username = $self->username();
  $self->logDebug( "username", $username );

  my $ssh  = Engine::Remote::Ssh->new({
    conf          =>  $self->conf(),
    log           =>  $self->log(),
    printlog      =>  $self->printlog()
  });

  $ssh->setUp( $profilehash );

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

  my $remote = $self->setRunFiles( $remotefileroot );
  my $local  = $self->setRunFiles( $localfileroot );

  #### REGISTER PROCESS IDS SO WE CAN MONITOR THEIR PROGRESS
  $self->registerRunInfo( $local->{ stdoutfile }, $local->{ stderrfile } );

  #### SET SYSTEM CALL TO POPULATE RUN SCRIPT
  my $systemcall = $self->setSystemCall( $profilehash, $remote );

  #### ADD STDOUT AND STDERR FILES TO SYSTEM CALL
  $systemcall = $self->addOutputFiles( $systemcall, $remote->{stdoutfile}, $remote->{stderrfile} );

  #### CREATE REMOTE SCRIPTDIR
  my $projectname  = $self->projectname();
  my $workflowname = $self->workflowname();
  my $scriptdir = "$remotefileroot/$projectname/$workflowname/scripts";
  $self->logDebug("scriptdir", $scriptdir);
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
  $self->logDebug( "stdout", $stdout );
  $self->logDebug( "stderr", $stderr );

  #### EXECUTE
  ( $stdout, $stderr ) = $self->ssh()->command( $remote->{scriptfile} );
  
  #### RUN FILES
  $self->downloadRunFiles( $profilehash, $remote, $local );

  #### EXIT CODE
  my $exitcode = $self->getExitCode( $local->{exitfile} );
  $self->logDebug( "exitcode", $exitcode );
 
  #### IF exitcode IS ZERO, SET STATUS TO 'completed'
  #### OTHERWISE, SET STATUS TO 'error' 
  $self->setFinalStatus( $exitcode );
  
  return $exitcode;
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

    my $homedir = $self->getProfileValue( "host:homedir", $profilehash ) || "/home";
    my $basedir = $self->conf()->getKey("core:DIR");
    my $fileroot = "$homedir/$username/$basedir";
    
    return $fileroot;    
}

method copyToRemote ( $profilehash, $sourcefile, $targetfile ) {
  $self->logDebug( "profilehash", $profilehash );
  # $self->logDebug( "sourcefile", $sourcefile );
  # $self->logDebug( "targetfile", $targetfile );

  my $remoteusername = $profilehash->{ virtual }->{ username };
  my $ipaddress = $profilehash->{ instance }->{ ipaddress };
  my $source = $sourcefile;
  my $target = "$remoteusername\@$ipaddress:$targetfile";
  # $self->logDebug( "source", $source ); 
  $self->logDebug( "target", $target );   
  
  my $result = $self->ssh()->copy( $source, $target );
  $self->logDebug( "result", $result );

  return $result;
}

method copyFromRemote ( $profilehash, $sourcefile, $targetfile ) {
  # $self->logCaller();
  # $self->logDebug( "profilehash", $profilehash );
  # $self->logDebug( "sourcefile", $sourcefile );
  # $self->logDebug( "targetfile", $targetfile );

  my $remotehost = $profilehash->{ virtual }->{ username };
  my $source = "$remotehost:$sourcefile";
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

