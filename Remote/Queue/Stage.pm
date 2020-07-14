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
  $stagedata->{ first   } = $self->first();
  $stagedata->{ last    } = $self->last();
  $stagedata->{ current } = $self->appnumber();
  $self->logDebug( "stagedata", $stagedata, 1 );

  #### ADD TO job DATABASE TABLE
  my $success = $self->table()->addJob( $stagedata );
  $self->logDebug( "success", $success );

  my $queuename = "$username.$projectname.$workflowname.queue";
  $self->sendTask( $queuename, $stagedata );

  #### SET STATUS=queued
  my $now = $self->table()->db()->now();
  $self->table()->setStageQueued( $stagedata, $now );
}

# method kill {
#   #### SEND doShutdown

#   $self->stop();

#   my $queuename = "$username.$projectname.$workflowname.queue";
#   $self->sendTask( $queuename, $stagedata );

# }

method stop {
  $self->logCaller();

  my $data = {
    mode            => "doStop",
    username        => $self->username(),
    projectname     => $self->projectname(),
    workflowname    => $self->workflowname(),
    workflownumber  => $self->workflownumber(),
    appname         => $self->appname(),
    appnumber       => $self->appnumber()    
  };

  my $exchange = $self->conf()->getKey( "mq:service:steward" );
  $self->logDebug( "exchange", $exchange );
  my $routingkey = $self->username() . "." . $self->projectname() . "." . $self->workflowname() . ".topic";
  $self->sendTopic( $exchange, $routingkey, $data );

  # #### SET STATUS=queued
  # my $now = $self->table()->db()->now();
  # $self->table()->setStageQueued( $data, $now );

  # my $processid = $self->getProcessId( $workflowdata );
  # $self->logDebug( "processid", $processid );

# #### 1. 'kill -9' THE PROCESS IDS OF ANY RUNNING STAGE OF THE WORKFLOW
# #### 2. INCLUDES STAGE PID, App PARENT PID AND App CHILD PID)

#     # $self->logDebug("stages", $stages);
#     # my $messages = [];
#     # foreach my $stage ( @$stages )
#     # {
#     #     #### OTHERWISE, KILL ALL PIDS
#     #     push @$messages, $self->util()->killPid($stage->{childpid}) if defined $stage->{childpid};
#     #     push @$messages, $self->util()->killPid($stage->{parentpid}) if defined $stage->{parentpid};
#     #     push @$messages, $self->util()->killPid($stage->{stagepid}) if defined $stage->{stagepid};
#     #     push @$messages, $self->util()->killPid($stage->{workflowpid}) if defined $stage->{workflowpid};
#     # }


}

method getProcessId( $data ) {
# #   my $job = $self->table()->getJobByWorkflow( $data );
# #   $self->logDebug( "job", $job );

# #   return $job->{ processid };
# # }

# #   # #### UPDATE STATUS TO 'running'
# #   # $self->setCompleted();


# #   # #### SET SSH
# #   # $self->setSsh( $profilehash );
  
# #   # #### SET RUN FILES
# #   # my $localusername = $self->username();
# #   # my $remoteusername = $profilehash->{ virtual }->{ username };
# #   # my $localfileroot = $self->util()->getFileroot( $localusername );  
# #   # my $remotefileroot = $self->getRemoteFileroot( $profilehash, $remoteusername );
# #   # $self->logDebug( "localfileroot", $localfileroot );
# #   # $self->logDebug( "remotefileroot", $remotefileroot );
# #   # my $remote = $self->setRunFiles( $remotefileroot );
# #   # my $local  = $self->setRunFiles( $localfileroot );
  

# # #   #### POLL FOR COMPLETION
# # #   my $success = $self->pollForCompletion( $processid );

# # # $self->logDebug( "success", $success );
# # # $self->logDebug( 'DEBUG EXIT' ) and exit;

# # #   #### RUN FILES
# # #   $self->downloadRunFiles( $profilehash, $remote, $local );

# # #   # #### EXIT CODE
# # #   # my $exitcode = $self->getExitCode( $local->{exitfile} );
# # #   # $self->logDebug( "exitcode", $exitcode );
 
# # #   #### IF success IS ZERO, SET STATUS TO 'completed'
# # #   #### OTHERWISE, SET STATUS TO 'error' 
# # #   $self->setFinalStatus( $success );
  
# # #   return $success;
}


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




} #### Stage

