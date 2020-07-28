use MooseX::Declare;

=head2

  PACKAGE    Engine::Local::Shell::Stage
  
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

class Engine::Local::Shell::Stage with (Engine::Common::Stage,
  Util::Logger, 
  Util::Timer) {

use Util::Profile;

#### SEE Engine::Common::Stage
#### EXTERNAL MODULES
#### INTERNAL MODULES
#### ATTRIBUTES
# Bool
# Int
# Str
# HashRef/ArrayRef
# Class/Object


method stop {
  $self->logCaller();

  my $workflowdata = {
    username => $self->username(),
    projectname => $self->projectname(),
    workflowname => $self->workflowname(),    
  };
  my $processid = $self->getProcessId( $workflowdata );
  $self->logDebug( "processid", $processid );

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

method run ( $dryrun ) {
=head2

  SUBROUTINE    run
  
  PURPOSE

    1. RUN THE STAGE APPLICATION AND UPDATE STATUS TO 'running'
    
    2. UPDATE THE PROGRESS FIELD PERIODICALLY (CHECKPROGRESS OR DEFAULT = 10 SECS)

    3. UPDATE STATUS TO 'complete' WHEN EXECUTED APPLICATION HAS FINISHED RUNNING
    
=cut

  $self->logCaller();

  my $profilehash = $self->profilehash();
  $self->logDebug( "profile", $profilehash );
  $self->logDebug("dryrun", $dryrun);
  
  #### TO DO: START PROGRESS UPDATER

  #### GENERATE OUTPUT FILE PATHS
  my $username = $self->username();
  $self->logDebug( "username", $username );
  my $fileroot = $self->util()->getFileroot( $username );
  $self->logDebug( "fileroot", $fileroot );
  my $runfiles = $self->setRunFiles( $fileroot );

  #### SET STAGE START TIME
  my $mysqltime     =    $self->getMysqlTime();
  $self->logDebug("mysqltime", $mysqltime);
  $self->started( $mysqltime );
  
  #### CLEAR STDOUT/STDERR FILES
  my $stdoutfile    =    $self->stdoutfile();
  File::Path::rmtree( $stdoutfile ) if -f $stdoutfile;
  my $stderrfile    =    $self->stderrfile();
  File::Path::rmtree( $stderrfile ) if -f $stderrfile;
  
  #### ADD out AND err FILES TO stage TABLE
  $self->saveRunFiles( $runfiles->{stdoutfile}, $runfiles->{stderrfile} );

  #### SET SYSTEM CALL TO POPULATE .sh SCRIPT
  my $systemcall = $self->setSystemCall( $profilehash, $runfiles );

  #### ADD out AND err FILES TO SYSTEM CALL
  $systemcall = $self->addOutputFiles( $systemcall, $runfiles->{stdoutfile}, $runfiles->{stderrfile} );

  #### PRINT COMMAND TO .sh SCRIPT
  my $command = join " \\\n", @$systemcall;
  $self->printScriptFile( $command, $runfiles->{scriptfile}, $runfiles->{exitfile}, $runfiles->{lockfile} );
  
  #### RUN
  my $commandfile = $runfiles->{scriptfile};
  $self->logDebug( "commandfile", $commandfile );

  # #### GET PID
  $self->logDebug("BEFORE SUBMIT command");
  my $processid = open my $fh, "-|", "/bin/bash $commandfile" or die $!;
  $self->logDebug( "processid", $processid );

  return $processid;
}

method addOutputFiles ( $systemcall, $stdoutfile, $stderrfile ) {
  my $redirection  =  $self->containsRedirection( $systemcall );
  $self->logDebug("redirection", $redirection);
  
  #### SET STDOUT AND STDERR FILES
  push @$systemcall, "1> $stdoutfile" if defined $stdoutfile and $redirection ne "stdout";
  push @$systemcall, "2> $stderrfile" if defined $stderrfile and $redirection ne "stderr";
  #$self->logDebug("$$ systemcall: @systemcall");

  return $systemcall;
}

method containsRedirection ($arguments) {
  return if not defined $arguments or not @$arguments;
  
  foreach my $argument ( @$arguments ) {
    return "stdout" if $argument eq ">" or $argument eq "1>";
    return "stderr" if $argument eq "2>";
  }
  
  return 0;
}


method mkdirCommand ($file) {
  $self->logDebug( "file", $file );
  my ( $directory )  =  $file  =~  /^(.+?)\/[^\/]+$/;
  
  File::Path::make_path( $directory );
}

method saveRunFiles ( $stdoutfile, $stderrfile ) {
=head2

  SUBROUTINE    saveRunFiles
  
  PURPOSE
  
    SET THE PROCESS IDS FOR:
    
      - THE STAGE ITSELF
      
      - THE PARENT OF THE STAGE'S APPLICATION (SAME AS STAGE)
    
      - THE CHILD OF THE STAGE'S APPLICATION
    
=cut
  $self->logDebug("$$ Engine::Stage::saveRunFiles()");

  my $username   = $self->username();
  my $projectname   = $self->projectname();
  my $workflowname   = $self->workflowname();
  my $workflownumber = $self->workflownumber();
  my $appnumber     = $self->appnumber();

  #### UPDATE status TO waiting IN TABLE stage
  my $query = qq{UPDATE stage
  SET
  stdoutfile='$stdoutfile',
  stderrfile='$stderrfile'
  WHERE username = '$username'
  AND projectname = '$projectname'
  AND workflowname = '$workflowname'
  AND workflownumber = '$workflownumber'
  AND appnumber = '$appnumber'};
  my $success = $self->table()->db()->do($query);
  if ( not $success )
  {
    $self->logDebug("$$ Could not insert entry for stage $self->stagenumber() into 'stage' table");
    return 0;
  }

  return 1;
}

method isComplete () {
=head2

  SUBROUTINE    isComplete
  
  PURPOSE

    CHECK IF THIS STAGE HAS STATUS 'complete' IN THE stage
    
  INPUT
  
    WORKFLOW NAME (workflowname) AND STAGE NAME (appname)
  
  OUTPUT
  
    RETURNS 1 IF COMPLETE, 0 IF NOT COMPLETE
  
=cut

  
  my $projectname = $self->projectname();
  my $workflowname = $self->workflowname();
  my $appnumber = $self->appnumber();

  my $query = qq{SELECT status
  FROM stage
  WHERE projectname='$projectname'
  AND workflowname = '$workflowname'
  AND appnumber = '$appnumber'
  AND status='completed'};
  $self->logDebug("$$ $query");
  my $complete = $self->table()->db()->query($query);
  $self->logDebug("$$ complete", $complete);
  
  return 0 if not defined $complete or not $complete;
  return 1;
}

method toString () {
  print $self->_toString();
}

method _toString () {
  my @keys = qw[ username projectname workflownumber workflowname appname appnumber start executor location fileroot outputdir scriptfile stdoutfile stderrfile workflowpid stagepid stagejobid submit setuid installdir cluster qsub qstat resultfile];
  my $string = '';
  foreach my $key ( @keys )
  {
    my $filler = " " x (20 - length($key));
    $string .= "$key$filler:\t";
    $string .= $self->$key() || '';
    $string .= "\n";
  }
  $string .= "\n\n";
}


#### ENVAR
  

} #### Engine::Stage

