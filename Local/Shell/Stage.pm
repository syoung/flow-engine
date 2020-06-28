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

#### SEE Engine::Common::Stage
#### EXTERNAL MODULES
#### INTERNAL MODULES
#### ATTRIBUTES
# Bool
# Int
# Str
# HashRef/ArrayRef
# Class/Object

method BUILD ( $args ) {
  #$self->logDebug("$$ Stage::BUILD  args:");
  #$self->logDebug("$$ args", $args);
}

method run ( $dryrun ) {
=head2

  SUBROUTINE    run
  
  PURPOSE

    1. RUN THE STAGE APPLICATION AND UPDATE STATUS TO 'running'
    
    2. UPDATE THE PROGRESS FIELD PERIODICALLY (CHECKPROGRESS OR DEFAULT = 10 SECS)

    3. UPDATE STATUS TO 'complete' WHEN EXECUTED APPLICATION HAS FINISHED RUNNING
    
=cut

  my $profilehash = $self->profilehash();
  $self->logDebug( "profile", $profilehash );
  $self->logDebug("dryrun", $dryrun);
  
  #### TO DO: START PROGRESS UPDATER

  #### GENERATE OUTPUT FILE PATHS
  my $fileroot = $self->fileroot();
  my $runfiles = $self->setRunFiles( $fileroot );

  #### REGISTER PROCESS IDS SO WE CAN MONITOR THEIR PROGRESS
  $self->registerRunInfo( $runfiles->{stdoutfile}, $runfiles->{stderrfile} );

  #### SET SYSTEM CALL TO POPULATE RUN SCRIPT
  my $systemcall = $self->setSystemCall( $profilehash, $runfiles );

  #### ADD STDOUT AND STDERR FILES TO SYSTEM CALL
  $systemcall = $self->addOutputFiles( $systemcall, $runfiles->{stdoutfile}, $runfiles->{stderrfile} );

  #### PRINT COMMAND TO .sh FILE
  my $command = join " \\\n", @$systemcall;
  $self->printScriptFile( $command, $runfiles->{scriptfile}, $runfiles->{exitfile}, $runfiles->{lockfile} );
  
  #### UPDATE STATUS TO 'running'
  my $now = $self->table()->db()->now();
  $self->setStageRunning( $now );

  #### RUN
  $self->logDebug("BEFORE SUBMIT command");
  my $commandfile = $runfiles->{scriptfile};
  $self->logDebug( "commandfile", $commandfile );

  # #### GET PID
  # push @$systemcall, ' & echo $!';  
  # my $pid = `$commandfile`;
  my $processid = open my $fh, "-|", "/bin/bash $commandfile" or die $!;
  $self->logDebug( "processid", $processid );

  #### ADD processid TO stage TABLE
  my $stagedata = $self->toData();
  $stagedata->{ processid } = $processid;

  $self->table()->updateJobs( $stagedata );



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

method registerRunInfo ( $stdoutfile, $stderrfile ) {
=head2

  SUBROUTINE    registerRunInfo
  
  PURPOSE
  
    SET THE PROCESS IDS FOR:
    
      - THE STAGE ITSELF
      
      - THE PARENT OF THE STAGE'S APPLICATION (SAME AS STAGE)
    
      - THE CHILD OF THE STAGE'S APPLICATION
    
=cut
  $self->logDebug("$$ Engine::Stage::registerRunInfo()");

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

method register ( $status ) {
#### SET STATUS TO waiting FOR A STAGE IN THE stage TABLE
  $self->logDebug("PID $$");
  
  #### SET SELF _status TO waiting
  $self->status('waiting');

  my $username = $self->username();
  my $projectname = $self->projectname();
  my $workflowname = $self->workflowname();
  my $workflownumber = $self->workflownumber();
  my $appnumber = $self->appnumber();

  #### UPDATE status TO waiting IN TABLE stage
  my $query = qq{UPDATE stage
  SET status='waiting'
  WHERE username = '$username'
  AND projectname = '$projectname'
  AND workflowname = '$workflowname'
  AND workflownumber = '$workflownumber'
  AND appnumber = '$appnumber'};
  $self->logDebug("$$ $query");
  my $success = $self->table()->db()->do($query);
  $self->logDebug("$$ insert success", $success);
  if ( not $success )
  {
    warn "Stage::register  Could not insert entry for stage $self->stagenumber() into 'stage' table\n";
    return 0;
  }

  $self->logDebug("$$ Successful insert!");
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


method initialiseRunTimes ($mysqltime) {
  $self->logDebug("mysqltime", $mysqltime);
  my $set = qq{
queued = '$mysqltime',
started = '$mysqltime',
completed = ''};
  #$self->logDebug("$$ set", $set);

  $self->setFields($set);
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

