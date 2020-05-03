package Engine::Common::Stage;
use Moose::Role;
use Method::Signatures::Simple;

=head2

  PACKAGE  Engine::Stage
  
  PURPOSE:
  
  A Stage IS ONE STEP IN A WORKFLOW.
  
  IT HAS THE FOLLOWING CHARACTERISTICS:
  
  1. EACH Stage RUNS ITSELF AND LOGS ITS
  
    STATUS TO THE stage DATABASE TABLE.
  
  2. A Stage WILL RUN LOCALLY BY DEFAULT.
  
  3. IF THE submit VARIABLE IS NOT ZERO AND
  
    cluster VARIABLE IS NOT EMPTY, IT
    
    WILL RUN ON A CLUSTER.
  
  3. EACH Stage DYNAMICALLY SETS ITS
  
    STDOUT, STDERR, INPUT AND OUTPUT
  
    FILES.
  
=cut 

use strict;
use warnings;

#### EXTERNAL MODULES
use IO::Pipe;
use Data::Dumper;
use FindBin qw($Bin);

#### USE LIB FOR INHERITANCE
use lib "$Bin/../";

#### INTERNAL MODULES
use Engine::Envar;

# Booleans

# Int/Nums
has 'appnumber'      =>  ( isa => 'Str', is => 'rw');
has 'ancestor'       =>  ( isa => 'Str|Undef', is => 'rw');
has 'successor'      =>  ( isa => 'Str|Undef', is => 'rw');
has 'workflownumber' =>  ( isa => 'Str', is => 'rw');
has 'start'          =>  ( isa => 'Int', is => 'rw' );
has 'slots'          =>  ( isa => 'Int|Undef', is => 'rw' );
has 'maxjobs'        =>  ( isa => 'Int|Undef', is => 'rw' );
has 'runsleep'       =>  ( isa => 'Num|Undef', is => 'rw', default => 0.5 );

# Str
has 'profile'        =>  ( isa => 'Str|Undef', is => 'rw' );
has 'submit'         =>  ( isa => 'Str|Undef', is => 'rw' );
has 'workflowpid'    =>  ( isa => 'Str|Undef', is => 'rw' );
has 'stagepid'       =>  ( isa => 'Str|Undef', is => 'rw' );
has 'stagejobid'     =>  ( isa => 'Str|Undef', is => 'rw' );
has 'username'       =>  ( isa => 'Str', is => 'rw', required => 1  );
has 'workflowname'   =>  ( isa => 'Str', is => 'rw', required => 1  );
has 'projectname'    =>  ( isa => 'Str', is => 'rw', required => 1  );
has 'appname'        =>  ( isa => 'Str', is => 'rw', required => 1  );
has 'apptype'        =>  ( isa => 'Str', is => 'rw', required => 1  );
has 'outputdir'      =>  ( isa => 'Str', is => 'rw', required => 1  );
has 'scriptfile'     =>  ( isa => 'Str', is => 'rw', required => 0 );
has 'installdir'     =>  ( isa => 'Str', is => 'rw', required => 1  );
has 'version'        =>  ( isa => 'Str', is => 'rw', required => 1  );
has 'scheduler'      =>  ( isa => 'Str|Undef', is => 'rw', default  =>  "local" );

has 'fileroot'       =>  ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'userhome'       =>  ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'executor'       =>  ( isa => 'Str|Undef', is => 'rw', default => undef );
has 'prescript'      =>  ( isa => 'Str|Undef', is => 'rw', default => undef );
has 'location'       =>  ( isa => 'Str|Undef', is => 'rw', default => '' );

has 'setuid'         =>  ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'queue_options'  =>  ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'requestor'      =>  ( isa => 'Str', is => 'rw', required  =>  0  );
has 'stdoutfile'     =>  ( isa => 'Str', is => 'rw' );
has 'stderrfile'     =>  ( isa => 'Str', is => 'rw' );
has 'started'        =>  ( isa => 'Str', is => 'rw' );
has 'completed'      =>  ( isa => 'Str', is => 'rw' );

# Hash/Array
has 'profilehash'    =>  ( isa => 'HashRef|Undef', is => 'rw' );
has 'fields'         =>  ( 
  isa => 'ArrayRef[Str|Undef]', 
  is => 'rw', 
  default => sub { 
    ['owner', 'appname', 'appnumber', 'apptype', 'location', 'submit', 'executor', 'prescript', 'description', 'notes'] 
  } );

# has 'envarsub'       => ( 
#   isa => 'Maybe', 
#   is => 'rw', 
#   lazy => 1, 
#   builder => "setEnvarsub" 
# );

# method setEnvarsub {
#   return *_envarSub;
# }
  
# method _envarSub ($envars, $values, $parent) {
#   $self->logDebug("parent: $parent");
#   $self->logDebug("envars", $envars);
#   $self->logDebug("values", $values);
#   #$self->logDebug("SELF->CONF", $self->conf());
  
#   my $profile = $self->profile();

#   #### SET USERNAME AND CLUSTER IF NOT DEFINED
#   if ( not defined $values->{sgeroot} ) {
#     $values->{sgeroot} = $profile->{scheduler}->{sgeroot};
#   }
  
#   #### SET CLUSTER
#   if ( not defined $values->{cluster} and defined $values->{sgecell}) {
#     $values->{cluster} = $values->{sgecell};
#   }
  
#   #### SET QMASTERPORT
#   if ( not defined $values->{qmasterport}
#     or (
#       defined $values->{username}
#       and $values->{username}
#       and defined $values->{cluster}
#       and $values->{cluster}
#       and defined $self->table()->db()
#       and defined $self->table()->db()->dbh()      
#     )
#   ) {
#     $values->{qmasterport} = $profile->{scheduler}->{qmasterport};
#     $values->{execport}    = $profile->{scheduler}->{execport};
#     $self->logDebug("values", $values);
#   }
  
#   $values->{queue} = $parent->setQueueName($values);
#   $self->logDebug("values", $values);
  
#   return $self->values($values);  
# }

# has 'customvars'=>  ( isa => 'HashRef', is => 'rw', default => sub {
#   return {
#     cluster     =>   "CLUSTER",
#     qmasterport   =>   "SGE_MASTER_PORT",
#     execdport     =>   "SGE_EXECD_PORT",
#     sgecell     =>   "SGE_CELL",
#     sgeroot     =>   "SGE_ROOT",
#     queue       =>   "QUEUE"
#   };
# });

# Object
has 'conf'           => ( isa => 'Conf::Yaml', is => 'rw', required => 1 );

has 'db'             => ( isa => 'Any', is => 'rw', required => 0 );
# has 'monitor'    =>   ( isa => 'Maybe', is => 'rw', required => 0 );
has 'stageparameters'=> ( isa => 'ArrayRef', is => 'rw', required => 1 );
has 'samplehash'     =>  ( isa => 'HashRef|Undef', is => 'rw', required => 0  );

has 'util'           =>  (
  is       =>  'rw',
  isa      =>  'Util::Main',
  lazy     =>  1,
  builder  =>  "setUtil"
);

method setUtil () {
  my $util = Util::Main->new({
    conf      =>  $self->conf(),
    log        =>  $self->log(),
    printlog  =>  $self->printlog()
  });

  $self->util($util);  
}

has 'envar'  => ( 
  is => 'rw',
  isa => 'Envar',
  lazy => 1,
  builder => "setEnvar" 
);

method setEnvar {
  my $customvars  =  $self->can("customvars") ? $self->customvars() : undef;
  my $envarsub  =  $self->can("envarsub") ? $self->envarsub() : undef;
  $self->logDebug("customvars", $customvars);
  $self->logDebug("envarsub", $envarsub);
  
  my $envar = Envar->new({
    db      =>  $self->table()->db(),
    conf    =>  $self->conf(),
    customvars  =>  $customvars,
    envarsub  =>  $envarsub,
    parent    =>  $self
  });
  
  $self->envar($envar);
}

has 'table'    =>  (
  is       =>  'rw',
  isa     =>  'Table::Main',
  lazy    =>  1,
  builder  =>  "setTable"
);


method BUILD ($args) {
  #$self->logDebug("$$ Stage::BUILD  args:");
  #$self->logDebug("$$ args", $args);
}

method getField ($field) {
  my $username  =  $self->username();
  my $projectname  =  $self->projectname();
  my $workflowname  =  $self->workflowname();
  my $appnumber  =  $self->appnumber();

  my $query = qq{SELECT $field
FROM stage
WHERE username='$username'
AND projectname='$projectname'
AND workflowname='$workflowname'
AND appnumber='$appnumber'};
  #$self->logDebug("query", $query);
  my $successor = $self->table()->db()->query($query);
  #$self->logDebug("successor", $successor);
  
  return $successor;  
}

method getSuccessor {
  return $self->getField("successor");
}

method getAncestor {
  return $self->getField("successor");
}

method getStatus {
  return $self->getField("status");
}

method setRunningStatus {
  my $now = $self->table()->db()->now();
  my $set = qq{status='running',
started=$now,
queued=$now,
completed='0000-00-00 00:00:00'};
  $self->setFields($set);
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
  #### ADD PERL5LIB TO ENABLE EXTERNAL SCRIPTS TO USE OUR MODULES
  my $installdir = $self->conf()->getKey("core:INSTALLDIR");
  my $perl5lib = $ENV{"PERL5LIB"};
  $self->logDebug( "installdir: $installdir" );
  $self->logDebug( "perl5lib: $perl5lib" );


  $self->logDebug("$$ fileroot", $fileroot);

  my $stageparameters =  $self->stageparameters();
  $self->logDebug( "stageparameters", $stageparameters );
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
  
  return $systemcall;  
}

method setRunFiles ( $fileroot ) {
  my $projectname  = $self->projectname();
  my $workflowname = $self->workflowname();
  my $stagenumber  = $self->appnumber();
  my $stagename    = $self->appname();
  $self->logDebug("fileroot", $fileroot);
  # $self->logDebug("projectname", $projectname);
  # $self->logDebug("workflowname", $workflowname);
  # $self->logDebug("stagenumber", $stagenumber);
  # $self->logDebug("stagename", $stagename);

  my $scriptdir = "$fileroot/$projectname/$workflowname/scripts";
  $self->logDebug("scriptdir", $scriptdir);

  #### SET FILE STUB - ADD samplename IF DEFINED
  my $filestub = "$stagenumber-$stagename";
  if ( $self->samplehash() and $self->samplehash()->{samplename} ) { 
    my $samplename = $self->samplehash()->{samplename};
    $filestub    =    "$stagenumber-$stagename-$samplename";
  }
  $self->logDebug( "filestub", $filestub );

  my $files = {
    scriptfile => "sh",
    stdoutfile => "out", 
    stderrfile => "err",
    exitfile   => "exit",
    lockfile   => "lock",
    usagefile  => "usage"
  };

  my $runfiles = {};
  foreach my $key ( keys %$files ) {
    $runfiles->{ $key } = "$scriptdir/$filestub." . $files->{ $key };
  }
  $self->logDebug( "runfiles", $runfiles );

  return $runfiles;
}

method setUsagefile( $usagefile ) {
  #### LINUX OR WINDOS GITBASH
  my $usage = qq{/usr/bin/time \\
 -o $usagefile \\
 -f "%Uuser %Ssystem %Eelapsed %PCPU (%Xtext+%Ddata %Mmax)k"};

  #### OSX
  my $os = $^O;
  if ( $os eq "darwin" ) {
    $usage    =  "/usr/bin/time";
  }

  return $usage;
}

method getPrescript ( $profilehash, $userhome, $fileroot, $projectname, $workflowname, $installdir, $basedir ) {

  my $prescript    =  $self->prescript() || "";
  $self->logDebug( "prescript", $prescript );

  if ( defined $prescript and $prescript ne "" ) {
    $prescript  =  $self->replaceTags( $prescript, $profilehash, $userhome, $fileroot, $projectname, $workflowname, $installdir, $basedir );

    my @scripts = split ",", $prescript;
    $prescript = "";
    foreach my $script ( @scripts ) {
      if ( ($script) =~ s/^file:// ) {
        $self->logDebug("script", $script);
        $prescript  .=  $self->parsePreScriptFile( $script );
      }
      else {
        $prescript .= $script;      
      }
      $prescript =~ s/[;\s]*$//g;
      $prescript .= ";";
    }

    $self->logDebug("prescript", $prescript);
  }

  return $prescript;
}

method parsePreScriptFile ($file) {
  $self->logDebug("file", $file);
  open(FILE, $file) or die "Can't open file: $file: $!";

  my $exports  =  "";
  while ( <FILE> ) {
    next if $_  =~ /^#/ or $_ =~ /^\s*$/;
    chomp;
    $exports .= "$_; ";
  }

  return $exports;
}

method replaceTags ( $string, $profilehash, $userhome, $fileroot, $projectname, $workflowname, $installdir, $basedir ) {
  # $self->logCaller();
  # $self->logDebug( "string", $string ); 
  # $self->logDebug( "profilehash", $profilehash ); 
  # $self->logDebug( "installdir", $installdir );
  
  while ( $string =~ /<profile:([^>]+)>/ ) {
    my $keystring = $1;
    # $self->logDebug( "string", $string );
    my $value = $self->getProfileValue( $keystring, $profilehash );
    # $self->logDebug( "value", $value );

    $string =~ s/<profile:$keystring>/$value/ if $value;
  }
 
	my $flowhome = $ENV{'FLOW_HOME'};
  $string  =~  s/<BASEDIR>/$basedir/g;
  $string  =~  s/<USERHOME>/$userhome/g;
  $string  =~  s/<FILEROOT>/$fileroot/g;
  $string  =~  s/<PROJECT>/$projectname/g;
  $string  =~  s/<WORKFLOW>/$workflowname/g;
  $string  =~  s/<WORKFLOWDIR>/$fileroot\/$projectname\/$workflowname/g;
  $string  =~  s/<INSTALLDIR>/$installdir/g;
  $string  =~  s/<FLOW_HOME>/$flowhome/g;

  $self->logDebug( "string", $string );

  return $string;
}

method getProfileValue ( $keystring, $profilehash ) {
  $self->logDebug( "keystring", $keystring );
  my @keys = split ":", $keystring;
  my $hash = $profilehash;
  foreach my $key ( @keys ) {
    $hash  = $hash->{$key};
    return undef if not defined $hash;
    # $self->logDebug("hash", $hash);
  }

  return $hash;
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
#

method printScriptFile ( $command, $scriptfile, $exitfile, $lockfile ) {
  $self->logDebug("scriptfile", $scriptfile);

  #### CREATE DIR COMMANDS
  $self->mkdirCommand($scriptfile);

  my $contents  =  qq{#!/bin/bash

echo "-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*"
echo USERNAME:        \$USERNAME
echo PROJECT:         \$PROJECT
echo WORKFLOW:        \$WORKFLOW
echo QUEUE:         \$QUEUE
echo "-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*"

hostname -f
date

# OPEN LOCKFILE
date > $lockfile

$command

#### REMOVE LOCKFILE
echo \$? > $exitfile

# REMOVE LOCKFILE
unlink $lockfile;

exit 0;

};
  $self->logDebug("contents", $contents);



# $self->logDebug( "DEBUG EXIT" ) and exit;




  open(OUT, ">$scriptfile") or die "Can't open script file: $scriptfile\n";
  print OUT $contents;
  close(OUT);
  chmod(0777, $scriptfile);
  $self->logNote("scriptfile printed", $scriptfile);
}

method mkdirCommand ($file) {
  my ($dir)  =  $file  =~  /^(.+?)\/[^\/]+$/;
  my $command  =  "mkdir -p $dir";
  #$self->logDebug("command", $command);
  
  `$command`;
}

method setArguments ( $stageparameters ) {
#### SET ARGUMENTS AND GET VALUES FOR ALL PARAMETERS
  $self->logNote("stageparameters", $stageparameters);
  $self->logNote("no. stageparameters: " . scalar(@$stageparameters));

  #### SANITY CHECK
  return if not defined $stageparameters;
  return if ref($stageparameters) eq '';

  #### GET FILEROOT
  my $username   = $self->username();
  my $version   = $self->version();
  my $fileroot   = $self->fileroot();
  my $userhome   = $self->userhome();
  $self->logNote("username", $username);
  $self->logNote("fileroot", $fileroot);
  $self->logNote("version", $version);
  
  #### SORT BY ORDINALS
  @$stageparameters = sort { $a->{ordinal} <=> $b->{ordinal} } @$stageparameters;
  #$self->logNote("SORTED stageparameters", $stageparameters);
  
  #### GENERATE ARGUMENTS ARRAY
  my $arguments = [];
  foreach my $stageparameter ( @$stageparameters ) {
    my $paramname  =  $stageparameter->{paramname};
    my $argument   =  $stageparameter->{argument};
    my $value     =  $stageparameter->{value};
    my $valuetype   =  $stageparameter->{valuetype};
    my $discretion   =  $stageparameter->{discretion};
    my $projectname    =  $stageparameter->{projectname};
    my $workflowname  =  $stageparameter->{workflowname};    
    my $samplehash  =  $self->samplehash();
    # $self->logDebug("samplehash", $samplehash);

    $value  =~  s/<USERHOME>/$userhome/g;
    $value  =~  s/<FILEROOT>/$fileroot/g;
    $value  =~  s/<PROJECT>/$projectname/g;
    $value  =~  s/<WORKFLOW>/$workflowname/g;
    $value  =~  s/<VERSION>/$version/g if defined $version;
    $value  =~  s/<USERNAME>/$username/g if defined $username;
    $value  =~  s/<USER>/$username/g if defined $username;

    if ( defined $samplehash ) {
      foreach my $key ( keys %$samplehash ) {
        my $match  =  uc($key);
        my $mate  =  $samplehash->{$key};
        #$self->logNote("key", $key);
        #$self->logNote("match", $match);
        #$self->logNote("mate", $mate);
        #$self->logNote("DOING MATCH $match / $mate");
        $value  =~  s/<$match>/$mate/g;
        #$self->logNote("AFTER MATCH value: $value");
      }
    }
  
    $self->logNote("paramname", $paramname);
    $self->logNote("argument", $argument);
    $self->logNote("value", $value);
    $self->logNote("valuetype", $valuetype);
    $self->logNote("discretion", $discretion);

    #### SKIP EMPTY FLAG OR ADD 'checked' FLAG
    if ( $valuetype eq "flag" ) {
      if (not defined $argument or not $argument) {
        $self->logNote("Skipping empty flag", $argument);
        next;
      }

      push @$arguments, $argument;
      next;
    }
    
    if ( $value =~ /^\s*$/ and $discretion ne "required" ) {
      $self->logNote("Skipping empty argument", $argument);
      next;
    }
    
    if ( defined $value )  {
      $self->logNote("BEFORE value", $value);

      #### ADD THE FILE ROOT FOR THIS USER TO FILE/DIRECTORY PATHS
      #### IF IT DOES NOT BEGIN WITH A '/', I.E., AN ABSOLUTE PATH
      if ( $valuetype =~ /^(file|directory)$/ and $value =~ /^[^\/]/ ) {  
        $self->logNote("Adding fileroot to $valuetype", $value);
        $value =~ s/^\///;
        $value = "$fileroot/$value";
      }

      #### ADD THE FILE ROOT FOR THIS USER TO FILE/DIRECTORY PATHS
      #### IF IT DOES NOT BEGIN WITH A '/', I.E., AN ABSOLUTE PATH
      if ( $valuetype =~ /^(files|directories)$/ and $value =~ /^[^\/]/ ) {  
        $self->logNote("Adding fileroot to $valuetype", $value);
        my @subvalues = split ",", $value;
        foreach my $subvalue ( @subvalues ) {
          $subvalue =~ s/^\///;
          $subvalue = "$fileroot/$subvalue";
        }
        
        $value = join ",", @subvalues;
      }

      #### 'X=' OPTIONS
      if ( $argument =~ /=$/ ) {
        push @$arguments, qq{$argument$value};
      }

      #### '-' OPTIONS (E.G., -i)
      elsif ( $argument =~ /^\-[^\-]/ ) {
        push @$arguments, qq{$argument $value};
      }
      
      #### DOUBLE '-' OPTIONS (E.G., --inputfile)
      else {
        push @$arguments, $argument if defined $argument and $argument ne "";
        push @$arguments, $value;
      }

      $self->logNote("AFTER value", $value);
      $self->logNote("current arguments", $arguments);
    }
  }

  $self->logNote("arguments", $arguments);

  return $arguments;
}

method registerRunInfo {
=head2

  SUBROUTINE    registerRunInfo
  
  PURPOSE
  
    SET THE PROCESS IDS FOR:
    
      - THE STAGE ITSELF
      
      - THE PARENT OF THE STAGE'S APPLICATION (SAME AS STAGE)
    
      - THE CHILD OF THE STAGE'S APPLICATION
    
=cut
  $self->logDebug("$$ Engine::Stage::registerRunInfo()");

  my $workflowpid = $self->workflowpid();
  my $stagepid   = $self->stagepid() || '';
  my $stagejobid = $self->stagejobid() || '';
  my $username   = $self->username();
  my $projectname   = $self->projectname();
  my $workflowname   = $self->workflowname();
  my $workflownumber = $self->workflownumber();
  my $appnumber     = $self->appnumber();
  my $stdoutfile     = $self->stdoutfile();
  my $stderrfile     = $self->stderrfile();
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

method isComplete {
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

method setRunTimes ($jobid) {
  $self->logDebug("$$ Stage::setRunTimes(jobid)");
  $self->logDebug("$$ jobid", $jobid);
  my $username = $self->username();
  my $cluster = $self->cluster();
  my $qacct = $self->monitor()->qacct($username, $cluster, $jobid);
  $self->logDebug("$$ qacct", $qacct);

  return if not defined $qacct or not $qacct;
  return if $qacct =~ /^error: job id \d+ not found/;

  #### QACCT OUTPUT FORMAT:
  #  qsub_time  Sat Sep 24 01:05:17 2011
  #  start_time   Sat Sep 24 01:05:24 2011
  #  end_time   Sat Sep 24 01:05:24 2011

  my ($queued) = $qacct =~ /qsub_time\s+([^\n]+)/ms;
  my ($started) = $qacct =~ /start_time\s+([^\n]+)/ms;
  my ($completed) = $qacct =~ /end_time\s+([^\n]+)/ms;
  $self->logDebug("queued", $queued);
  $self->logDebug("started", $started);
  $self->logDebug("completed", $completed);

  $queued = $self->datetimeToMysql($queued);
  $started = $self->datetimeToMysql($started);
  $completed = $self->datetimeToMysql($completed);
  
  my $set = qq{
queued = '$queued',
started = '$started',
completed = '$completed'};
  $self->logDebug("$$ set", $set);

  $self->setFields($set);
}
method setStatus ($status) {  
#### SET THE status FIELD IN THE stage TABLE FOR THIS STAGE
  $self->logDebug("status", $status);

  #### GET TABLE KEYS
  my $username = $self->username();
  my $projectname = $self->projectname();
  my $workflowname = $self->workflowname();
  my $appnumber = $self->appnumber();
  my $completed = "completed=" . $self->table()->db()->now();
  # my $completed = "completed = DATE_SUB(NOW(), INTERVAL 3 SECOND)";
  $completed = "completed=''" if $status eq "running";
  
  my $query = qq{UPDATE stage
SET
status = '$status',
$completed
WHERE username = '$username'
AND projectname = '$projectname'
AND workflowname = '$workflowname'
AND appnumber = '$appnumber'};
  # $self->logNote("$query");
  my $success = $self->table()->db()->do($query);
  if ( not $success )
  {
    $self->logError("Can't update stage (project: $projectname, workflow: $workflowname, number: $appnumber) with status: $status");
    exit;
  }
}

method setQueued {
  $self->logDebug("$$ Stage::setQueued(set)");
  my $now = $self->table()->db()->now();
  my $set = qq{
status    =  'queued',
started   =   '',
queued     =   $now,
completed   =   ''};
  $self->setFields($set);
}

method setRunning {
  $self->logDebug("$$ Stage::setRunning(set)");
  my $now = $self->table()->db()->now();
  my $set = qq{
status    =  'running',
started   =   $now,
completed   =   ''};
  $self->setFields($set);
}

method setFields ($set) {
  #$self->logDebug("set", $set);

  #### GET TABLE KEYS
  my $username   =   $self->username();
  my $projectname   =   $self->projectname();
  my $workflowname   =   $self->workflowname();
  my $appnumber     =   $self->appnumber();
  my $now     =   $self->table()->db()->now();

  my $query = qq{UPDATE stage
SET $set
WHERE username = '$username'
AND projectname = '$projectname'
AND workflowname = '$workflowname'
AND appnumber = '$appnumber'};  
  #$self->logDebug("$query");
  my $success = $self->table()->db()->do($query);
  $self->logError("Could not set fields for stage (project: $projectname, workflow: $workflowname, number: $appnumber) set : '$set'") and exit if not $success;
}

method setStagePid ($stagepid) {
  $self->logDebug("stagepid", $stagepid);
  
  #### GET TABLE KEYS
  my $username   = $self->username();
  my $projectname   = $self->projectname();
  my $workflowname   = $self->workflowname();
  my $appnumber     = $self->appnumber();
  my $now     = $self->table()->db()->now();
  my $query = qq{UPDATE stage
SET
stagepid = '$stagepid'
WHERE username = '$username'
AND projectname = '$projectname'
AND workflowname = '$workflowname'
AND appnumber = '$appnumber'};
  $self->logDebug("$query");
  my $success = $self->table()->db()->do($query);
  $self->logDebug("success", $success);
  $self->logError("Could not update stage table with stagepid: $stagepid") and exit if not $success;
}

method toString () {
  print $self->_toString();
}

method _toString () {
  my @keys = qw[ username projectname workflownumber workflowname appname appnumber start executor location fileroot queue queue_options outputdir scriptfile stdoutfile stderrfile workflowpid stagepid stagejobid submit setuid installdir cluster qsub qstat resultfile];
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


1;


# method setStageJob {
#   #$self->logCaller("");

#   #### CLUSTER MONITOR
#   my $monitor    =  $self->monitor();  
#   #### GET MAIN PARAMS
#   my $username   = $self->username();
#   my $projectname   = $self->projectname();
#   my $workflownumber   = $self->workflownumber();
#   my $workflowname   = $self->workflowname();
#   my $appnumber     = $self->appnumber();
#   my $queue     = $self->queue();
#   my $cluster    = $self->cluster();
#   my $qstat    = $self->qstat();
#   my $qsub    = $self->qsub();
#   my $workflowpid = $self->workflowpid();
#   #$self->logDebug("$$ cluster", $cluster);

#   #### SET DEFAULTS
#   $queue = '' if not defined $queue;

#   #### GET FILE ROOT
#   my $fileroot = $self->util()->getFileroot($username);

#   #### GET ARGUMENTS ARRAY
#   my $stageparameters =  $self->stageparameters();
#   #$self->logDebug("$$ Arguments", $stageparameters);
#   $stageparameters =~ s/\'/"/g;
#   my $arguments = $self->setArguments($stageparameters);  

#   #### GET PERL5LIB FOR EXTERNAL SCRIPTS TO FIND Agua MODULES
#   my $installdir = $self->conf()->getKey("core:INSTALLDIR");
#   my $perl5lib = $ENV{"PERL5LIB"};
  
#   #### SET EXECUTOR
#   my $executor  .=  "export PERL5LIB=$perl5lib; ";
#   $executor     .=   $self->executor() if defined $self->executor();
#   #$self->logDebug("$$ self->executor(): " . $self->executor());

#   #### SET APPLICATION
#   my $application = $self->installdir() . "/" . $self->location();  
#   #$self->logDebug("$$ application", $application);

#   #### ADD THE INSTALLDIR IF THE LOCATION IS NOT AN ABSOLUTE PATH
#   #$self->logDebug("$$ installdir", $installdir);
#   if ( $application !~ /^\// and $application !~ /^[A-Z]:/i ) {
#     $application = "$installdir/bin/$application";
#     #$self->logDebug("$$ Added installdir to stage_arguments->{location}: " . $application);
#   }

#   #### SET SYSTEM CALL
#   my @systemcall = ($application, @$arguments);
#   my $command = "$executor @systemcall";
  
#   #### GET OUTPUT DIR
#   my $outputdir = $self->outputdir();
#   #$self->logDebug("$$ outputdir", $outputdir);

#   #### SET JOB NAME AS projectname-workflowname-appnumber
#   my $label =  $projectname;
#   $label .= "-" . $workflownumber;
#   $label .= "-" . $workflowname;
#   $label .= "-" . $appnumber;
#   #$self->logDebug("$$ label", $label);
  
#   my $samplehash  =  $self->samplehash();
#   $self->logNote("samplehash", $samplehash);
#   if ( defined $samplehash ) {
#     my $id    =  $samplehash->{sample};
#     $label    =  "$id.$label";
#   }

#   #### SET JOB 
#   return $self->setJob([$command], $label, $outputdir);
# }
