#!/usr/bin/perl -w

BEGIN {
    my $installdir = $ENV{'installdir'} || "/a";
    unshift(@INC, "$installdir/extlib/lib/perl5");
    unshift(@INC, "$installdir/extlib/lib/perl5/x86_64-linux-gnu-thread-multi/");
    unshift(@INC, "$installdir/lib");
}




=head2

APPLICATION 	shepherd

PURPOSE

	1. Run a series of commands in the order received
	
	2. Run 'max' number of commands concurrently
	
	3. Poll running completed to determine which have complete
	
	4. Execute remaining commands up to 'max' number
	
	5. Repeat 2-4 until all commands are run

HISTORY

	v0.01	Basic loop with message queus

USAGE

$0 [--max Int] [--sleep Int] <--commands|--commandfile String>

max         :    Maximum number of commands to run concurrently
sleep       :    Number of seconds pause between polling commands
command     :    String of commands separated by line breaks
commandfile :    File containing list of commands (one-per-line)

=cut

#### EXTERNAL MODULES
use Term::ANSIColor qw(:constants);
use Data::Dumper;
use Getopt::Long;
use FindBin qw($Bin);

#### USE LIBRARY
use lib "$Bin/../../lib";	
my $installdir;
BEGIN {
    $installdir = $ENV{'installdir'} || "/a";
    unshift(@INC, "$installdir/extlib");
    unshift(@INC, "$installdir/lib");
}

use Engine::Logic::Shepherd::Queue;

##### STORE ARGUMENTS TO PRINT TO FILE LATER
my $arguments;
@$arguments = @ARGV;

my $configfile   =	"$installdir/conf/config.yml";
my $message;
my $max			=	0;
my $sleep		=	10;
my $commands;
my $commandfile;
my $log			=	2;
my $printlog	=	2;
my $logfile		=	"/tmp/agua-shepherd.log";
my $help;
GetOptions (
    'command=s@'  	=> \$commands,
    'commandfile=s'	=> \$commandfile,
    'message=s'  	=> \$message,
    'max=i'  		=> \$max,
    'sleep=i'  		=> \$sleep,
    'log=i'     	=> \$log,
    'printlog=i'    => \$printlog,
    'help'          => \$help
) or die "No options specified. Try '--help'\n";
usage() if defined $help;

#### SET CONF
my $conf = Conf::Yaml->new(
    memory      =>  0,
    inputfile   =>  $configfile,
    backup      =>  1,

    log			=>	$log,
    printlog	=>	$printlog,
    logfile     =>  $logfile
);


my $object = Engine::Logic::Shepherd::Queue->new({
    commands    =>  $commands,
	commandfile	=>	$commandfile,
    max			=>	$max,
    sleep		=>	$sleep,
	conf		=>	$conf,
    log			=>	$log,
    printlog	=>	$printlog,
    logfile     =>  $logfile
});

#$object->openConnection();
#$object->openTaskQueue();
#$object->sendTask($message);
$object->handleTasks();

exit;

#$commands	=	$object->commands() if not defined $commands;
#my $outputs = $object->run();
#for ( my $i = 0; $i < @$outputs; $i++ ) {
#	my $output = $$outputs[$i];
#	print "[command $i] $$commands[$i]\n";
#	print "$output\n\n";
#}
#
#exit;

##############################################################

sub usage {
	print `perldoc $0`;
	exit;
}
