use MooseX::Declare;

=head2

  PACKAGE    Engine::Workflow
  
  PURPOSE
  
    THE Workflow OBJECT PERFORMS THE FOLLOWING TASKS:
    
      1. SAVE WORKFLOWS
      
      2. RUN WORKFLOWS
      
      3. PROVIDE WORKFLOW STATUS

=cut

use strict;
use warnings;
use Carp;

class Engine::Workflow {

#### EXTERNAL MODULES
use FindBin qw($Bin);
use Data::Dumper;

##### INTERNAL MODULES  

# Bool
# Int
# String
# Object

sub new {
  my $class     = shift;
  my $hosttype  = shift;
  my $runtype   = shift;
  my $args      = shift;

  my $modulepath =  $INC{"Engine/Workflow.pm"};
  my ($path) = $modulepath =~ /^(.+?)\/[^\/]+.pm$/; 


  $hosttype = cowCase( $hosttype );
  $runtype = cowCase( $runtype );
  # print "Engine::Workflow    runtype: $runtype\n";
  # print "Engine::Workflow    hosttype: $hosttype\n";

  my $location    = "$path/$hosttype/$runtype/Workflow.pm";
  $class          = "Engine::" . $hosttype . "::" . $runtype . "::Workflow";
  require $location;

  return $class->new( $args );
}

sub cowCase {
  my $string = shift;

  return uc( substr( $string, 0, 1) ) . substr( $string, 1);
}


Engine::Workflow->meta->make_immutable(inline_constructor => 0);

}  #### class
