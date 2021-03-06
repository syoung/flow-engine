use MooseX::Declare;

=head2

	PACKAGE		Test::Engine::Stage

    PURPOSE
    
        TEST Engine::Stage METHODS

=cut

class Test::Engine::Stage extends Engine::Stage {

use FindBin qw($Bin);
use Test::More;

#### INTERNAL MODULES
use DBase::Factory;
use Conf::Yaml;

#### EXTERNAL MODULES
use Data::Dumper;
use File::Path;
use Getopt::Simple;

has 'envars'		=>	( isa => 'Str|Undef', is => 'rw' );
has 'installdir'	=>	( isa => 'Str|Undef', is => 'rw' );
has 'name'			=>	( isa => 'Str|Undef', is => 'rw' );
has 'outputdir'		=>	( isa => 'Str|Undef', is => 'rw' );
has 'project'		=>	( isa => 'Str|Undef', is => 'rw' );
has 'workflow'		=>	( isa => 'Str|Undef', is => 'rw' );
has 'queue'			=>	( isa => 'Str|Undef', is => 'rw' );
has 'scriptfile'	=>	( isa => 'Str|Undef', is => 'rw' );
has 'username'		=>	( isa => 'Str|Undef', is => 'rw' );
has 'stageparameters'=> ( isa => 'ArrayRef|Undef', is => 'rw' );


####/////}}

method BUILD ($hash) {
    $self->logDebug("");
}

#### BALANCER
method testGetFileExports {
	my $file	=	"$Bin/inputs/envars.sh";
	
	my $exports	=	$self->getFileExports($file);
	#$self->logDebug("exports", $exports);
	
	my $expected	=	qq{export ASSIGNEE=ucsc_biofarm; export KEYFILE=/root/annai-cghub.key; export REF_SEQ=/opt/reference/genome.fa.gz; export WORK_DIR=/mnt; export REF_SEQ=/pancanfs/reference/genome.fa.gz; export WORK_DIR=/mnt; export PATH=/mnt/data/apps/libs/boost/1.39.0/libs:\$PATH; export PATH=/agua/apps/pcap/0.3.0/bin:\$PATH; export PATH=/agua/apps/pcap/PCAP-core/install_tmp/bwa:\$PATH; export PATH=/agua/apps/pcap/PCAP-core/install_tmp/samtools:\$PATH; export PATH=/agua/apps/pcap/PCAP-core/bin:\$PATH; export PATH=/agua/apps/pcap/PCAP-core/install_tmp/biobambam/src:\$PATH; export PYTHONPATH=/usr/local/lib/python2.7/:\$PYTHONPATH; export PYTHONPATH=/usr/local/lib/python2.7/lib-dynload:\$PYTHONPATH; export PERL5LIB=; export PERL5LIB=/agua/apps/pcap/0.3.0/lib:\$PERL5LIB; export PERL5LIB=/agua/apps/pcap/0.3.0/lib/perl5:\$PERL5LIB; export PERL5LIB=/agua/apps/pcap/0.3.0/lib/perl5/x86_64-linux-gnu-thread-multi:\$PERL5LIB; export PERL5LIB=/agua/apps/pcap/PCAP-core/lib:\$PERL5LIB; export PERL5LIB=/agua/apps/pcap/0.3.0/lib/perl5/x86_64-linux-gnu-thread-multi:\$PERL5LIB; export LD_LIBRARY_PATH=; export LD_LIBRARY_PATH=/mnt/data/apps/libs/boost/1.39.0/libs:\$LD_LIBRARY_PATH; export LD_LIBRARY_PATH=/agua/apps/biobambam/libmaus-0.0.108-release-20140319092837/src/.libs:\$LD_LIBRARY_PATH; export LD_LIBRARY_PATH=/agua/apps/pcap/PCAP-core/install_tmp/libmaus/src/.libs:\$LD_LIBRARY_PATH; export LD_LIBRARY_PATH=/agua/apps/pcap/PCAP-core/install_tmp/snappy/.libs:\$LD_LIBRARY_PATH; };
	
	is_deeply($exports, $expected, "file exports extracted to string");
}


}	#### class Engine::Stage
