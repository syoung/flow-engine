use MooseX::Declare;
use Method::Signatures::Simple;

class Engine::Remote::Ssh with Util::Logger {

#### EXTERNAL
use Net::SCP qw(scp iscp);
use Net::SSH::Perl;
use Data::Dumper;
use File::Path;

#### INTERNAL
use Conf::Yaml;
use FindBin qw($Bin);

#### Str
has 'keyfile'       => ( is  => 'rw', 'isa' => 'Str|Undef', required  =>  0 );
has 'command'       => ( is  => 'rw', 'isa' => 'Str|Undef', required  =>  0 );
has 'username'      => ( is  => 'rw', 'isa' => 'Str|Undef', required  =>  0 );
has 'remotehost'    => ( is  => 'rw', 'isa' => 'Str|Undef', required  =>  0 );
has 'keyname'       => ( is  => 'rw', 'isa' => 'Str|Undef', required  =>  0  );
has 'keypairfile'   => ( is  => 'rw', 'isa' => 'Str|Undef', required  =>  0  );

#### Obj
has 'conf'          => ( is => 'rw', isa => 'Conf::Yaml', required => 1 );
has 'profilehash'   => ( is  => 'rw', 'isa' => 'HashRef|Undef', required  =>  0 );

has 'ssh'  =>  (
  is     =>  'rw',
  isa    =>  'Net::OpenSSH',
  # lazy   =>  1,
  # builder  =>  "setSsh"
);

has 'scp'  =>  (
  is     =>  'rw',
  isa    =>  'Net::SCP',
  # lazy   =>  1,
  # builder  =>  "setScp"
);


method BUILD ( $args ) {
  # $self->logDebug("args", $args); 
  my $username = $args->{username};
  $self->username( $username );
  my $profilehash = $args->{profilehash};
  # $self->logDebug( "profilehash", $profilehash );
  my $hostname = $profilehash->{host}->{name};
  $self->logDebug( "hostname", $hostname );
  $self->logDebug( "username", $username );

  $self->_setScp( $username, $hostname );

  $self->_setSsh( $username, $hostname );
}

method _setScp ( $username, $hostname ) {
  $self->logDebug("username", $username);
  # $self->logDebug("hostname", $hostname);
  
  my $scp = Net::SCP->new( $hostname, $username );
  $self->scp( $scp );
  
  return $scp;
}

method _setSsh ( $username, $hostname ) {
  $self->logDebug("username", $username);
  # $self->logDebug("hostname", $hostname);
  
  use Net::OpenSSH; 
  my $ssh = Net::OpenSSH->new( $hostname );

  $ssh->error and die "Couldn't establish SSH connection: ". $ssh->error;

  $self->ssh( $ssh );
  
  return $ssh;
}

method command ( $command ) {
  $self->logDebug( "command", $command );

  my ($stdout, $stderr) = $self->ssh()->capture2( $command );
  $self->ssh()->error and die "remote command failed with error: " . $self->ssh()->error;
  # my ( $stdout, $stderr, $exit ) = $self->ssh()->system( $command );
  $self->logDebug( "stdout", $stdout );
  $self->logDebug( "stderr", $stderr );
  # $self->logDebug( "exit", $exit );

  return ( $stdout, $stderr );
  # return ( $stdout, $stderr, $exit );
}

method makeDir ( $directory ) {
  $self->logDebug( "directory", $directory );

  my $command = "/bin/mkdir -p $directory";
  # my $command = "/bin/mkdir $directory";

  # $command = "/bin/mkdir -p /scratch/kbsf633/.flow/max/3-store2scp/scripts";  
  # $command = "/bin/mkdir -p /scratch/kbsf633/.flow/max/3-store2scp/next";  
  # $command = "ls /";
  $self->logDebug( "command", $command );

  return $self->command( $command );
}

method copy ( $source, $destination ) {
  # $self->logDebug( "source", $source );
  # $self->logDebug( "destination", $destination );

  my $result = Net::SCP::scp($source, $destination);
  # $self->logDebug( "result", $result );

  return $result;
}

method copyFromRemote ( $source, $destination ) {
  $self->logDebug( "source", $source );
  $self->logDebug( "destination", $destination );

  my $result = Net::SCP::scp($source, $destination);
  $self->logDebug( "result", $result );
}

#### SET SSH COMMAND IF KEYPAIRFILE, ETC. ARE DEFINED
method setKeypairFile ( $username) {
  $self->logCaller("username: $username");

  $username = $self->username() if not defined $username;
  $self->logError("username not defined") and exit if not defined $username;

  my $keyname   =   "$username-key";
  my $conf     =   $self->conf();
  #$self->logDebug("conf", $conf);
  my $homedir   =   $conf->getKey("core:HOMEDIR");
  $self->logCaller("userdir not defined") and exit if not defined $homedir;

  my $keypairfile = "$homedir/$username/.starcluster/id_rsa-$keyname";

  my $adminkey   =   $self->getAdminKey($username);
  $self->logDebug("adminkey", $adminkey);
  return if not defined $adminkey;
  my $configdir = "$homedir/$username/.starcluster";
  if ( $adminkey ) {
    my $adminuser = $self->conf()->getKey("core:ADMINUSER");
    $self->logDebug("adminuser", $adminuser);
    my $keyname = "$adminuser-key";
    $keypairfile = "$homedir/$adminuser/.starcluster/id_rsa-$keyname";
  }
  $self->keypairfile($keypairfile);
  
  return $keypairfile;
}

method getAdminKey ($username) {   
  $self->logCaller("username", $username);
  $self->logDebug("username not defined") and return if not defined $username;

  return $self->adminkey() if $self->can('adminkey') and defined $self->adminkey();
  
  my $adminkey_names = $self->conf()->getKey("aws:ADMINKEY");
   #$self->logDebug("adminkey_names", $adminkey_names);
  $adminkey_names = '' if not defined $adminkey_names;
  my @names = split ",", $adminkey_names;
  my $adminkey = 0;
  foreach my $name ( @names ) {
     #$self->logDebug("name", $name);
    if ( $name eq $username )  {  return $adminkey = 1;  }
  }

  $self->adminkey($adminkey) if $self->can('adminkey');
  
  return $adminkey;
}


} # class