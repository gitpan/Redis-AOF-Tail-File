package Redis::AOF::Tail::File;

use 5.008008;
use strict;
use warnings;
use File::Tail;

our $VERSION = '0.02';

sub new
{
	my $pkg = shift;
	my $self = {@_};
	bless $self, $pkg;

	return 0 unless -e $self->{aof_filename};
	$self->{interval} = 1 unless $self->{interval};

	$self->{ARRAY_REDIS_AOF} = ();
	$self->{FILE_TAIL_FH} = File::Tail->new(
		name=>$self->{aof_filename}, 
		interval=>$self->{interval}, 
		maxinterval=>$self->{interval});
	return $self;
}

sub read_command
{
	my $self = shift;
	return 0 unless $self->{FILE_TAIL_FH};

	while (defined(my $line = $self->{FILE_TAIL_FH}->read)) 
	{
		$line =~ s/\s//g;
		push @{$self->{ARRAY_REDIS_AOF}}, $line;
		while( defined ${$self->{ARRAY_REDIS_AOF}}[0] and ${$self->{ARRAY_REDIS_AOF}}[0] !~ /^\*\d/)
		{
			shift @{$self->{ARRAY_REDIS_AOF}};
		}
		my ($cmd_num) = ${$self->{ARRAY_REDIS_AOF}}[0] =~ /^\*(\d{1,2})/;

		next if(scalar @{$self->{ARRAY_REDIS_AOF}} < $cmd_num*2 + 1); # Wait for the complete command

		shift @{$self->{ARRAY_REDIS_AOF}};
		my $cmd = "";
		for(1..$cmd_num)
		{
			shift @{$self->{ARRAY_REDIS_AOF}};
			$cmd .= shift @{$self->{ARRAY_REDIS_AOF}};
			$cmd .= ' ';
		}
		$cmd = substr($cmd, 0, -1);
		return $cmd;
	}
}

1;
__END__

=head1 NAME

Redis::AOF::Tail::File - Read redis aof file in realtime

=head1 SYNOPSIS

  use Redis::AOF::Tail::File;

	my $aof_file = "/var/redis/appendonly.aof";
	my $redis_aof = Redis::AOF::Tail::File->new(aof_filename => $aof_file);
	while (my $cmd = $redis_aof->read_command)
	{
  	print "[$cmd]\n";
	}

=head1 DESCRIPTION

This extension can be used for persistence data from redis to MySQL.
Maybe you can code like below.

	use DBI;
  use Redis::AOF::Tail::File;

	# variables in this comment should be defined
	# $data_source, $username, $auth, \%attr, 
	# some_func_translate_redis_command_to_sql()
	
  my $dbh = DBI->connect($data_source, $username, $auth, \%attr);
	my $aof_file = "/var/redis/appendonly.aof";
	my $redis_aof = Redis::AOF::Tail::File->new(aof_filename => $aof_file);
	while (my $cmd = $redis_aof->read_command)
	{
  	my $sql = some_func_translate_redis_command_to_sql($cmd);
		$dbh->do($sql);
	}
	

=head2 EXPORT

None by default.


=head1 SEE ALSO

L<Redis::Term>, L<Redis>

=head1 AUTHOR

Chen Gang, E<lt>yikuyiku.com@gmail.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2014 by Chen Gang

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.16.2 or,
at your option, any later version of Perl 5 you may have available.


=cut
