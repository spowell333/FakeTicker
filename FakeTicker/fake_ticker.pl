#!/usr/bin/perl -w
use strict;
use warnings;
use Finance::Quote;
use Data::Dumper;
use BerkeleyDB;
use threads;

use threads::shared;
use Thread::Semaphore;
use Thread::Queue;


my $SEPARATOR = qq{\x1C}; 

my $quote_service = Finance::Quote->new;
my @tickers       = refresh_tickers();

my $counter_semaphore = Thread::Semaphore->new();
my $counter : shared  = 200 ;

my %quote_data = (
					'ask' => {},
					'bid' => {},
                 );	
	

my $quote_queue = Thread::Queue->new(); 

my $quote_loader = threads->new(\&quote_loader, $quote_service, $quote_queue, @tickers);
my $randomiser   = threads->new(\&quote_randomier, $quote_queue);

$quote_loader->join();
$randomiser->join();

sub quote_loader{
	my ($quote_service, $queue, @tickers) = @_;
	my $rep_count = 0;
	while (can_run()) {
		print 'Quoter can run: ' . $rep_count++  .qq{\n};
		my %info = $quote_service->fetch("usa", @tickers);
		
		while (my ($k, $v) =each(%info)) { 
			if ($k =~/ask/ or $k =~/bid/) {
				my @price : shared = ($k, $v);
				$queue->enqueue(\@price);
				
			} 
		}
		
		#print Dumper $queue;
		sleep 60;
	}
	
}

sub quote_randomier {
	my ($queue) = @_;
	my $recent_bids = {};
	my $recent_asks = {};
	my $rep_count = 0 ;
	while(my $price_ref = $queue->dequeue()) {
		last unless (can_run());
		my ($ticker_and_type, $price)= @{$price_ref};
		
		my ($ticker, $type) = split(/$SEPARATOR/,$ticker_and_type, 2);
		
		
		UPDATE_PRICES: {
			my $asks = $quote_data{$type}->{$ticker};
			if (defined($asks)) { 
				unshift(@{$asks}, $price);
			} else {
				$quote_data{$type}->{$ticker} = [$price];
				
			} 
		 }
		
		print 'randomiser thread: ' .qq{\n};
		print Dumper \%quote_data;
	

	}
	
}

sub refresh_tickers {
	return qw{GS GOOG MKTX BAC BARC.L};
}

sub get_ask{
	return get('ask', @_);
}
sub get_bid{
	return get('bid', @_);
}

sub get($$) {
	my ($attrib, $string, @extras) = @_;
	return $1 if ($string =~ /(.*?) $SEPARATOR $attrib \z/xms);
}


sub can_run{
	$counter_semaphore->down();
	my $answer = $counter--;
	$counter_semaphore->up();
	
	print '$counter is now ' . $counter . qq{\n};
	return $answer > 0;
}


