#!/usr/bin/perl -w
use strict;
#use warnings;
use Finance::Quote;
use Data::Dumper;
use BerkeleyDB;
use threads;

use threads::shared;
use Thread::Semaphore;
use Thread::Queue;
use XML::Simple;


my $SEPARATOR = qq{\x1C}; 

my $quote_service = Finance::Quote->new;
my @tickers       = refresh_tickers();

my $counter_semaphore = Thread::Semaphore->new();
my $counter : shared  = 200 ;

my $quote_queue      = Thread::Queue->new();
my $randomiser_queue =  Thread::Queue->new();

my $quote_loader = threads->new(\&quote_loader, $quote_service, $quote_queue, @tickers);
my $collector    = threads->new(\&quote_collector, $quote_queue, $randomiser_queue);

$quote_loader->join();
$collector->join();

sub quote_loader{
	my ($quote_service, $queue, @tickers) = @_;
	while (can_run()) {
		my %info = $quote_service->fetch("usa", @tickers);
		
		ATTRIBUTE: while (my ($k, $v) =each(%info)) { 
			if ($k =~/ask/ or $k =~/bid/) {
				next ATTRIBUTE if (!defined($v));
				print $k ,' ',$v,qq{\n};
				my @price : shared = ($k, $v);
				$queue->enqueue(\@price);
			} 
		}
		
		sleep 60;
	}
	
}

sub quote_collector {
	my ($queue, $randomiser_queue) = @_;
    my %quote_data  = ();
    my %queues = ();
                 
	my $rep_count = 0 ;
	while(my $price_ref = $queue->dequeue()) {
		last unless (can_run());
		
		my ($ticker_and_type, $price)= @{$price_ref};
		
		my ($ticker, $type) = split(/$SEPARATOR/,$ticker_and_type, 2);
		
		my $most_recent_price = $quote_data{$ticker}->{$type}->[0];
		my $prices = $quote_data{$ticker}->{$type};
		unshift(@{$prices}, $price);
		
		my $queue = $queues{$ticker}->{$type}->[0];
		if(!defined($queue)) {
			$queue = Thread::Queue->new();
			$queues{$ticker}->{$type}->[0] = $queue;
			my $new_thread = threads->new(\&quote_randomiser, $ticker, $type, $queue);
			$new_thread->detach();
				
		}
		
		my $shared_price : shared = $price; 
		$queue->enqueue($shared_price);
	}
}

sub quote_randomiser{
	my $ticker = shift;
	my $type   = shift;
	my $q      = shift;
	
	print 'randomiser for ' . $ticker . ' / ' . $type . qq{\n}; 
		
	my @all_quotes = ();
	while (1) {
		my $fresh_quote = $q->dequeue_nb();
		if (defined($fresh_quote)) { 
			unshift(@all_quotes, $fresh_quote);
		}
		
		
		if ( 0 == scalar(@all_quotes)) { 
			$fresh_quote = $q->dequeue();
			unshift(@all_quotes, $fresh_quote);
		}
		
		my $data_points = scalar(@all_quotes);
		$data_points = ($data_points > 20) ? 20 :$data_points;
		 
		my $mean_ask = 0 ; 
		
		for(my $i=0 ; $i < $data_points ; $i++) { 
			$mean_ask += $all_quotes[$i];
		}
		
		$mean_ask /= ($data_points * 1.0);
		print 'mean '  .  $type . ' for ' . $ticker . ' is now ' .$mean_ask .qq{\n};
		
		my $msg = XMLout({
			'TICKER' => $ticker,
			'TYPE'   => $type,
			'PRICE'  => $mean_ask,
		});
		print $msg;
		sleep 3;
	}
}

sub refresh_tickers {
	return qw{GS GOOG MKTX BAC BARC.L GM};
	#return qw{GOOG};
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


