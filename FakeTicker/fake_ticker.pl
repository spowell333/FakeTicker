#!/usr/bin/perl -w
use strict;
use Finance::Quote;
use Data::Dumper;
use threads;

use threads::shared;
use Thread::Semaphore;
use Thread::Queue;
use XML::Simple;
use JSON;
use IO::Socket::INET;



my $SEPARATOR = qq{\x1C}; 
my $HANGUP    = qq{\x04};


my $DEBUG         = 1; 
my $quote_service = Finance::Quote->new;
my @tickers       = refresh_tickers();

my $counter_semaphore = Thread::Semaphore->new();
my $counter : shared  = 200 ;

my $quote_queue      = Thread::Queue->new();


my $quote_loader = threads->new(\&quote_loader, $quote_service, $quote_queue, $DEBUG, @tickers);
my $collector    = threads->new(\&quote_collector, $quote_queue, \&quote_randomiser, $DEBUG);

$quote_loader->join();
$collector->join();

sub quote_loader{
	my ($quote_service, $queue, $DEBUG, @tickers) = @_;
	while (can_run()) {
		my %info = $quote_service->fetch("usa", @tickers);
		
		ATTRIBUTE: while (my ($k, $v) =each(%info)) { 
			if ($k =~/ask/ or $k =~/bid/) {
				next ATTRIBUTE if (!defined($v));
				print $k ,' ',$v,qq{\n} if ($DEBUG);
				my @price : shared = ($k, $v);
				$queue->enqueue(\@price);
			} 
		}
		
		sleep 30;
	}
	
}

sub quote_collector {
	my ($queue, $output_handler, $DEBUG) = @_;
    my %queues = ();
                 
	my $rep_count = 0 ;
	while(my $price_ref = $queue->dequeue()) {
		last unless (can_run());
		
		my ($ticker_and_type, $price)= @{$price_ref};
		my ($ticker, $type) = split(/$SEPARATOR/,$ticker_and_type, 2);
		
		my $output_queue = $queues{$ticker}->{$type}->[0];
		if(!defined($output_queue)) {
			$output_queue = Thread::Queue->new();
			$queues{$ticker}->{$type}->[0] = $output_queue;
			my $new_thread = threads->new(
				$output_handler, 
				$ticker, 
				$type, 
				$output_queue, 
				\&get_fresh_quote_nb, 
				\&pulish_as_json, 
				$DEBUG);
			$new_thread->detach();
				
		}
		my $shared_price : shared = $price;
		$output_queue->enqueue($shared_price);
		
		
	}
}
sub quote_publisher {
	my $ticker		= shift;
	my $type		= shift;
	my $q			= shift;
	my $get_fresh_q = shift; # ignored here, we always want to block
	my $publisher 	= shift;
	my $DEBUG 		= shift || 0;
	
	print 'publisher for ' . $ticker . ' / ' . $type . qq{ started \n}; 	
	while(my $fresh_quote = get_fresh_quote_b($q)) {
		
		my $quote = {
			'TICKER' => $ticker,
			'TYPE'   => $type,
			'PRICE'  => $fresh_quote,
		};
	
		$publisher->($quote, $DEBUG);
	
	}
}

sub quote_randomiser{
	my $ticker		= shift;
	my $type		= shift;
	my $q			= shift;
	my $get_fresh_q = shift;
	my $publisher 	= shift;
	my $DEBUG 		= shift || 0;
	
	print 'randomiser for ' . $ticker . ' / ' . $type . qq{ started \n}; 
		
	my @all_quotes   = ();
	my %queues       = ();
	my $recent_quote = 0;
	while (1) {
		my $fresh_quote = $get_fresh_q->($q);
		if (defined($fresh_quote)) { 
			unshift(@all_quotes, $fresh_quote);
			$recent_quote = $fresh_quote;
		}
		
		if ( 0 == scalar(@all_quotes)) { 
			$fresh_quote = $q->dequeue();
			unshift(@all_quotes, $fresh_quote);
			$recent_quote = $fresh_quote;
		}
		
		my $data_points = scalar(@all_quotes);
		$data_points = ($data_points > 20) ? 20 :$data_points;
		 
		my $mean_price = 0 ; 
		
		for(my $i=0 ; $i < $data_points ; $i++) { 
			$mean_price += $all_quotes[$i];
		}
		
		$mean_price /= ($data_points * 1.0);
		
		my $mean_price_with_jitter = apply_jitter($recent_quote, $mean_price, $DEBUG);
		my $mean_price_with_rounding  = apply_rounding($mean_price_with_jitter, $type, $recent_quote, $DEBUG );
		
		print 'mean '  .  $type . ' for ' . $ticker . ' is now ' .$mean_price_with_rounding . 
		' latest price ' . $recent_quote . ' with ' . $data_points . ' data point(s)'. qq{\n} if ($DEBUG);
		
	
		#my $output_queue = get_queue_for_thread();
		
		my $output_queue = $queues{$ticker}->{$type}->[0];
		if(!defined($output_queue)) {
			$output_queue = Thread::Queue->new();
			$queues{$ticker}->{$type}->[0] = $output_queue;
			my $new_thread = threads->new(
				\&quote_publisher, 
				$ticker, 
				$type, 
				$output_queue, 
				\&get_fresh_quote_b, 
				$publisher, 
				$DEBUG);
			$new_thread->detach();
				
		}
		my $shared_price : shared = $mean_price_with_rounding;
		$output_queue->enqueue($shared_price);
			

		threads->yield();
		my $nap_time = int(rand(5));
		print 'About to sleep for ' . $nap_time . 's' .qq{\n} if ($DEBUG);	
		sleep($nap_time);
	}
}


sub get_fresh_quote_nb {
	return shift->dequeue_nb();
}

sub get_fresh_quote_b {
	return shift->dequeue();
}

sub apply_jitter{
	my $real_quote = shift; 
	my $fake_quote = shift; 
	my $DEBUG       = shift || 0;
	
	my $diff = $real_quote - $fake_quote;
	print 	'About to apply a difference of ' . $diff /2.0 . 
			qq{ to a real price of $real_quote\n} if ($DEBUG);
	$fake_quote += ($diff / 2.0);
	
	print 	'$diff is '.(($diff>0)?'>':'<=').' 0, '.
			'we should be ' . (($diff>0) ?  'BUY' : 'SELL') . 'ING' .qq{\n} if ($DEBUG);
	
	my $jitter = rand() - 0.5;
	print 'About to scale $diff by ' . $jitter .qq{\n} if ($DEBUG);
	
	return $fake_quote + ($diff * $jitter);
}

sub pow($$) {
	my $base  = shift;
	my $power = shift;
	my $answer = $base;
	while ($power > 1) { 
		$answer *= $base;
		$power--;
	}
	return $answer;
}

sub apply_rounding{
	my $fake_quote = shift || die;
	my $type       = shift || die;
	my $real_quote = shift || die;
	my $DEBUG      = shift || 0;  
	
	my $decimal_places  = length($1) if ($real_quote =~/\d*?\.(\d*)\z/xms);
    print 'decimal places ' . $decimal_places .qq{\n} 	if ($DEBUG) ;
	
	my $multiplier = pow(10.0,$decimal_places);
	print '$multiplier ' . $multiplier .qq{\n} if ($DEBUG);
	
	my $scaled_up_price = $fake_quote * $multiplier;
	print '$scaled_up_price ' . $scaled_up_price .qq{\n} if ($DEBUG);
	
	my $rounding_dir    = (($type eq 'bid')?-1:1);
	print '$rounding_dir ' . $rounding_dir .qq{\n} if ($DEBUG);
	
	return int($scaled_up_price + (0.5 * $rounding_dir) ) / $multiplier;
	
}
sub pulish_as_json {
	my $quote = shift;
	my $DEBUG = shift || 0;
	my $json_text = to_json($quote);
	print $json_text .qq{\n} if ($DEBUG);
	send_to_port($json_text);
}

sub publish_as_xml { 
	my $quote = shift;
	my $DEBUG = shift || 0;
	my $msg = XMLout($quote);
	print $msg .qq{\n} if ($DEBUG);
	send_to_port($msg);
}


sub send_to_port { 
	my $message = shift;
	my $port    = shift || get_server_port();
	my $server  = shift || get_server_address();

	my $sock = IO::Socket::INET->new($server . ':' . $port ) ; 

	my $optional_newline = ($message =~ /\n\z/xms) ? q{} : qq{\n} ;
	if (defined($sock) && $sock->connected()) { 
		print 'About to send ' . $message . qq{ to $server : $port\n}; 
		$sock->send($message . $optional_newline . $HANGUP . qq{\n});
	}

}

sub send_shutdown() { 
	send_to_port("SHUTDOWN\n", get_admin_port());
	send_to_port($HANGUP, get_server_port());
}

sub get_server_address(){
	return "127.0.0.1";
}
sub get_server_port() { 
	return 4200;
}
sub get_admin_port() { 
	return 4201;
}

sub refresh_tickers {
	#return qw{YHOO MSFT CSCO GOOG MKTX C BAC  JPM IBM OIL PM LMT BA HON BTI RAI INTC GE UNG QQQQ};
	return qw{BARC.L GSK LSE.L HSBA.L LLOY.L RBS.L UU.L SVT.L BG.L BP.L CPG.L BVIC.L VOD.L ANTO.L TSCO.L};
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
	my $can_run = $answer > 0;
	 
	send_shutdown() unless ($can_run);
	return $can_run;
}


