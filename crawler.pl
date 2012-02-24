#!/usr/bin/perl 
#
#
use strict;
use warnings;
use Net::Stomp;
use LWP::UserAgent;
use Data::Dump qw(dump);
use JSON;
use Encode;
use POSIX;
use File::Pid;
use AppConfig;
use URI::URL;

my $daemonName = "MQcrawler";

#=======================================
# Lecture du fichier de config
#=======================================
my $config = AppConfig->new();
   $config->define("patterns",      {ARGCOUNT => AppConfig::ARGCOUNT_LIST});
   $config->define("redis_server",  {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
   $config->define("redis_set_urls",  {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
   $config->define("redis_set_urls_crawled",  {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
   $config->define("activemq_server",  {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
   $config->define("activemq_source",  {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
   $config->define("activemq_links",  {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
   $config->define("activemq_crawl",  {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
   $config->define("logging",       {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
   $config->define("logpath",       {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
   $config->define("pidpath",       {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
   $config->file('YSpider.conf');

   my @patterns_to_crawl   = ();
   my $patterns = $config->patterns;
   my $i;
   foreach $i (@$patterns) {
                push( @patterns_to_crawl, $i);
   }
   my $redis_server     = $config->redis_server;
   my $activemq_server  = $config->activemq_server;
   my $activemq_source  = $config->activemq_source;
   my $activemq_links   = $config->activemq_links;
   my $activemq_crawl   = $config->activemq_crawl;
   my $logging          = $config->logging();                           # 1= logging is on
   my $logFilePath      = $config->logpath();                           # log file path
   my $pidFilePath      = $config->pidpath();                           # PID file path
      $logFilePath      =~ s/([^\/])$/$1\//; # add slash / if not specified in config 
      $pidFilePath      =~ s/([^\/])$/$1\//; # add slash / if not specified in config
   my $logFile          = $logFilePath . $daemonName . ".log";
   my $pidFile          = $pidFilePath . $daemonName . ".pid";

# global variables
my @redirects=();
my @headers=();

sub GiveMeNextURLToCrawl
{
	my $pid;
	my $ichild=0;
	for (1..10){
		$ichild ++;
		sleep(0.1);
   		$pid = fork();
   		last unless defined $pid;  # Too many processes already?
   		unless($pid){
		my $stomp = Net::Stomp->new( { hostname => $activemq_server, port => '61613' } );
		eval {
			# Child code here
        		$stomp->connect();
        		$stomp->subscribe({destination  => $activemq_crawl, 'ack' => 'client', 'activemq.prefetchSize' => 1 });
        		if ($stomp->can_read({ timeout => '1' }) eq 1) {
	    			my $frame = $stomp->receive_frame;
    				my $url= $frame->body; 
				logEntry("(child $ichild) : crawl de $url");

				my $ua = LWP::UserAgent->new;
        			$ua->timeout(10);
				@redirects = ();
				$ua->add_handler("response_redirect", sub {  my($response, $ua, $h) = @_;
                                                                                 my $url      = $response->request->uri;
                                                                                 my $code     = $response->code;
                                                                                 my $location = $response->header("location");
                                                                                 if (($code eq "301")||($code eq "302")) {
                                                                                        my $redirect = {};
                                                                                        $redirect->{"url"} = $url->as_string;
                                                                                        $location=url($location, URL_GetBase($url))->abs;
                                                                                        $redirect->{"location"} = $location->as_string;
                                                                                        $redirect->{"code"} = $code;
                                                                                        push (@redirects, $redirect);
                                                                                 };
                                                                            return });

				my $res = $ua->get($url);
					my $source = "";
					if ( $res->header("content-type") =~  /text\/htm/i) {
					#if ( $res->header("content-type") =~  /.*text\/htm.*/i) {
						$source = $res->decoded_content;
					}
					@headers=();
					my $h=$res->headers;
					$h->remove_header("Link");  $h->remove_header("X-Meta-Description");  $h->remove_header("X-Meta-Keywords");  $h->remove_header("X-Meta-Robots");
					$h->remove_header("Title");  $h->remove_header("Client-Response-Num");
					$h->scan( sub {  my ($a,$b) = @_;  my $header ={};  $header->{$a}=$b;  push @headers, $header;  } );

					my $frameproducer = Net::Stomp::Frame->new( {
  						body    => encode_utf8($source),
  						command => "SEND",
  						headers => {  "mqcrawler.initial_url"   => $frame->body,
							"mqcrawler.final_url"           => $res->request->uri_canonical->as_string,
               						"mqcrawler.redirects" 		=> to_json(\@redirects),
               						"mqcrawler.http_headers" 	=> to_json(\@headers),
               						"mqcrawler.status_code" 	=> $res->code,
               						"correlation-id" 		=> $frame->body,
               						"destination"    		=> $activemq_source,
							"persistent"     		=> 'true'
	     						}
						});
					$stomp->send_frame($frameproducer);
    					$stomp->ack( { frame => $frame } );
			}
		};
		logEntry($@) if $@;
		eval {  $stomp->disconnect; };
        	exit;
		}
	}
	# wait() for kids
	while(($pid = wait()) > 0){  sleep(0.2); }
	return;
}
 
#################################################################################################
# MAIN PROGRAM
#################################################################################################
        my $dieNow        = 0;                                     # used for "infinte loop" construct - allows daemon mode to gracefully exit
        my $sleepMainLoop = 1;                                    # number of seconds to wait between "do something" execution after queue is clear

        # daemonize
        use POSIX qw(setsid);
        chdir '/';
        umask 0;
        open STDIN,  '/dev/null'   or die "Can't read /dev/null: $!";
        open STDOUT, '>>/dev/null' or die "Can't write to /dev/null: $!";
        open STDERR, '>>/dev/null' or die "Can't write to /dev/null: $!";

        # dissociate this process from the controlling terminal that started it and stop being part
        # of whatever process group this process was a part of.
        POSIX::setsid() or die "Can't start a new session.";

        # callback signal handler for signals.
        $SIG{INT} = $SIG{TERM} = $SIG{HUP} = \&signalHandler;
        $SIG{PIPE} = 'ignore';

        # create pid file in /var/run/
        my $pidfile = File::Pid->new( { file => $pidFile, } );
        $pidfile->write or die "Can't write PID file, /dev/null: $!";

        # turn on logging
        if ($logging) {
            open LOG, ">>$logFile";
            select((select(LOG), $|=1)[0]); # make the log file "hot" - turn off buffering
        }

    logEntry("daemon started");

    # Enter loop to do work
    # "infinite" loop where some useful process happens
    until ($dieNow) {
	GiveMeNextURLToCrawl();
        sleep($sleepMainLoop);
    }

    logEntry("daemon stopped");
    exit;

# add a line to the log file
sub logEntry {
    my ($logText) = @_;
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
    my $dateTime = sprintf "%4d-%02d-%02d %02d:%02d:%02d", $year + 1900, $mon + 1, $mday, $hour, $min, $sec;
    if ($logging) {
        print LOG "$dateTime|$daemonName|$logText\n";
    }
}

# catch signals and end the program if one is caught.
sub signalHandler {
    $dieNow = 1;    # this will cause the "infinite loop" to exit
}

# do this stuff when exit() is called.
END {
    if ($logging) { close LOG }
    $pidfile->remove if defined $pidfile;
}
                            
sub URL_GetBase {
my ($url) = @_;
        my $base = url($url)->scheme."://".url($url)->host;
        if (url($url)->port != "80") { $base = $base.":".url($url)->port }
        $base = $base."/";
        return $base;
}

 
