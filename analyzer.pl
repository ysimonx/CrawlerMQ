#!/usr/bin/perl 
#
use strict;
use warnings;
use Net::Stomp;
use Data::Dump qw(dump);
use HTML::Parser;
use JSON;
use URI::URL;
use Encode;
use POSIX;
use File::Pid;
use AppConfig;

my $daemonName = "analyzer";

#=======================================
# Lecture du fichier de config
#=======================================
my $config = AppConfig->new();
   $config->define("patterns",      {ARGCOUNT => AppConfig::ARGCOUNT_LIST});
   $config->define("redis_server",  {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
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

   my $redis_server  = $config->redis_server;

   my $logging       = $config->logging();                           # 1= logging is on
   my $logFilePath   = $config->logpath();                           # log file path
   my $pidFilePath   = $config->pidpath();                           # PID file path

   $logFilePath   =~ s/([^\/])$/$1\//; # add slash / if not specified in config 
   $pidFilePath   =~ s/([^\/])$/$1\//; # add slash / if not specified in config

   my $logFile       = $logFilePath . $daemonName . ".log";
   my $pidFile       = $pidFilePath . $daemonName . ".pid";


my @taboflinks=();

sub parseur_start {
        my ($tag, $attr,$tableau) = @_;
		$tag=lc($tag);
                if ($tag eq "a") {
                        my $atag = {};
			$atag->{'href'}="";
			$atag->{'rel'}="";
                        if (defined($attr->{"href"}))  {
                                $atag->{'href'} = $attr->{"href"};
			}
                        if (defined($attr->{"rel"})) {
                        	$atag->{'rel'} = $attr->{"rel"};
                        }
			eval "push \@$tableau,\$atag";
		}
}

sub URL_GetBase {
	my ($url) = @_;
        my $base = url($url)->scheme."://".url($url)->host;
        if (url($url)->port != "80") { $base = $base.":".url($url)->port }
        $base = $base."/";
        return $base;
}


sub GiveMeNextSourceToAnalyze
{
       $daemonName = 'test';

        my $stomp = Net::Stomp->new( { hostname => 'activemq.refnat.com', port => '61613'} );

        my $frame_connect = Net::Stomp::Frame->new( {
                                                command => "CONNECT",
                                                headers => {
                                                        'client-id' => $daemonName
                                                }});
        $stomp->send_frame($frame_connect);
        $frame_connect = $stomp->receive_frame;

        $stomp->subscribe(
        {   destination             => '/topic/source',
                'ack'                   => 'client',
                'activemq.prefetchSize' => 1,
                'activemq.subscriptionName' => $daemonName,
                'activemq.subcriptionName' =>  $daemonName
        });

        while($stomp->can_read({ timeout => '1' }) eq 1) {
		my $link;
		my  $linkurl;
		my  $linkurls;

    		my $frame = $stomp->receive_frame;
    		my $source= decode_utf8($frame->body); # do something here
		my $url   = $frame->headers->{"correlation-id"};

		logEntry("analyse de $url");

		@taboflinks=();
		my $parserhtml = HTML::Parser->new();
           	$parserhtml->handler(start =>\&parseur_start,"tagname,attr,'taboflinks'");
           	$parserhtml->parse($source);

		foreach $link (@taboflinks) {
                	$linkurl  = url($link->{'href'}, URL_GetBase($url))->abs;   # absolute url
			$linkurls = $linkurl->as_string;                            # 
			$linkurls =~ s/(^http[^\#]*)\#.*/$1/;                       # eliminate anchor (after "#")
			$link->{'href'}=$linkurls;
		}
	
		my $json = to_json(\@taboflinks);
		# you can retrieve the array with  : my @tab = @{decode_json($json)};
		my $frame_producer = Net::Stomp::Frame->new( {
                		body    => $json,
                		command => "SEND",
                		headers => {
                        		"destination"    => "/queue/links",
					"correlation-id" => $url,
					"persistent"     => 'true'

                			}
                });
                $stomp->send_frame($frame_producer);

 		$stomp->ack( { frame => $frame } );
	}
  	$stomp->disconnect;

  return;
}
 
#################################################################################################
# MAIN PROGRAM
#################################################################################################
	my $dieNow        = 0;                                     # used for "infinte loop" construct - allows daemon mode to gracefully exit
	my $sleepMainLoop = 10;                                    # number of seconds to wait between "do something" execution after queue is clear
	 
	# daemonize
	use POSIX qw(setsid);
	chdir '/';
	umask 0;
	open STDIN,  '/dev/null'   or die "Can't read /dev/null: $!";
	open STDOUT, '>>/dev/null' or die "Can't write to /dev/null: $!";
	open STDERR, '>>/dev/null' or die "Can't write to /dev/null: $!";
	defined( my $pid = fork ) or die "Can't fork: $!";
	exit if $pid;
	 
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
       GiveMeNextSourceToAnalyze(); 
       sleep($sleepMainLoop);
    }

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

    logEntry("daemon stopped");
    if ($logging) { close LOG }
    $pidfile->remove if defined $pidfile;
}

