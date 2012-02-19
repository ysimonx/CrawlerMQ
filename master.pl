#!/usr/bin/perl 
#
#
use strict;
use warnings;
use Net::Stomp;
use Data::Dump qw(dump);
use HTML::Parser;
use JSON;
use AppConfig;
use AppConfig::State;
use AppConfig::File;
use POSIX;
use File::Pid;
use Redis;

my $daemonName = "master";

#=======================================
# Lecture du fichier de config
#=======================================
my $config = AppConfig->new();
   $config->define("patterns",      {ARGCOUNT => AppConfig::ARGCOUNT_LIST});
   $config->define("redis_server",  {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
   $config->define("activemq_server",  {ARGCOUNT => AppConfig::ARGCOUNT_ONE});
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
   my $activemq_server  = $config->activemq_server;

   my $logging       = $config->logging();                           # 1= logging is on
   my $logFilePath   = $config->logpath();                           # log file path
   my $pidFilePath   = $config->pidpath();                           # PID file path

   $logFilePath   =~ s/([^\/])$/$1\//; # add slash / if not specified in config 
   $pidFilePath   =~ s/([^\/])$/$1\//; # add slash / if not specified in config

   my $logFile       = $logFilePath . $daemonName . ".log";
   my $pidFile       = $pidFilePath . $daemonName . ".pid";

sub GiveMeNextLinksToCrawl
{
    eval {
	my $link;
	my @taboflinks;
 	my $stomp = Net::Stomp->new( { hostname => $activemq_server, port => '61613'} );

	$stomp->connect();
	$stomp->subscribe(
        {   destination             => '/queue/links',
                'ack'                   => 'client',
                'activemq.prefetchSize' => 1
        });

	my $r = Redis->new( server => $redis_server);

	while($stomp->can_read({ timeout => '1' }) eq 1) {
	    	my $frame = $stomp->receive_frame;
    		my $json_links= $frame->body; # do something here
		my $url   = $frame->headers->{"correlation-id"};
		logEntry("pop ".$url);
		@taboflinks=();
		@taboflinks = @{decode_json($json_links)};
		foreach $link (@taboflinks) {
			if (URL_isAutorised( $link->{"href"} ) eq "true" )
                        {
				if (! $r->sismember( "url",  $link->{"href"} )) {
					# print $link->{"href"}."\n";
					logEntry("push ".$link->{"href"});
		                	my $frame_producer = Net::Stomp::Frame->new( {
                	                	body    => $link->{"href"},
                       	         		command => "SEND",
                                		headers => {
                                        		"destination"    => "/queue/crawl",
							"correlation-id" => $link->{"href"},
							"persistent"     => 'true'
                                        	}
                			});
                			$stomp->send_frame($frame_producer);
					my $ok = $r->sadd( "url", $link->{"href"} );
				}
			}	
                }

		$stomp->ack( { frame => $frame } );
	}
	$r->quit;
  	$stomp->disconnect;
  };
  logEntry($@) if $@;
  return;
}

sub URL_isAutorised {           # Verify if the given url can be crawled by configuration file (patterns) or robots.txt
        my ($url) = @_;
        my ($pattern);

        foreach $pattern (@patterns_to_crawl) 
	{
                if (substr($url,0,length($pattern)) eq $pattern)
	        {
                        return "true";
                }
        }
        return "false";
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
#       defined( my $pid = fork ) or die "Can't fork: $!";
#       exit if $pid;

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
          GiveMeNextLinksToCrawl(); 
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
