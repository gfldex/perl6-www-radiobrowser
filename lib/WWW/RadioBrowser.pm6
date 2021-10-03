use v6.d;
use WWW;
use JSON::Fast;

# my &note = $*OUT.t ?? sub (**@s) { $*ERR.put: "{now.DateTime.Str} \e[1m{@s.join('')}\e[0m" } !! sub (|c) { $*ERR.say: c };

constant term:<GMT> = :timezone(0);
constant MAX_DEGREE = 16;

my $cfg-dir = „$*HOME/.config“.IO.e ?? „$*HOME/.config/perl6-www-radiobrowser“ !! „$*HOME/.perl6-www-radiobrowser“;

mkdir $cfg-dir unless $cfg-dir.IO.e;

enum InternetRadioStationsState is export <setup fetching reading processing storing ready error>;

constant internet-radio-update-progress is export = Supplier::Preserving.new;
constant radio-browser-debug is export = Supplier::Preserving.new;
constant internet-radio-batch-progress is export = Supplier.new;
constant radio-browser-benchmark is export = Supplier.new;

# my &note = sub (**@s){ radio-browser-debug.emit:  DateTime.now.hh-mm-ss ~ ': ' ~ @s.join }

my @cache;
my $cache-state = setup;

start { 
    @cache = internet-radio-stations-update();
}

sub internet-radio-stations() is export {
    @cache
}

sub internet-radio-stations-state() is export {
    $cache-state;
}

sub change-state(InternetRadioStationsState $s){
    $cache-state = $s;
    internet-radio-update-progress.emit: $s
}

sub benchmark(|c) {
    radio-browser-benchmark.emit: c;
}

sub ISO8601(DateTime:D $dt) {
    sub zPad(Str(Any) $s, int $p){'0' x $p - $s.chars ~ $s}

    ( 0 > $dt.year     ?? '-'~ zPad( $dt.year.abs, 4 ) !!
          $dt.year <= 9999 ?? zPad( $dt.year, 4 )
                           !! '+'~ zPad( $dt.year, 5 ) ) ~'-'~
    zPad( $dt.month, 2 ) ~'-'~ zPad( $dt.day, 2 ) ~'T'~
    zPad( $dt.hour, 2 ) ~':'~ zPad( $dt.minute, 2 ) ~':'~
        ( $dt.second.floor == $dt.second
            ?? zPad( $dt.second.Int, 2 )
            !! $dt.second.fmt('%09.6f') )
    ~
    ( $dt.timezone == 0
      ?? 'Z'
      !! $dt.timezone > 0
         ?? ( '+' ~ zPad( ($dt.timezone/3600).floor, 2 ) ~':'~
                    zPad( ($dt.timezone/60%60).floor, 2 ) )
         !! ( '-' ~ zPad( ($dt.timezone.abs/3600).floor, 2 ) ~':'~
                    zPad( ($dt.timezone.abs/60%60).floor, 2 ) ) )
}

my %type-map = 
    lastchangetime => { .Bool ?? DateTime.new(.subst(' ', 'T') ~ 'Z', :formatter(&ISO8601)) !! DateTime },
    clickcount => Int,
    bitrate => Int,
    clicktimestamp => { .Bool ?? DateTime.new(.subst(' ', 'T') ~ 'Z', :formatter(&ISO8601)) !! DateTime },
    votes => Int,
    lastcheckoktime => { .Bool ?? DateTime.new(.subst(' ', 'T') ~ 'Z', :formatter(&ISO8601)) !! DateTime },
    clicktrend => Int,
    lastcheckok => Bool,
    tags => { .split(',') },
    lastchecktime => { .Bool ?? DateTime.new(.subst(' ', 'T') ~ 'Z', :formatter(&ISO8601)) !! DateTime },
    stationuuid => Int;

sub internet-radio-stations-update(:$schema = 'http', Int :$limit = 100000) is export {
    my $cache-time = (try „$cfg-dir/stations.json“.IO.modified.DateTime) // now.DateTime.new(0);
    my @stations;

    if $cache-time < now.DateTime.earlier(:30days) {
        change-state fetching;
        note "fetching all unbroken stations";
        
        @stations = | jpost "$schema://de1.api.radio-browser.info/json/stations", {User-Agent => 'https://github.com/gfldex/perl6-www-radiobrowser'}, :$limit;
        my $stations-count = +@stations;

        change-state processing;
        note "processing $stations-count stations";
        my $progress-counter;
        internet-radio-batch-progress.Supply.tap: -> Int $i {
            $progress-counter += $i;
            note "$progress-counter/$stations-count processed";
        };
        @stations = @stations.race(:batch(1000), :degree(MAX_DEGREE)).map: { 
            .<lastchangetime> = .<lastchangetime> ?? DateTime.new(.<lastchangetime>.subst(' ', 'T') ~ 'Z', :formatter(&ISO8601)) !! DateTime; 
            .<clickcount> = .<clickcount>.Int;
            .<bitrate> = .<bitrate>.Int;
            .<clicktimestamp> = .<clicktimestamp> ?? DateTime.new(.<clicktimestamp>.subst(' ', 'T') ~ 'Z', :formatter(&ISO8601)) !! DateTime; 
            .<votes> = .<votes>.Int;
            .<lastcheckoktime> = .<lastcheckoktime> ?? DateTime.new(.<lastcheckoktime>.subst(' ', 'T') ~ 'Z', :formatter(&ISO8601)) !! DateTime; 
            .<clicktrend> = .<clicktrend>.Int;
            .<lastcheckok> = .<lastcheckok>.Int.Bool;
            .<tags> = .<tags>.split(',');
            .<lastchecktime> = .<lastchecktime> ?? DateTime.new(.<lastchecktime>.subst(' ', 'T') ~ 'Z', :formatter(&ISO8601)) !! DateTime; 
            .<stationuuid> = .<stationuuid>.Int;
            
            internet-radio-batch-progress.emit: 1000 if $++ %% 1000;

            .Hash
        };


        @cache = @stations;
        change-state storing;
        $stations-count = +@stations;
        {
            my $json = '[' ~ @stations.hyper(:batch(1000), :degree(MAX_DEGREE)).map({.&to-json}).join(',') ~ ']';
            „$cfg-dir/stations.json“.IO.spurt($json);
            my $elapsed = now - ENTER now;
            benchmark "$stations-count stored in {$elapsed}s with {$stations-count / $elapsed} stations/s";
        }
        change-state ready;
        benchmark ‚cache filled in: ‘ ~ (now - ENTER now) ~ ‚s‘;
    } else {
        my @stations;
        my $stations-count;
        {
            change-state reading;
            # note "read cache from $cfg-dir/stations.json";
            @stations = @(„$cfg-dir/stations.json“.IO.slurp.&from-json);
            $stations-count = @stations.elems;
            my $elapsed = now - ENTER now;
            benchmark "$stations-count read in {$elapsed}s with {$stations-count / $elapsed} stations/s";
        }

        {
            change-state processing;
            note "processing $stations-count stations";
            my $progress-counter;
            internet-radio-batch-progress.Supply.tap: -> Int $i {
                $progress-counter += $i;
                note "$progress-counter/$stations-count processed";
            };
            @stations = @stations.race(:batch(1000), :degree(MAX_DEGREE)).map: { 
                .AT-KEY(‚lastchangetime‘) = .AT-KEY(‚lastchangetime‘) ?? DateTime.new(.AT-KEY(‚lastchangetime‘), :formatter(&ISO8601)) !! DateTime; 
                .AT-KEY(‚clickcount‘) = .AT-KEY(‚clickcount‘).Int;
                .AT-KEY(‚bitrate‘) = .AT-KEY(‚bitrate‘).Int;
                .AT-KEY(‚clicktimestamp‘) = .AT-KEY(‚clicktimestamp‘) ?? DateTime.new(.AT-KEY(‚clicktimestamp‘), :formatter(&ISO8601)) !! DateTime; 
                .AT-KEY(‚votes‘) = .AT-KEY(‚votes‘).Int;
                .AT-KEY(‚lastcheckoktime‘) = .AT-KEY(‚lastcheckoktime‘) ?? DateTime.new(.AT-KEY(‚lastcheckoktime‘), :formatter(&ISO8601)) !! DateTime; 
                .AT-KEY(‚clicktrend‘) = .AT-KEY(‚clicktrend‘).Int;
                .AT-KEY(‚lastcheckok‘) = .AT-KEY(‚lastcheckok‘).Int.Bool;
                # .AT-KEY(‚tags‘) = .AT-KEY(‚tags‘).split(',');
                .AT-KEY(‚lastchecktime‘) = .AT-KEY(‚lastchecktime‘) ?? DateTime.new(.AT-KEY(‚lastchecktime‘), :formatter(&ISO8601)) !! DateTime; 
                .AT-KEY(‚stationuuid‘) = .AT-KEY(‚stationuuid‘).Int;

                internet-radio-batch-progress.emit: 1000 if $++ %% 1000;

                .Hash
            };
            
            my $elapsed = now - ENTER now;
            benchmark "$stations-count processed in {$elapsed}s with {$stations-count / $elapsed} stations/s";
        }

        my @deleted-stations;
        my @updated-stations;
        await(
            start { @deleted-stations = internet-radio-stations-deleted(since => $cache-time) },
            start { @updated-stations = internet-radio-stations-changed(since => $cache-time) }
        );

        note [ ‚total:‘, +@stations, ‚deleted:‘, +@deleted-stations, ‚changed:‘, +@updated-stations ];

        if +@deleted-stations | +@updated-stations {
            my Set $d-s-ids = @deleted-stations».<stationuuid>.Set;
            my Set $u-s-ids = @updated-stations».<stationuuid>.Set;
            my Set $to-be-removed-stations = $d-s-ids ∩ $u-s-ids;
            @stations = @stations.grep({
                my $station = .item;
                my $ret = .<stationuuid>.Int ∉ $to-be-removed-stations;
                CATCH { default { 
                    note .^name, ': ', .Str;
                    exit 0
                }
            }
                $ret
            });
            note ‚after delete: ‘, +@stations;

            # dd @stations.grep({say .<stationuuid>; .<stationuuid>.Int ∈ $to-be-removed-stations});
            # dd $to-be-removed-stations;

            @stations.append(|@updated-stations);
            note ‚after updated: ‘, +@stations;

            change-state storing;
            {
                my $json = '[' ~ @stations.hyper(:batch(1000), :degree(MAX_DEGREE)).map({.&to-json}).join(',') ~ ']';
                „$cfg-dir/stations.json“.IO.spurt($json);
                my $elapsed = now - ENTER now;
                benchmark "$stations-count stored in {$elapsed}s with {$stations-count / $elapsed} stations/s";
            }
        }
        change-state ready;
        benchmark ‚cache updated in: ‘ ~ (now - ENTER now) ~ ‚s‘;
    }
    @stations.Slip
}

multi sub internet-radio-stations-deleted(:$schema = 'http', DateTime :$since = now.DateTime.earlier(:30days)) is export {
    note "fetching deleted stations";
    my \v = jpost "$schema://de1.api.radio-browser.info/json/stations/deleted", {User-Agent => 'https://github.com/gfldex/perl6-www-radiobrowser'}
    note "processing deleted stations";
    v.hyper.map: { 
        .<lastchangetime> = DateTime.new: .<lastchangetime>.subst(' ', 'T') ~ 'Z'; 
        .<changeuuid> = .<changeuuid>.Int;
        .<stationuuid> = .<changeuuid>.Int;
        .Hash
    };
    v.grep: { .<lastchangetime> > $since }
}

multi sub internet-radio-stations-deleted(:$schema = 'http', :$age) is export {
    internet-radio-stations-deleted(:$schema, since => now.DateTime.earlier(|$age))
}

multi sub internet-radio-stations-changed(:$schema = 'http', DateTime :$since) is export {
    note "fetching changed stations";
    my \v = jpost "$schema://de1.api.radio-browser.info/json/stations/lastchange", {User-Agent => 'https://github.com/gfldex/perl6-www-radiobrowser'};
    note "processing changed stations";
    v.hyper.map: { 
        .<lastchangetime> = .<lastchangetime> ?? DateTime.new: .<lastchangetime>.subst(' ', 'T') ~ 'Z' !! DateTime; 
        .<clickcount> = .<clickcount>.Int;
        .<bitrate> = .<bitrate>.Int;
        .<clicktimestamp> = .<clicktimestamp> ?? DateTime.new: .<clicktimestamp>.subst(' ', 'T') ~ 'Z' !! DateTime; 
        .<votes> = .<votes>.Int;
        .<lastcheckoktime> = .<lastcheckoktime> ?? DateTime.new: .<lastcheckoktime>.subst(' ', 'T') ~ 'Z' !! DateTime; 
        .<clicktrend> = .<clicktrend>.Int;
        .<lastcheckok> = .<lastcheckok>.Int.Bool;
        .<tags> = .<tags>.split(',');
        .<lastchecktime> = .<lastchangetime> ?? DateTime.new: .<lastchecktime>.subst(' ', 'T') ~ 'Z' !! DateTime; 
        .<stationuuid> = .<stationuuid>.Int;
        .Hash
    };
    v.grep: { .<lastchangetime> > $since }
}

multi sub internet-radio-stations-changed(:$schema = 'http', :$age) is export {
    internet-radio-stations-changed(:$schema, since => now.DateTime.earlier(|$age))
}

multi sub fetch(:$stations-changed){
    
}
