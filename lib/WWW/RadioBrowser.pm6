use v6.c;
use WWW;

constant term:<GMT> = :timezone(0);

sub internet-radio-stations(:$schema = 'http', :$limit = 100000) is export {
    my $local-file-time = DateTime.now(|GMT);
    jpost "$schema://www.radio-browser.info/webservice/json/stations", {User-Agent => 'https://github.com/gfldex/perl6-www-radiobrowser'}, :$limit
}

multi sub internet-radio-stations-deleted(:$schema = 'http', DateTime :$since = now.DateTime.earlier(:30days)) is export {
    my $local-file-time = DateTime.now(|GMT);
    my \v = jpost "$schema://www.radio-browser.info/webservice/json/stations/deleted", {User-Agent => 'https://github.com/gfldex/perl6-www-radiobrowser'}
    v.map: { 
        .<lastchangetime> = DateTime.new: .<lastchangetime>.subst(' ', 'T') ~ 'Z'; 
        .<changeid> = .<changeid>.Int;
        .<id> = .<changeid>.Int;
        .Hash
    };
    v.grep: { .<lastchangetime> > $since }
}

multi sub internet-radio-stations-deleted(:$schema = 'http', :$age) is export {
    internet-radio-stations-deleted(:$schema, since => now.DateTime.earlier(|$age))
}

multi sub internet-radio-stations-changed(:$schema = 'http', DateTime :$since) is export {
    my $local-file-time = DateTime.now(|GMT);
    my \v = jpost "$schema://www.radio-browser.info/webservice/json/stations/deleted", {User-Agent => 'https://github.com/gfldex/perl6-www-radiobrowser'};
    v.map: { 
        .<lastchangetime> = DateTime.new: .<lastchangetime>.subst(' ', 'T') ~ 'Z'; 
        .<changeid> = .<changeid>.Int;
        .<id> = .<changeid>.Int;
        .Hash
    };
    v.grep: { .<lastchangetime> > $since }
}

multi sub internet-radio-stations-changed(:$schema = 'http', :$age) is export {
    internet-radio-stations-changed(:$schema, since => now.DateTime.earlier(|$age))
}

