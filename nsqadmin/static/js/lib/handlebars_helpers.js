var $ = require('jquery');
var _ = require('underscore');
var Handlebars = require('hbsfy/runtime');

var AppState = require('../app_state');

var formatStatsdKey = function(metricType, key) {
    var fullKey = key;
    var fmt;

    if (metricType === 'counter') {
        fmt = AppState.get('STATSD_COUNTER_FORMAT');
        fullKey = fmt.replace(/%s/g, key);
    } else if (metricType === 'gauge') {
        fmt = AppState.get('STATSD_GAUGE_FORMAT');
        fullKey = fmt.replace(/%s/g, key);
    }

    return fullKey;
};

var statsdPrefix = function(host) {
    var prefix = AppState.get('STATSD_PREFIX');
    var statsdHostKey = host.replace(/[\.:]/g, '_');
    prefix = prefix.replace(/%s/g, statsdHostKey);
    if (prefix.substring(prefix.length, 1) !== '.') {
        prefix += '.';
    }
    return prefix;
};

/* eslint-disable key-spacing */
var metricType = function(key) {
    return {
        'depth':                  'gauge',
        'in_flight_count':        'gauge',
        'deferred_count':         'gauge',
        'requeue_count':          'counter',
        'timeout_count':          'counter',
        'message_count':          'counter',
        'clients':                'gauge',
        '*_bytes':                'gauge',
        'gc_pause_*':             'gauge',
        'gc_runs':                'counter',
        'heap_objects':           'gauge',
        'e2e_processing_latency': 'gauge'
    }[key];
};
/* eslint-enable key-spacing */

var genColorList = function(typ, key) {
    if (typ === 'topic' || typ === 'channel') {
        if (key === 'depth' || key === 'deferred_count') {
            return 'red';
        }
    } else if (typ === 'node') {
        return 'red,green,blue,purple';
    } else if (typ === 'counter') {
        return 'green';
    }
    return 'blue';
};

// sanitizeGraphiteKey removes special characters from a graphite key
// this matches behavior of bitly/statsdaemon
// eslint-disable-next-line max-len
// https://github.com/bitly/statsdaemon/blob/fc46d9cfe29b674a0c8abc723afaa9370430cdcd/statsdaemon.go#L64-L88
var sanitizeGraphiteKey = function(s) {
    return s.replaceAll(' ', '_').replaceAll('/', '-').replaceAll(/[^a-zA-Z0-9-_.]/g, '');
};

var genTargets = function(typ, node, ns1, ns2, key) {
    var targets = [];
    var prefix = statsdPrefix(node ? node : '*');
    var fullKey;
    var target;
    if (typ === 'topic') {
        fullKey = formatStatsdKey(metricType(key),
            prefix + 'topic.' + sanitizeGraphiteKey(ns1) + '.' + key);
        targets.push('sumSeries(' + fullKey + ')');
    } else if (typ === 'channel') {
        fullKey = formatStatsdKey(metricType(key),
            prefix + 'topic.' + sanitizeGraphiteKey(ns1) + '.channel.' +
            sanitizeGraphiteKey(ns2) + '.' + key);
        targets.push('sumSeries(' + fullKey + ')');
    } else if (typ === 'node') {
        target = prefix + 'mem.' + key;
        if (key === 'gc_runs') {
            target = 'movingAverage(' + target + ',45)';
        }
        targets.push(formatStatsdKey(metricType(key), target));
    } else if (typ === 'e2e') {
        targets = _.map(ns1['percentiles'], function(p) {
            var t;
            if (ns1['channel'] !== '') {
                t = prefix + 'topic.' + ns1['topic'] + '.channel.' + ns1['channel'] + '.' +
                    key + '_' + (p['quantile'] * 100);
            } else {
                t = prefix + 'topic.' + ns1['topic'] + '.' + key + '_' + (p['quantile'] * 100);
            }
            if (node === '*') {
                t = 'averageSeries(' + t + ')';
            }
            return 'scale(' + formatStatsdKey(metricType(key), t) + ',0.000001)';
        });
    } else if (typ === 'counter') {
        fullKey = formatStatsdKey(metricType(key), prefix + 'topic.*.channel.*.' + key);
        targets.push('sumSeries(' + fullKey + ')');
    }
    return targets;
};

Handlebars.registerHelper('default', function(x, defaultValue) {
    return x ? x : defaultValue;
});

Handlebars.registerHelper('ifeq', function(a, b, options) {
    return (a === b) ? options.fn(this) : options.inverse(this);
});

Handlebars.registerHelper('unlesseq', function(a, b, options) {
    return (a !== b) ? options.fn(this) : options.inverse(this);
});

Handlebars.registerHelper('ifgteq', function(a, b, options) {
    return (a >= b) ? options.fn(this) : options.inverse(this);
});

Handlebars.registerHelper('iflteq', function(a, b, options) {
    return (a <= b) ? options.fn(this) : options.inverse(this);
});

Handlebars.registerHelper('length', function(xs) {
    return xs.length;
});

Handlebars.registerHelper('lowercase', function(s) {
    return s.toLowerCase();
});

Handlebars.registerHelper('uppercase', function(s) {
    return s.toUpperCase();
});

// this helper is inclusive of the top number
Handlebars.registerHelper('for', function(from, to, incr, block) {
    var accum = '';
    for (var i = from; i <= to; i += incr) {
        accum += block.fn(i);
    }
    return accum;
});

// Logical operators as helper functions, which can be useful when used within
// an `if` or `unless` block via the new helper composition syntax, like so:
//
//     {{#if (or step.unlocked step.is_finished)}}
//       Step is unlocked or finished!
//     {{/if}}
//
// Any number of arguments may be given to either helper. NOTE: _.initial() is
// used below because every helper takes an options hash as its last argument.
Handlebars.registerHelper('and', function() {
    return _.all(_.initial(arguments));
});

Handlebars.registerHelper('or', function() {
    return _.any(_.initial(arguments));
});

Handlebars.registerHelper('eq', function(a, b) {
    return a === b;
});

Handlebars.registerHelper('neq', function(a, b) {
    return a !== b;
});

Handlebars.registerHelper('urlencode', function(a) {
    return encodeURIComponent(a);
});

Handlebars.registerHelper('floatToPercent', function(f) {
    return Math.floor(f * 100);
});

Handlebars.registerHelper('floatToDecimalPercent', function(f) {
    return parseFloat((f * 100).toFixed(2));
});

Handlebars.registerHelper('percSuffix', function(f) {
    var v = Math.floor(f * 100) % 10;
    if (v === 1) {
        return 'st';
    } else if (v === 2) {
        return 'nd';
    } else if (v === 3) {
        return 'rd';
    }
    return 'th';
});

Handlebars.registerHelper('commafy', function(n) {
    n = n || 0;
    return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
});

function round(num, places) {
    var multiplier = Math.pow(10, places);
    return Math.round(num * multiplier) / multiplier;
}

Handlebars.registerHelper('nanotohuman', function(n) {
    var s = '';
    var v;
    if (n >= 3600000000000) {
        v = Math.floor(n / 3600000000000);
        n = n % 3600000000000;
        s = v + 'h';
    }
    if (n >= 60000000000) {
        v = Math.floor(n / 60000000000);
        n = n % 60000000000;
        s += v + 'm';
    }
    if (n >= 1000000000) {
        n = round(n / 1000000000, 2);
        s += n + 's';
    } else if (n >= 1000000) {
        n = round(n / 1000000, 2);
        s += n + 'ms';
    } else if (n >= 1000) {
        n = round(n / 1000, 2);
        s += n + 'us';
    } else {
        s = n + 'ns';
    }
    return s;
});

Handlebars.registerHelper('sparkline', function(typ, node, ns1, ns2, key) {
    var q = {
        'colorList': genColorList(typ, key),
        'height': '20',
        'width': '120',
        'hideGrid': 'true',
        'hideLegend': 'true',
        'hideAxes': 'true',
        'bgcolor': 'ff000000', // transparent
        'fgcolor': 'black',
        'margin': '0',
        'yMin': '0',
        'lineMode': 'connected',
        'drawNullAsZero': 'false',
        'from': '-' + AppState.get('graph_interval'),
        'until': '-1min'
    };

    var interval = AppState.get('STATSD_INTERVAL') + 'sec';
    q['target'] = _.map(genTargets(typ, node, ns1, ns2, key), function(t) {
        return 'summarize(' + t + ',"' + interval + '","avg")';
    });

    return AppState.get('GRAPHITE_URL') + '/render?' + $.param(q, true);
});

Handlebars.registerHelper('large_graph', function(typ, node, ns1, ns2, key) {
    var q = {
        'colorList': genColorList(typ, key),
        'height': '450',
        'width': '800',
        'bgcolor': 'ff000000', // transparent
        'fgcolor': '999999',
        'yMin': '0',
        'lineMode': 'connected',
        'drawNullAsZero': 'false',
        'from': '-' + AppState.get('graph_interval'),
        'until': '-1min'
    };

    var interval = AppState.get('STATSD_INTERVAL') + 'sec';
    q['target'] = _.map(genTargets(typ, node, ns1, ns2, key), function(t) {
        if (metricType(key) === 'counter') {
            var scale = 1 / AppState.get('STATSD_INTERVAL');
            t = 'scale(' + t + ',' + scale + ')';
        }
        return 'summarize(' + t + ',"' + interval + '","avg")';
    });

    return AppState.get('GRAPHITE_URL') + '/render?' + $.param(q, true);
});

Handlebars.registerHelper('rate', function(typ, node, ns1, ns2) {
    return genTargets(typ, node, ns1, ns2, 'message_count')[0];
});

Handlebars.registerPartial('error', require('../views/error.hbs'));
Handlebars.registerPartial('warning', require('../views/warning.hbs'));

Handlebars.registerHelper('basePath', function(p) {
    return AppState.basePath(p);
});
