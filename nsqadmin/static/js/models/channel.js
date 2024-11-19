var _ = require('underscore');

var AppState = require('../app_state');
var Backbone = require('backbone');

var Channel = Backbone.Model.extend({
    idAttribute: 'name',

    constructor: function Channel() {
        Backbone.Model.prototype.constructor.apply(this, arguments);
    },

    url: function() {
        return AppState.apiPath('/topics/' +
            encodeURIComponent(this.get('topic')) + '/' +
            encodeURIComponent(this.get('name')));
    },

    parse: function(response) {
        response['nodes'] = _.map(response['nodes'] || [], function(node) {
            var nodeParts = node['node'].split(':');
            var port = nodeParts.pop();
            var address = nodeParts.join(':');
            var hostname = node['hostname'];
            var zonecount = node['zone_local_msg_count'];
            var deliverycount = node['delivery_msg_count'];
            var regioncount = node['region_local_msg_count'];
            var globalcount = node['global_msg_count'];
            node['show_broadcast_address'] = hostname.toLowerCase() !== address.toLowerCase();
            node['hostname_port'] = hostname + ':' + port;
            node['zone_local_percentage'] = zonecount / deliverycount;
            node['region_local_percentage'] = regioncount / deliverycount;
            node['global_percentage'] = globalcount / deliverycount;
            if (isNaN(node['zone_local_percentage'])) {
                node['zone_local_percentage'] = 0;
            }
            if (isNaN(node['region_local_percentage'])) {
                node['region_local_percentage'] = 0;
            }
            if (isNaN(node['global_percentage'])) {
                node['global_percentage'] = 0;
            }
            return node;
        });

        response['clients'] = _.map(response['clients'] || [], function(client) {
            var clientId = client['client_id'];
            var hostname = client['hostname'];
            var shortHostname = hostname.split('.')[0];

            // ignore client_id if it's duplicative
            client['show_client_id'] = (clientId.toLowerCase() !== shortHostname.toLowerCase()
                                        && clientId.toLowerCase() !== hostname.toLowerCase());

            var port = client['remote_address'].split(':').pop();
            client['hostname_port'] = hostname + ':' + port;

            return client;
        });

        return response;
    }
});

module.exports = Channel;
