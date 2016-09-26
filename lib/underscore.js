/**
 * Created by marcosduarte on 9/22/16.
 */

var _ = require('underscore');

_.mixin({
    capitalize: function(string) {
        return string.charAt(0).toUpperCase() + string.substring(1).toLowerCase();
    }
});

module.exports = _;