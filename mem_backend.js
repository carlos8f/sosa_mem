function PlainObject () {}
PlainObject.prototype = Object.create(null);
function obj () { return new PlainObject }

var crypto = require('crypto');
var mem = obj();

function hash (id) {
  return crypto.createHash('sha1').update(JSON.stringify(id)).digest('hex');
}

module.exports = function (coll_name, backend_options) {
  backend_options || (backend_options = {});
  var coll_path = [backend_options.prefix, coll_name].concat(backend_options.key_prefix);
  var key = hash(coll_path);
  mem[key] || (mem[key] = {keys: [], values: obj()});
  var coll = mem[key];
  return {
    _copy: function (obj) {
      return JSON.parse(JSON.stringify(obj));
    },
    load: function (id, opts, cb) {
      var key = hash(id);
      cb(null, this._copy(coll.values[key] || null));
    },
    save: function (id, obj, opts, cb) {
      var key = hash(id);
      coll.values[key] = this._copy(obj);
      if (!~coll.keys.indexOf(id)) coll.keys.push(id);
      cb(null, coll.values[key]);
    },
    destroy: function (id, opts, cb) {
      var self = this;
      this.load(id, opts, function (err, obj) {
        if (err) return cb(err);
        if (obj) {
          var idx = coll.keys.indexOf(id);
          if (idx !== -1) coll.keys.splice(idx, 1);
          var key = hash(id);
          delete coll.values[key];
        }
        cb(null, self._copy(obj || null));
      });
    },
    select: function (opts, cb) {
      var self = this;
      var keys = coll.keys.slice();
      if (opts.filter) keys = keys.filter(opts.filter);
      if (opts.reverse) keys.reverse();
      var begin = opts.offset || 0;
      var end = opts.limit ? begin + opts.limit : undefined;
      if (begin || end) keys = keys.slice(begin, end);
      var objs = keys.map(function (id) {
        var key = hash(id);
        return self._copy(coll.values[key] || null);
      });
      cb(null, objs);
    }
  };
};
