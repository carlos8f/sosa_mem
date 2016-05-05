var crypto = require('crypto');

function PlainObject () {}
PlainObject.prototype = Object.create(null);
function newObj () { return new PlainObject }

var globalCache = newObj();

module.exports = function (coll_name, backend_options) {
  backend_options || (backend_options = {});

  function escapeBase64 (str) {
    return str.replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '')
  }

  function hash (id) {
    return backend_options.hashKeys ? escapeBase64(crypto.createHash('sha1').update(id).digest('base64')) : id.replace(/\./g, '')
  }

  var coll_path = coll_name;
  if (backend_options.key_prefix && backend_options.key_prefix.length) {
    coll_path += '.' + backend_options.key_prefix.map(hash).join('.')
  }

  globalCache[coll_path] || (globalCache[coll_path] = {keys: [], values: newObj()});
  var coll = globalCache[coll_path];

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

module.exports.globalCache = globalCache;