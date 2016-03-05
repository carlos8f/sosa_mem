var crypto = require('crypto');
var mem = {};

function hash (id) {
  return crypto.createHash('sha1').update(JSON.stringify(id)).digest('hex');
}

module.exports = function (coll_name, backend_options) {
  backend_options || (backend_options = {});
  var coll_path = [backend_options.prefix, coll_name].concat(backend_options.key_prefix);
  var key = hash(coll_path);
  mem[key] || (mem[key] = {keys: [], values: {}});
  var coll = mem[key];
  return {
    load: function (id, opts, cb) {
      var key = hash(id);
      cb(null, coll.values[key] || null);
    },
    save: function (id, obj, opts, cb) {
      var key = hash(id);
      coll.values[key] = obj;
      if (!~coll.keys.indexOf(id)) coll.keys.push(id);
      cb(null, obj);
    },
    destroy: function (id, opts, cb) {
      this.load(id, opts, function (err, obj) {
        if (err) return cb(err);
        if (obj) {
          var idx = coll.keys.indexOf(id);
          if (idx !== -1) coll.keys.splice(idx, 1);
          var key = hash(id);
          delete coll.values[key];
        }
        cb(null, obj || null);
      });
    },
    select: function (opts, cb) {
      var objs = coll.keys.map(function (id) {
        var key = hash(id);
        return coll.values[key] || null;
      });
      cb(null, objs);
    }
  };
};
