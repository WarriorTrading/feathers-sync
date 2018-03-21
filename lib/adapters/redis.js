const redis = require('redis');
const debug = require('debug')('feathers-sync:redis');
const core = require('../core');

function findMergingEvent ({event, path}, mergingEvents) {
  for (let i = mergingEvents.length - 1; i >= 0; i--) {
    let item = mergingEvents[i];
    if (event === item.event && path === item.path) {
      return i;
    }
  }

  return -1;
}

module.exports = config => {
  return app => {
    const db = config.uri || config.db;
    const pub = redis.createClient(db);
    const sub = redis.createClient(db);
    const key = config.key || 'feathers-sync';
    const mergingEvents = (config.mergingEvents || []);
    const mergingInteval = config.mergingInteval || 1000;

    debug(`Setting up Redis client for ${db}`);

    app.configure(core);
    app.sync = {
      pub,
      sub,
      type: 'redis',
      ready: new Promise((resolve, reject) => {
        sub.once('ready', resolve);
        sub.once('error', reject);
      })
    };

    // init merging events storage
    app.eventsStorage = {}; 

    app.on('sync-out', data => {
      debug(`Publishing key ${key} to Redis`);
      delete data.context;
      pub.publish(key, JSON.stringify(data));
    });

    sub.subscribe(key);
    sub.on('message', function (e, data) {
      if (e === key) {
        debug(`Got ${key} message from Redis`);
        let obj = JSON.parse(data);

        // check whether or not to merge the event to storage
        let i = findMergingEvent(obj, mergingEvents);
        if (i >= 0) {
          // build key of storage
          let key = obj.path + ':' + obj.event;
          let propertyForGrouping = mergingEvents[i].propertyForGrouping;

          if (propertyForGrouping) {
            key = key + ':' + obj.data[propertyForGrouping];
          }

          // get storage with key
          if (!app.eventsStorage.hasOwnProperty(key)) {
            let s = {
              event: obj.event,
              path: obj.path,
              propertyForGrouping, // for example : roomId
              valueToProperty: obj.data[propertyForGrouping], // for example : 10001
              cache: []
            };
            app.eventsStorage[key] = s;
          }
          let storage = app.eventsStorage[key];

          // store current event data in storage
          storage.cache.push(obj.data);

          // set timer to emit grouped events in one shot
          if (storage.timer == null) {
            storage.timer = setTimeout(() => {
              let d = {list: storage.cache.splice(0, storage.cache.length)};
              if (storage.propertyForGrouping) {
                d[storage.propertyForGrouping] = storage.valueToProperty;
              }

              let mergingData = {
                path: obj.path,
                event: obj.event,
                data: d
              };
              app.emit('sync-in', mergingData);
              storage.timer = null;
            }, mergingInteval);
          }
        } else {
          app.emit('sync-in', obj);
        }
      }
    });
  };
};
