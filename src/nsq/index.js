// insert configuration file
const config = require('../../configuration.js')(process.env.NODE_ENV);

const _ = require('lodash');
const Promise = require('bluebird');
const URI = require('urijs');
const rp = require('request-promise');

const nsq = require('nsqjs');
const NSQwriter = new nsq.Writer(config.nsq.nsqdServer, config.nsq.wPort);
NSQwriter.connect();
NSQwriter.on('ready', function () {
  console.info(`NSQ Writer ready on ${config.nsq.nsqdServer}:${config.nsq.wPort}`);
});
NSQwriter.on('closed', function () {
  console.info('NSQ Writer closed Event');
});

const NSQreader = new nsq.Reader('trackinops.requeue-concurrency', 'Execute_requeue', {
  lookupdHTTPAddresses: config.nsq.lookupdHTTPAddresses,
  nsqdTCPAddresses: config.nsq.nsqdTCPAddresses
});
NSQreader.connect();
NSQreader.on('ready', function () {
  console.info(`NSQ Reader ready on nsqlookupd:${config.nsq.lookupdHTTPAddresses}`);
});
NSQreader.on('error', function (err) {
  console.error(`NSQ Reader error Event`);
  console.error(new Error(err));
});
NSQreader.on('closed', function () {
  console.info('NSQ Reader closed Event');
});

const levelup = require('level');
var LvlDB = levelup(config.levelup.location, config.levelup.options, function (err, db) {
  if (err) throw err
  console.log(db.db.getProperty('leveldb.num-files-at-level0'));
  console.log(db.db.getProperty('leveldb.stats'));
  console.log(db.db.getProperty('leveldb.sstables'));
});

process.on('SIGINT', function () {
  console.info("\nStarting shutting down from SIGINT (Ctrl-C)");
  // closing NSQwriter and NSQreader connections
  NSQwriter.close();
  NSQreader.close();
  process.exit(0);
})

const publishCrawlerRequest = function (topic = "trackinops.crawler.000", url, uniqueUrl, executionDoc) {
  return new Promise(function (resolve, reject) {
    NSQwriter.publish(topic, {
      url: url,
      uniqueUrl: uniqueUrl,
      executionDoc: executionDoc
    }, function (err) {
      if (err) {
        console.error(`NSQwriter Crawler Request publish Error: ${err.message}`);
        return reject(err);
      }
      console.info(`Crawler Request sent to NSQ, 150 chars: ${uniqueUrl.substring(0, 150)}`);
      return resolve();
    })
  })
}

const publishParserRequest = function (topic = "trackinops.parser.000", url, uniqueUrl, executionDoc) {
  return new Promise(function (resolve, reject) {
    NSQwriter.publish(topic, {
      url: url,
      uniqueUrl: uniqueUrl,
      executionDoc: executionDoc
    }, function (err) {
      if (err) {
        console.error(`NSQwriter Parser Request publish Error: ${err.message}`);
        return reject(err);
      }
      console.info(`Parser Request sent to NSQ, 150 chars: ${uniqueUrl.substring(0, 150)}`);
      return resolve();
    })
  })
}
/**
* @public
*  // @paramm {String} routingKey - where to requeue
*  // @paramm {String} type = routingKey - by default for crawler requests bindings
* @param {MongoId} requestsId
* @param {*} bodyData - data to include in a message
* @returns {Promise}
*
*/


const startRequeueSubscription = function () {
  NSQreader.on('message', function (msg) {
    console.log(msg.json());
    // console.log('Received message [%s]: %s', msg.id, msg.json().publishedMessageId);
    console.info(`Received message [${msg.id}]`);
    const crawlingHostname = new URI(msg.json().url).hostname();

    return Promise.all([
      crawlerRequeueConcurrency(crawlingHostname, msg.json()),
      parserRequeueConcurrency(crawlingHostname, msg.json())
    ]).then((got) => {
      // const gotFailed = _.compact(got);
      // if (gotFailed.length > 0) {
      //   console.info('Requeue Concurrency FAILED!');
      //   return msg.requeue(gotFailed[0].delay, backoff = true);
      // }
      console.info('Requeue Concurrency Successfull!');
      console.log(got);
      return msg.finish();
    }).catch((err) => {
      console.info('Requeue Concurrency FAILED!');
      console.error(err);
      if (err.failed && err.delay && err.backoff)
        return msg.requeue(delay = err.delay, backoff = err.backoff);
      return msg.requeue(delay = 0, backoff = true);
    });
  });
}

function crawlerRequeueConcurrency(crawlingHostname, msg) {
  return requeueConcurrency(crawlingHostname, 'trackinops.crawler.', msg)
    .then(() => {
      console.info('Crawler Requeue Concurrency Successfull!');
      return Promise.resolve();
    }).catch((err) => {
      console.info('Crawler Requeue Concurrency FAILED!');
      console.error(err);
      if (err.delay && err.backoff)
        return Promise.resolve({ failed: true, delay: 30, backoff: true });
      return Promise.resolve({ failed: true });
    });
}
function parserRequeueConcurrency(crawlingHostname, msg) {


  return parserUrlRegexMatches(msg.executionDoc.crawlMatches, msg.url)
    .then(() => {
      requeueConcurrency(crawlingHostname, 'trackinops.parser.', msg)
        .then(() => {
          console.info('Parser Requeue Concurrency Successfull!');
          return Promise.resolve();
        })
    }, (err) => {
      // if url didn't match regex, just skip it
      console.error(new Error(`${msg.url} didn't match any parserUrlRegexMatches`));
      return Promise.resolve();
    })
    .catch((err) => {
      console.info('Parser Requeue Concurrency FAILED!');
      console.error(err);
      if (err.delay && err.backoff)
        return Promise.resolve({ failed: true, delay: 30, backoff: true });
      return Promise.resolve({ failed: true });
    });
}

function parserUrlRegexMatches(crawlMatches, url) {
  return Promise.all(crawlMatches.map(function (cMatch) {
    let regexMatch = false;
    let checkUrl = url;
    if (!cMatch.urlRegEx) return cMatch;

    if (cMatch.urlRegEx !== null && cMatch.urlRegEx.length > 0) {
      // replaces [] to ()
      let matchStr = cMatch.urlRegEx.replace(/\[/g, '(').replace(/\]/g, ')');
      if (_.endsWith(matchStr, '/')) {
        // removes trailing slash
        matchStr = matchStr.substring(0, matchStr.length - 1);
      }
      if (_.endsWith(checkUrl, '/')) {
        checkUrl = checkUrl.substring(0, checkUrl.length - 1);
      }
      let patt = new RegExp('^' + matchStr + '$');
      regexMatch = patt.test(checkUrl);
    }
    if (regexMatch) return cMatch;
  }))
    .then(function (crawlMatchesAfterRegexCheck) {
      // evaluate elements on the page
      const regexMatched = _.compact(crawlMatchesAfterRegexCheck);
      console.info(url, 'regexMatched', regexMatched);
      if (regexMatched.length > 0) return Promise.resolve();
      return Promise.reject();
    }).catch((err) => {
      console.error(err);
      return Promise.reject();
    })
}

function requeueConcurrency(crawlingHostname, prefix, msg) {
  // query leveldb to find if hostname is already being crawled
  return levelDBCheckIfAlreadyCrawling(crawlingHostname, prefix) // returns founded {topic:key,hostname:value} pair or empty
    .then(
    (founded) => {
      // if exists publish to founded topic and finish message
      if (founded.topic) {
        // TODO: should check if this topic is alive and running
        return Queue.publishCrawlerRequest(founded.topic, msg.url, msg.uniqueUrl, msg.executionDoc)
          .error((err) => {
            console.error(new Error(err));
            return Promise.reject(new Error(err));
          });
      } else {
        // if not already crawling and LvlDB haven't failed
        // query lookupd for available/free crawler Topics
        return chooseTopicNameToPublish({ prefix: prefix, lookupd: config.nsq.lookupdHTTPAddresses })
          .then(
          (publishTo) => {
            console.info('publishTo', publishTo);
            // save given domain for chosen topic name in LvlDB
            return levelDBput(publishTo.topic, crawlingHostname)
              .then(() => {
                return Queue.publishCrawlerRequest(publishTo.topic, msg.url, msg.uniqueUrl, msg.executionDoc)
                  .error((err) => {
                    console.error(new Error(err));
                    return Promise.reject();
                  });
              },
              (err) => {
                // up to this point, if error happens
                // levelDB key is still not reserved for crawler
                // levelDBput arror
                console.error(new Error(err));

                return Promise.reject({ delay: 30, backoff: true });
              })
          },
          (err) => {
            // Lookupd query failed or no available/free Topics
            // lookupd/topics error
            console.error(new Error(err));

            return Promise.reject({ delay: 30, backoff: true });
          }
          )
      }
    },
    (err) => {
      // LvlDB error, requeue the message
      console.error(new Error(err));

      return Promise.reject({ delay: 30, backoff: true });
    }
    )
  // .catch((err) => {
  //   // TODO: check if all of the topics are working on  levelDB keys/values, and cleanup the keys
  //   console.error(new Error(err));
  //   return msg.requeue(delay = null, backoff = true);
  // })
}

function chooseTopicNameToPublish({ prefix, lookupd }) {
  return queryLookupdTopics(lookupd, prefix)
    .then(
    (availableTopics) => {
      // console.info(availableTopics);
      if (!availableTopics || availableTopics.length === 0)
        throw new Error(`No available topics for ${lookupd} with ${prefix} prefix`);
      return availableTopics[0]; // just return first available topic for publishing
      // TODO: we could get more information about heavy load of the clients and pick available by that
    },
    (err) => {
      console.error(new Error(err));
      throw new Error(err);
    })
}

function queryLookupdTopics(lookupdHTTPAddresses, prefix = 'trackinops.') {
  return rp({
    uri: `http://${lookupdHTTPAddresses}/nodes`,
    // qs: {
    //   access_token: 'xxxxx xxxxx' // -> uri + '?access_token=xxxxx%20xxxxx'
    // },
    headers: {
      'User-Agent': 'Request-Promise'
    },
    json: true // Automatically parses the JSON string in the response
  })
    .then(function (nodes) {
      // console.info('All available producers:', nodes.producers);
      let nodesInfo = nodes.producers.reduce((nsqds, producer) => {
        nsqds.push({
          broadcast_address: producer.broadcast_address,
          http_port: producer.http_port,
          topics: _.zipWith(producer.topics, producer.tombstones, (tpc, tmb) => {
            return {
              topic: tpc,
              tombstone: tmb
            };
          }).filter((rez) => { return _.startsWith(rez.topic, prefix) && !rez.tombstone; })
        });
        return nsqds;
      }, []);
      // drop nodes which have no Topics
      nodesInfo = nodesInfo.filter((rez) => { return rez.topics.length > 0 ? true : false });
      // console.info('Filtered:', nodesInfo);
      return nodesInfo;
    })
    .then(function (nsqds) {
      return Promise.all(nsqds.map((nsqd) => {
        return rp({
          uri: `http://${nsqd.broadcast_address}:${nsqd.http_port}/stats?format=json`,
          // qs: {
          //   access_token: 'xxxxx xxxxx' // -> uri + '?access_token=xxxxx%20xxxxx'
          // },
          headers: {
            'User-Agent': 'Request-Promise'
          },
          json: true // Automatically parses the JSON string in the response
        })
          .then(
          (nsqdStats) => {
            return nsqdStats.topics.filter((topic) => {
              return topic.depth === 0 // empty topic head
                && topic.backend_depth === 0 // also empty topic head backend
                && !topic.pause // topic is not paused
                && _.some(nsqd.topics, ['topic', topic.topic_name]); // nsqd topic_name is one of filtered from lookupd
            })
              .reduce((emptyTopics, filteredTopic) => {
                emptyTopics.push(
                  filteredTopic.channels.filter((channel) => {
                    return channel.channel_name === "Execute_request"
                      && channel.depth === 0 // empty channel
                      && channel.clients.length > 0 // channel has connected readers
                      && channel.backend_depth === 0 // also empty channel backend
                      && !channel.pause // channel is not paused
                      && channel.in_flight_count === 0; // currently not working on a job
                  }).map((filtered) => { return { topic: filteredTopic.topic_name, channel: filtered.channel_name } })
                );
                return emptyTopics;
              }, [])
          },
          (err) => {
            console.error(`Query nsqd ${nsqd.broadcast_address}:${nsqd.http_port} Stats failed: ${err}`);
            return {};
          })

      }))
        .then((filteredNsqds) => {
          return _.uniqWith(_.flattenDeep(filteredNsqds), _.isEqual);
        })
    })
    .catch(function (err) {
      // API call failed...
      throw new Error(`Lookupd Nodes request failed: ${err}`);
    });
}

function levelDBCheckIfAlreadyCrawling(hostname, prefix) {
  return new Promise(function (resolve, reject) {
    let founded = {};
    const readStream = LvlDB.createReadStream({ fillCache: false, gte: `${prefix}000`, lte: `${prefix}999` });
    readStream.on('data', function (data) {
      if (data.value === hostname) {
        founded = { topic: data.key, hostname: data.value };
        console.log(founded);
        // return first founded
        readStream.destroy();
        resolve(founded);
      }
    })
      .on('error', function (err) {
        console.log('LvlDB Stream Oh my!', err)
        readStream.destroy();
        reject(new Error(`LvlDB Read Stream error: ${err}`));
      })
      // .on('close', function () {
      //   console.log('LvlDB Read Stream closed')
      //   resolve(founded);
      // })
      .on('end', function () {
        console.log(`LvlDB Read Stream ended, Hostname:${hostname}`)
        // reject(new Error(`LvlDB Read Stream Hostname:${hostname} Not Found!`));
        resolve(founded);
      })
  })
}

function getHostname(url) {
  return new URI(url).hostname();
}


function matchesRegex(string, regex) {
  return (new RegExp(regex, 'i')).test(string);
}



function levelDBput(key, value) {
  return new Promise(function (resolve, reject) {
    LvlDB.put(key, value, { sync: true }, function (err) {
      if (err) reject(new Error(`LevelDB key ${key} put error: ${err}`));
      resolve();
    })
  })
}

function levelDBkeyNotFound(key) {
  return new Promise(function (resolve, reject) {
    LvlDB.get(key, { fillCache: true, asBuffer: true }, function (err, value) {
      if (err) {
        if (err.notFound) {
          // handle a 'NotFoundError' here
          return resolve();
        }
        // I/O or other error, pass it up the callback chain
        // return callback(err)
        reject(new Error(`LevelDB key: ${key}, keyNotFound error: ${err}`));
      }
      // .. handle `value` here
      reject(new Error(`LevelDB key already found: ${key}`));
    })
  })
}

function levelDBdel() {

}

exports = module.exports = Queue = {
  publishCrawlerRequest: publishCrawlerRequest,
  publishParserRequest: publishParserRequest,
  startRequeueSubscription: startRequeueSubscription
};
