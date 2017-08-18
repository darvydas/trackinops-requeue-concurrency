const development = {
  NODE_ENV: process.env.NODE_ENV,
  NODE_LOG_LEVEL: process.env.NODE_LOG_LEVEL,
  levelup: {
    location: './DB/levelDB',
    options: {
      createIfMissing: true,
      errorIfExists: false,
      compression: true,
      cacheSize: 100 * 8 * 1024 * 1024,
      keyEncoding: 'utf8',
      valueEncoding: 'json'
    }
  },
  nsq: {
    server: '127.0.0.1',
    wPort: 32769, // TCP nsqd Write Port, default: 4150
    rPort: 32770, // HTTP nsqlookupd Read Port, default: 4161
    nsqdTCPAddresses: [`nsqd-inDocker:4150`],
    lookupdHTTPAddresses: ['127.0.0.1:32770'], // HTTP default: '127.0.0.1:4161'
    readerOptions: {
      maxInFlight: 1,
      maxBackoffDuration: 600, // seconds
      maxAttempts: 100,
      requeueDelay: 90,
      nsqdTCPAddresses: [`nsqd-inDocker:4150`],
      lookupdHTTPAddresses: ['nsqlookupd:4161'], // HTTP default: '127.0.0.1:4161'
      messageTimeout: 30 * 1000 // 30 secs
    }
  }
};
const production = {
  levelup: {
    location: './DB/nsqd-topic',
    options: {
      createIfMissing: true,
      errorIfExists: false,
      compression: true,
      cacheSize: 8 * 1024 * 1024 * 1024,
      keyEncoding: 'utf8',
      valueEncoding: 'json'
    }
  },
  nsq: {
    nsqdServer: 'nsqd', // '127.0.0.1'
    wPort: 4150, // TCP nsqd Write Port, default: 4150
    rPort: 4161, // HTTP nsqlookupd Read Port, default: 4161
    nsqdTCPAddresses: ['nsqd:4150'], // [`nsqd-inDocker:4150`],
    lookupdHTTPAddresses: ['nsqlookupd:4161'], // ['nsqlookupd:4161'] // HTTP default: '127.0.0.1:4161'
    readerOptions: {
      maxInFlight: 1,
      maxBackoffDuration: 60 * 60, // 1 hour
      maxAttempts: 50,
      requeueDelay: 90,
      nsqdTCPAddresses: [`nsqd:4150`],
      lookupdHTTPAddresses: ['nsqlookupd:4161'], // HTTP default: '127.0.0.1:4161'
      messageTimeout: 60 * 1000 // 1 min
    }
  }

};
module.exports = function (env) {
  if (env === 'production')
    return production;

  if (env === 'test')
    return development;

  if (!env || env === 'dev' || env === 'development')
    return development;
}
