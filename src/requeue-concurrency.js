// initialize NSQ connection
const NSQ = require("./nsq");
// start message requeue to deal with urls from parsed pages
NSQ.startRequeueSubscription();
