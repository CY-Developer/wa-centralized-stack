const { Queue, QueueScheduler } = require('bullmq');
const IORedis = require('ioredis');
const { CFG } = require('./config');

const connection = new IORedis(CFG.redisUrl, { maxRetriesPerRequest: null });
const sendQueue = new Queue('wa:send', { connection });
new QueueScheduler('wa:send', { connection });

module.exports = { sendQueue, connection };
