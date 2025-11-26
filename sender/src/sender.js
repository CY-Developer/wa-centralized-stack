// sender/src/sender.js —— 队列消费，调用 deliver()
const { deliver } = require('./worker');

async function handle(job){
  try{
    await deliver(job.data);
    return { ok:true };
  }catch(e){
    console.error('[sender] deliver failed:', e.message || e);
    throw e;
  }
}

module.exports = { handle };
