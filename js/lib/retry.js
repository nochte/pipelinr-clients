const sleep = require('./sleep');
// if fn throws an error, try again in a bit
// if fn throws an error for too long, return (null, er)
// if fn succeeds, return (ret, null)
const retry = async (fn, retrycount = 10, retrybackoff = 1000) => {
  return new Promise(async (resolve, reject) => {
    let count = 0;
    for (;;) {
      try {
        resolve(await fn());
        return; // separate lines, so that resolve the await fn will throw
      } catch (er) {
        if(process.env.DEBUG) {
          console.log('error returned, retrying', er);
        }
        count += 1;
        if (count >= retrycount) {
          return reject(er);
        }
        await sleep(retrybackoff*count);
      }
    }
  });
}

module.exports= retry;