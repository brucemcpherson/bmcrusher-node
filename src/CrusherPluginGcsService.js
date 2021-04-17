const { Chunker } = require("./Chunker");
const { Utils } = require("./Utils");
const {
  createWriteStream,
  getBucket,
  stringToStream,
  streamToString,
  createReadStream,
  removeFile,
} = require("./bmgcs");

function CrusherPluginGcsService() {
  const self = this;

  // these will be specific to your plugin
  let _settings = null;

  const checkStore = () => {
    if (!_settings.bucketName) throw "You must provide the bucket name to use";
    if (!_settings.chunkSize)
      throw "You must provide the maximum chunksize supported";
    if (!_settings.prefix)
      throw "The prefix is the path in the repo to start storing data at";
    if (!_settings.tokenService || typeof _settings.tokenService !== "function")
      throw "There must be a tokenservice function that returns credentials";
    return self;
  };

  /**
   * @param {object} settings these will vary according to the type of store
   */
  self.init = function (settings) {
    _settings = settings || {};
    _settings.prefix = _settings.prefix || "";

    // set default chunkzise for gcs (4000000)
    _settings.chunkSize = _settings.chunkSize || 4000000;

    // respect digest can reduce the number of chunks read, but may return stale
    _settings.respectDigest = Utils.isUndefined(_settings.respectDigest)
      ? false
      : _settings.respectDigest;

    // must have a cache service and a chunksize, and the store must be valid
    checkStore();

    const bucket = getBucket({
      credentials: _settings.tokenService(),
      bucketName: _settings.bucketName,
    });

    const store = {
      bucket,
      prefix: _settings.prefix,
    };

    // now initialize the squeezer
    self.squeezer = new Chunker();
    self.squeezer
      .setStore(store)
      .setChunkSize(_settings.chunkSize)
      .funcWriteToStore(write)
      .funcReadFromStore(read)
      .funcRemoveObject(remove)
      .setRespectDigest(_settings.respectDigest)
      .setCompressMin(_settings.compressMin)
      .setUselz(true)
      // the prefix is handled in the store, so we can ignore it here
      .setPrefix("");

    // export the verbs
    self.put = (...vargs) => self.squeezer.setBigProperty(...vargs);
    self.get = (...vargs) => self.squeezer.getBigProperty(...vargs);
    self.remove = (...vargs) => self.squeezer.removeBigProperty(...vargs);
    return self;
  };

  // return your own settings
  self.getSettings = () => _settings;

  /**
   * remove an item
   * @param {string} key the key to remove
   * @return {number} statusCode
   */
  const remove = (store, key) => {
    checkStore();
    console.log("removing...", key);
    return removeFile({
      ...store,
      key,
    })
      .then((r) => {
        let { code, statusCode, message, statusMessage } = r;
        code = code || statusCode;
        message = message || statusMessage;
        if (!Math.floor(code / 100) === 2) {
          console.log("failed to delete", code, message);
          return Promise.reject(r);
        } else {
          return r;
        }
      })
      .catch((err) => {
        if (err.code === 404) {
          // just didnt exit so thats ok
          console.log("...didnt exist", key);
          return Promise.resolve(null);
        }
        console.log("error removing", err);
        return Promise.reject(err);
      });
  };

  /**
   * write an item
   * @param {object} store whatever you initialized store with
   * @param {string} key the key to write
   * @param {string} str the string to write
   * @param {number} expiry time in secs .. ignored in drive
   * @return {object} whatever you lik
   */
  const write = async (store, key, str = "", expiry) => {
    checkStore();
    console.log("...writing", key);
    const writeStream = createWriteStream({
      ...store,
      key,
    });
    return stringToStream({ writeStream, content: str }).catch((err) => {
      console.log("failed to write", key, err);
      return Promise.reject(err);
    });
  };

  /**
   * read an item
   * @param {object} store whatever you initialized store with
   * @param {string} key the key to write
   * @return {object} whatever you like
   */
  const read = async (store, key) => {
    checkStore();
    const readStream = createReadStream({
      ...store,
      key,
    });
    console.log("...reading", key);
    return streamToString({ readStream }).catch((err) => {
      // it's ok for it not to exist
      if (err.code === 404) {
        console.log("...didnt exist", key);
        return Promise.resolve(null);
      }
      console.log("read failure", key);
      return Promise.reject(err);
    });
  };
}
module.exports = {
  CrusherPluginGcsService,
};
