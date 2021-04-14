const { Chunker } = require("./Chunker");
const { gqlRedis } = require("./bmUpstash");
const { Utils } = require("./Utils");
function CrusherPluginUpstashService() {
  const self = this;

  // these will be specific to your plugin
  let _settings = null;

  // prefixs on redis can be any string but
  // make sure we start and end with a single slash for consistency
  const fixPrefix = (prefix) =>
    ((prefix || "") + "/").replace(/^\/+/, "/").replace(/\/+$/, "/");

  // standard function to check store is present and of the correct type
  const checkStore = () => {
    if (!_settings.chunkSize)
      throw "You must provide the maximum chunksize supported";
    if (!_settings.prefix)
      throw "The prefix must be the path of a folder eg /crusher/store";
    if (!_settings.tokenService || typeof _settings.tokenService !== "function")
      throw "There must be a tokenservice function that returns an upstash access token";
    if (!_settings.fetcher || typeof _settings.fetcher !== "function")
      throw "There must be a fetch function that can do a urlfetch (url,options)";
    return self;
  };

  /**
   * @param {object} settings these will vary according to the type of store
   */
  self.init = function (settings) {
    _settings = settings || {};
    _settings.fetcher = _settings.fetcher || require("got");
    // the settings are the same as the crusher settings
    _settings.store = {
      ug: gqlRedis({
        fetcher: _settings.fetcher,
        tokenService: _settings.tokenService,
      }),
    };

    // make sure we start and end with a single slash
    _settings.prefix = fixPrefix(_settings.prefix);

    // upstash supports value sizes of up to 1mb - but actually it doesn't work above 400k for now.
    // see - https://github.com/upstash/issues/issues/3
    _settings.chunkSize = _settings.chunkSize || 400000;

    // respect digest can reduce the number of chunks read, but may return stale
    _settings.respectDigest = Utils.isUndefined(_settings.respectDigest)
      ? false
      : _settings.respectDigest;

    // must have a cache service and a chunksize, and the store must be valid
    checkStore();

    // now initialize the squeezer
    self.squeezer = new Chunker();

    self.squeezer
      .setStore(_settings.store)
      .setChunkSize(_settings.chunkSize)
      .funcWriteToStore(write)
      .funcReadFromStore(read)
      .funcRemoveObject(remove)
      .setRespectDigest(_settings.respectDigest)
      .setCompressMin(_settings.compressMin)
      .setUselz(true)
      .setPrefix(_settings.prefix);

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
   * @return {object} whatever you  like
   */
  const remove = async (store, key) => {
    checkStore();
    return await store.ug.execute("Del", key);
  };

  /**
   * write an item
   * @param {object} store whatever you initialized store with
   * @param {string} key the key to write
   * @param {string} str the string to write
   * @param {number} expiry time in secs .. ignored in drive
   * @param {string} propKey the parent KEY will be the same as key, except when multi records are being writt
   * @return {object} whatever you like
   */
  const write = async (store, key, str, expiry, propKey) => {
    checkStore();
    // upstash is capable of writing multiple chunks, so we'll queue up the the thing if there are any and we have a propKey to do it on
    if (!propKey) {
      throw new Error("propkey is missing for", key);
    }

    // actually upstash still limits the max payload to 400k,
    // so Mset isnt really helping
    // leave this code to work like this in case it gets changed in the future

    return (!expiry
      ? store.ug.execute("Set", key, str)
      : store.ug.execute("SetEX", key, str, expiry)
    ).then((result) => {
      if (result !== "OK")
        throw new Error("failed to set value for key", key, result);

      return result;
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
    return await store.ug.execute("Get", key);
  };
}
module.exports = {
  CrusherPluginUpstashService,
};
