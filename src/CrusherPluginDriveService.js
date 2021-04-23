const { Chunker } = require("./Chunker");
const { Utils } = require("./Utils");
const {
  getClient,
  createFile,
  getFile,
  getFolder,
  removeFile,
  getFilesByName,
} = require("./bmdrive");

function CrusherPluginDriveService() {
  const self = this;

  // these will be specific to your plugin
  let _settings = null;

  const checkStore = () => {
    if (!_settings.chunkSize)
      throw "You must provide the maximum chunksize supported";
    if (!_settings.prefix)
      throw "The prefix is the path on drive to start storing data at";
    if (!_settings.tokenService || typeof _settings.tokenService !== "function")
      throw "There must be a tokenservice function that returns credentials";
    if (!_settings.subject || typeof _settings.subject !== "string")
      throw "you need a subject option - an email address to impersonate";
    return self;
  };

  /**
   * @param {object} settings these will vary according to the type of store
   */
  self.init = async (settings) => {
    _settings = settings || {};
    _settings.prefix = _settings.prefix || "";

    // set default chunkzise for drive (4000000)
    _settings.chunkSize = _settings.chunkSize || 4000000;

    // respect digest can reduce the number of chunks read, but may return stale
    _settings.respectDigest = Utils.isUndefined(_settings.respectDigest)
      ? false
      : _settings.respectDigest;

    // must have a cache service and a chunksize, and the store must be valid
    checkStore();

    const store = {};
    return await getClient({
      credentials: _settings.tokenService(),
      subject: _settings.subject,
    })
      .then((client) => {
        store.client = client;
        return getFolder({ client, path: _settings.prefix });
      })
      .then((res) => {
        const folderId = res && res.id;
        if (!folderId)
          throw new Error("folder not found for prefix: " + _settings.prefix);
        store.parents = [folderId];

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
          // this is not implemented to do anything yet
          // but we can improve by using this to avoid reading content when just removing 
          // chunked files
          .funcExistFromStore(exist)
          .setUselz(true)
          .setPrefix("");

        // export the verbs
        self.put = (...vargs) => self.squeezer.setBigProperty(...vargs);
        self.get = (...vargs) => self.squeezer.getBigProperty(...vargs);
        self.remove = (...vargs) => self.squeezer.removeBigProperty(...vargs);
        return self;
      });
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
    return exist(store,key)
      .then((files) => {
        console.log("....removing", key);
        // remove all files of the same same name
        if (files && files.length) {
          return Promise.all(
            files.map((file) => {
              return removeFile({
                ...store,
                fileId: file.id,
              });
            })
          );
        }
      })

      .then((r) => {
        let { code, statusCode, message, statusMessage } = r[0];
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
    return createFile({
      name: key,
      client: store.client,
      content: str,
      parents: store.parents,
      mimeType: "application/json",
    }).then((res) => {

      if (res.status !== 200) {
        console.log("failed to write", res.status, res.statusText, key);
        return res;
      } else {
        console.log("....created file", key, res.data.id);
        return res;
      }
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
    return exist(store, key).then((files) => {
      // just get the first one.
      if (files && files.length > 1) {
        console.log('warning - multiple matches for ', key, files)
      }
      const fileId = files && files[0] && files[0].id;
      if (!fileId) {
        return Promise.resolve(null);
      }
      return getFile({ fileId, client: store.client })
        .then((res) => {
          return res && res.content;
        })
        .catch((err) => {
          // it's ok for it not to exist
          if (err.response.status === 404) {
            console.log("...didnt exist", key);
            return Promise.resolve(null);
          }
          console.log(
            "read failure",
            key,
            err.response.status,
            err.response.stausText
          );
          return Promise.reject(err);
        });
    });
  };

  /**
   * exist - this is mainly to cater for backends that can have multiple files with the same name
   * @param {object} store whatever you initialized store with
   * @param {string} key the key to write
   * @return {object} whatever you like
   */
  const exist = async (store, key) => {
    checkStore();
    return getFilesByName({
      ...store,
      name: key,
    })
  };
}
module.exports = {
  CrusherPluginDriveService,
};
