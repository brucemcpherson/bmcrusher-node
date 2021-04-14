const { Chunker } = require("./Chunker");
const { Utils } = require("./Utils");
const { Fetcher } = require("./Fetcher");
const Qottle = require("qottle");

function CrusherPluginGitService() {
  const self = this;

  // these will be specific to your plugin
  let _settings = null;

  // the prefix is the path in the repo to hold stuff like this
  const fixPrefix = (prefix) =>
    prefix ? (prefix + "/").replace(/\/+/g, "/").replace(/\/+$/, "/") : "";

  const checkStore = () => {
    if (!_settings.repo) throw "You must provide the repo to use";
    if (!_settings.owner) throw "You must provide the owner of the repo to use";
    if (!_settings.chunkSize)
      throw "You must provide the maximum chunksize supported";
    if (!_settings.prefix)
      throw "The prefix is the path in the repo to start storing data at";
    if (!_settings.tokenService || typeof _settings.tokenService !== "function")
      throw "There must be a tokenservice function that returns an oauth token";
    if (!_settings.fetcher || typeof _settings.fetcher !== "function")
      throw "There must be a fetch function that can do a urlfetch (url,options)";
    return self;
  };

  const getQuery = ({ store, key, getContent = false }) => {
    const { repo, owner, prefix } = store;
    const expression = "HEAD:" + prefix + key;
    return {
      query: `query ($repo: String! , $owner: String!, $expression: String) {
      repository(owner: $owner, name: $repo) {
        object(expression: $expression) {
          ... on Blob {
            oid
            ${getContent ? "text" : ""}
          }
        }
      }
    }`,
      variables: {
        repo,
        owner,
        expression,
      },
    };
  };

  /**
   * @param {object} settings these will vary according to the type of store
   */
  self.init = function (settings) {
    _settings = settings || {};
    _settings.prefix = fixPrefix(_settings.prefix);
    _settings.fetcher = _settings.fetcher || require("got");
    const fetchFunc = new Fetcher({
      fetcher: _settings.fetcher,
      tokenService: _settings.tokenService,
    }).got;

    // the github api doesnt like parallel requests from the same token, so we'll use this fetcher to queue them up
    const queue = new Qottle({ concurrent: 1 });
    const headers = {
      headers: {
        accept: "application/vnd.github.v3+json",
      },
    };
    const store = {
      rest: "https://api.github.com/",
      gql: "https://api.github.com/graphql",
      prefix: _settings.prefix,
      owner: _settings.owner,
      repo: _settings.repo,
      fetcher: (url, options = {}) =>
        queue
          .add(() => {
            return fetchFunc(url, { ...headers, ...options });
          })
          .then(({ result }) => result)
          .catch((err) => {
            console.log("console log- caught fetcher error", err);
            return Promise.reject(err);
          }),
    };

    // set default chunkzise for github (500k)
    _settings.chunkSize = _settings.chunkSize || 500000;

    // respect digest can reduce the number of chunks read, but may return stale
    _settings.respectDigest = Utils.isUndefined(_settings.respectDigest)
      ? false
      : _settings.respectDigest;

    // must have a cache service and a chunksize, and the store must be valid
    checkStore();

    // now initialize the squeezer
    self.squeezer = new Chunker()
    self.squeezer.setStore(store)
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
  const getUrl = (store, key) => {
    const { repo, owner, prefix } = store;
    return (
      store.rest +
      `repos/${owner}/${repo}/contents/${prefix}/${key}`.replace(/\/+/g, "/")
    );
  };

  // return your own settings
  self.getSettings = () => _settings;

  /**
   * remove an item
   * @param {string} key the key to remove
   * @return {object} whatever you  like
   */
  const remove = (store, key) => {
    checkStore();
    const url = getUrl(store, key);
    console.log("removing...", key);
    // so we need to get the sha in case its an update rather than a new entry
    return store
      .fetcher(url)
      .then((getItem) => {
        const sha =
          getItem && getItem.success && getItem.data && getItem.data.sha;
        // prepare the data

        if (!sha) return null;
        const body = {
          message: `bmcrusher:${key}`,
          sha,
        };
        return store
          .fetcher(url, {
            method: "DELETE",
            json: body,
          })
          .then((result) => {
            if (!result.success) {
              console.log("failed to delete", url, key, result);
            }
            return result;
          });
      })
      .catch((err) => {
        console.log("remove failed", err);
        return Promise.reject(err);
      });
  };

  const getGql = async (store, key) => {
    const json = getQuery({ store, key, getContent: true });
    return await store.fetcher(store.gql, {
      json,
      method: "POST",
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
    const url = getUrl(store, key);
    const body = {
      content: Buffer.from(str).toString("base64"),
      message: `bmcrusher:${key}`,
    };
    return store
      .fetcher(url, {
        json: body,
        method: "PUT",
      })
      .then((result) => {
        if (!result.success) {
          console.log("failed writing", result);
          return Promise.reject(result.content);
        } else {
          return result.data;
        }
      })
      .catch((err) => {
        console.log("failed writing ", key, err);
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
    const result = await getGql(store, key);

    const data = result && result.success && result.data && result.data.data;
    return (
      data &&
      data.repository &&
      data.repository.object &&
      data.repository.object.text
    );
  };
}
module.exports = {
  CrusherPluginGitService,
};
