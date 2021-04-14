const { Utils } = require("./Utils");
const { Compress } = require("./Compress");
class Chunker {
  constructor() {
    // the default maximum chunksize
    this._chunkSize = 9 * 1024;
    this._store = null;
    this._prefix = "chunking_";
    this._overhead = 200;
    this._respectDigest = true;
    this._uselz = true;
    this._compressMin = 300;
  }

  // default how to get an object
  _getObject(store, key) {
    return this._readFromStore(store, key).then((result) => {
      return result ? JSON.parse(result) : null;
    });
  }

  // how to set an object
  _setObject(store, key, ob, expire) {
    const s = JSON.stringify(ob || {});
    return this._writeToStore(store, key, s, expire, ob.propKey);
  }

  // default how to remove an object
  _removeObject(store, key) {
    return store.deleteProperty(key);
  }

  /**
   * set whether to use lz
   * @param {number} chunkSize the max size
   * @return {Chunker} this
   */
  setUselz(uselz) {
    this._uselz = uselz;
    return this;
  }

  /**
   * set the max chunksize
   * @param {number} chunkSize the max size
   * @return {Chunker} this
   */
  setChunkSize(chunkSize) {
    this._chunkSize = chunkSize;
    return this;
  }

  /**
   * minimum size over which to compress
   * @return {boolean} respectDigest the max size
   */
  getCompressMin() {
    return this._compressMin;
  }

  /**
   * whether to respect digest to avoid rewriting unchanged records
   * @param {boolean} compressMin the min size
   * @return {Chunker} this
   */
  setCompressMin(compressMin) {
    if (!Utils.isUndefined(compressMin)) this._compressMin = compressMin;
    return this;
  }

  /**
   * whether to respect digest to avoid rewriting unchanged records
   * @return {boolean} respectDigest
   */
  getRespectDigest() {
    return this._respectDigest;
  }

  /**
   * whether to respect digest to avoid rewriting unchanged records
   * @param {boolean} respectDigest the max size
   * @return {Chunker} this
   */
  setRespectDigest(respectDigest) {
    if (!Utils.isUndefined(this._respectDigest))
      this._respectDigest = respectDigest;
    return this;
  }

  /**
   * get the max chunksize
   * @return {number} chunkSize the max size
   */
  getChunkSize() {
    return this._chunkSize;
  }

  /**
   * set the key prefix
   * @param {string} prefix the key prefix
   * @return {Chunker} this
   */
  setPrefix(prefix) {
    if (!Utils.isUndefined(prefix)) this._prefix = prefix.toString();
    return this;
  }

  /**
   * get the prefix
   * @return {string} prefix the prefix
   */
  getPrefix() {
    return this._prefix;
  }
  /**
   * set the store
   * @param {object} store the store
   * @return {Chunker} this
   */
  setStore(store) {
    this._store = store;
    return this;
  }

  /**
   * get the store
   * @return {object} the store
   */
  getStore() {
    return this._store;
  }

  /**
   * set how to get an object
   * @param {function} func how to get an object
   * @return {Chunker} this
   */
  funcGetObject(func) {
    // func should take a store, key and return an object
    this._getObject = this._checkaFunc(func);
    return this;
  }

  /**
   * set how to get an object
   * @param {function} func how to set an object
   * @return {Chunker} this
   */
  funcSetObject(func) {
    // func should take a store, key and an object, and return the size of the stringified object
    this._setObject = this._checkaFunc(func);
    return this;
  }

  /**
   * set how to read from store
   * @param {function} func how to read from store
   * @return {Chunker} this
   */
  funcReadFromStore(func) {
    // func should take a store key, and return a string
    this._readFromStore = this._checkaFunc(func);
    return this;
  }

  /**
   * set how to write to store
   * @param {function} func how to set an object
   * @return {Chunker} this
   */
  funcWriteToStore(func) {
    // func should take a store key and a string to write
    this._writeToStore = this._checkaFunc(func);
    return this;
  }

  /**
   * set how to remove an object
   * @param {function} func how to remove an object
   * @return {Chunker} this
   */
  funcRemoveObject(func) {
    // func should take a store, key
    this._removeObject = this._checkaFunc(func);
    return this;
  }

  /**
   * check that a variable is a function and throw if not
   * @param {function} [func] optional function to check
   * @return {function} the func
   */
  _checkaFunc(func) {
    if (func && typeof func !== "function") {
      throw new Error("argument should be a function");
    }
    return func;
  }

  _payloadSize(metaSize) {
    if (this._chunkSize <= this._overhead || this._chunkSize <= metaSize) {
      throw (
        "chunksize must be at least " + (Math.max(this._overhead, metaSize) + 1)
      );
    }
    return this._chunkSize - metaSize;
  }

  _digest(what) {
    return Utils.keyDigest(what);
  }

  _uid() {
    return Utils.generateUniqueString(6);
  }

  _getChunkKey(key) {
    return key + "_" + this._uid();
  }

  fudgeKey(key) {
    if (Utils.isUndefined(key) || key === null)
      throw new Error("property key must have a value");
    return typeof key === "object" ? this._digest(key) : key;
  }

  /**
   * get the keys of multiple entries if it was too big
   * @param {PropertiesService} props the service to use
   * @param {object} propKey the key
   * @return {object} the result {chunks:[],data:{}} - an array of keys, or some actual data
   */
  async getChunkKeys(propKey) {
    // in case the key is an object
    propKey = this.fudgeKey(propKey);

    var data,
      crushed = await this._getObject(this.getStore(), propKey);
    const uselz = crushed && crushed.uselz;

    // at this point, crushed is an object with either
    // a .chunk property with a zipped version of the data, or
    // a .chunks property with an array of other entries to get
    // a .digest property with the digest of all the data which identifies it as a master

    // its a non split item
    if (crushed && crushed.chunk && crushed.digest) {
      // uncrush the data and parse it back to an object if there are no associated records
      data = crushed.chunk
        ? JSON.parse(
            crushed.skipZip ? crushed.chunk : this.unzip(crushed.chunk, uselz)
          )
        : null;
    }

    // return either the data or where to find the data
    return {
      chunks: crushed && crushed.chunks ? crushed.chunks : null,
      data: data,
      digest: crushed ? crushed.digest : "",
      skipZip: crushed && crushed.skipZip,
      expiresAt: crushed && crushed.expiresAt,
      uselz,
    };
  }

  /**
   * remove an entry and its associated stuff
   * @param {object} propKey the key
   * @return {Props} this
   */
  async removeBigProperty(propKey) {
    // in case the key is an object
    propKey = this.fudgeKey(propKey);

    // always big properties are always crushed
    return this.getChunkKeys(this._prefix + propKey).then((chunky) => {
      // now remove the properties entries

      return Promise.all(
        ((chunky && chunky.chunks) || []).map((d) =>
          this._removeObject(this.getStore(), d)
        )
      ).then((pieces) => {
        if (chunky.digest) {
          return this._removeObject(
            this.getStore(),
            this._prefix + propKey
          ).then(() => pieces.length + 1);
        } else {
          return Promise.resolve(pieces.length);
        }
      });
    });
  }

  /**
   * updates a property using multiple entries if its going to be too big
   * @param {object} propKey the key
   * @param {object} ob the thing to write
   * @param {number} expire secs to expire
   * @return {size} of data written - if nothing done, size is 0
   */
  setBigProperty(propKey, ob, expire) {
    // in case the key is an object
    propKey = this.fudgeKey(propKey);

    // donbt allow undefined
    if (Utils.isUndefined(ob)) {
      throw "cant write undefined to store";
    }

    // blob pulls it out
    if (Utils.isBlob(ob)) {
      var slob = {
        contentType: ob.getContentType(),
        name: ob.getName(),
        content: Utilities.base64Encode(ob.getBytes()),
        blob: true,
      };
    }

    // convery to timestamp
    else if (Utils.isDateObject(ob)) {
      var slob = {
        date: true,
        content: ob.getTime(),
      };
    }

    // strinfigy
    else if (typeof ob === "object") {
      var slob = {
        content: JSON.stringify(ob),
        parse: true,
      };
    } else {
      var slob = {
        content: ob,
      };
    }
    // whether to use lz compression algo or just zip it
    slob.uselz = this._uselz;

    // pack all that up to write to the store
    const sob = JSON.stringify(slob);

    // get the digest
    var digest = Utils.keyDigest(sob);

    // now get the master if there is one
    return this._getObject(this.getStore(), this._prefix + propKey).then(
      (master) => {
        if (
          master &&
          master.digest &&
          master.digest === digest &&
          this._respectDigest &&
          !expire
        ) {
          // nothing to do
          return 0;
        } else {
          // need to remove the previous entries and add this new one
          return this.removeBigProperty(propKey).then(() => {
            return this._setBigProperty(this._prefix + propKey, sob, expire);
          });
        }
      }
    );
  }

  /**
   * gets a property using multiple entries if its going to be too big
   * @param {object} propKey the key
   * @return {object} what was retrieved
   */
  async getBigProperty(propKey) {
    // in case the key is an objecs

    propKey = this.fudgeKey(propKey);

    // always big properties are always crushed
    var chunky = await this.getChunkKeys(this._prefix + propKey);
    const expired =
      chunky && chunky.expiresAt && chunky.expiresAt < new Date().getTime();
    const uselz = chunky && chunky.uselz;

    // that'll return either some data, or a list of keys
    if (expired) {
      // auto cleaning - it;s possible it'll fail if it cleans between checking and here and the store supports this cleansing
      // so we dont care if there's an error
      return this.removeBigProperty(propKey).catch((err) => {
        console.log("ignored an error this cleaning  for", propKey, err);
        return null;
      });
    } else {
      return (chunky && chunky.chunks
        ? Promise.all(
            chunky.chunks.map((c) => this._getObject(this.getStore(), c))
          ).then((r) => {
            const p = r.reduce((p, c, i) => {
              // should always be available
              if (!c) {
                throw (
                  "missing chunked property " +
                  chunky.chunks[i] +
                  " for key " +
                  propKey
                );
              }
              // rebuild the crushed string
              return p + c.chunk;
            }, "");
            const peed = chunky.skipZip ? p : this.unzip(p, uselz);

            return JSON.parse(peed);
          })
        : Promise.resolve(chunky ? chunky.data : null)
      ).then((pkge) => {
        // now need to unpack;
        if (pkge) {
          if (pkge.parse) {
            return JSON.parse(pkge.content);
          } else if (pkge.date) {
            return new Date(pkge.content);
          } else if (pkge.blob) {
            // this is a blob in Apps Script
            return {
              ...pkge,
              get getBytes() {
                return Buffer.from(this.content, "base64");
              },
              blob: true,
            };
          } else {
            return pkge.content;
          }
        } else {
          return null;
        }
      });
    }
  }

  _chunker(a, size) {
    return Array.from(new Array(Math.ceil(a.length / size)), (_, i) =>
      a.slice(i * size, i * size + size)
    );
  }
  /**
   * sets a property using multiple entries if its going to be too big
   *  use this.setBigProperty() from outside, which first deletes existing stuff
   *  as well as checking the digest
   * @param {object} propKey the key
   * @param {string} sob the thing to write
   * @return {number} total length of everything written
   */
  _setBigProperty = async (propKey, sob, expire) => {
    // crush the object
    var skipZip = sob.length < this._compressMin;
    var chunks,
      crushed = skipZip ? sob : this.zip(sob, this._uselz);

    // get the digest
    // the digest is used to avoid updates when theres no change
    var digest = this._digest(sob);

    // now split up the big thing if needed
    // expire should be a little bigger for the chunks to make sure they dont go away
    // important! always write the master record last if there's Chunker going on
    // this will allow plugins capable of multi writing to do it all in one fetch
    const expiresAt = expire ? new Date().getTime() + expire * 1000 : null;
    const meta = {
      digest: digest,
      skipZip: skipZip,
      expiresAt,
      uselz: this._uselz,
      propKey,
    };
    const payloadSize = this._payloadSize(JSON.stringify(meta).length);

    // chunk it up and write it out in reverse
    const lumps = this._chunker(crushed, payloadSize);
    const children = lumps.length > 1 ? lumps : [];
    const keys = children.map((f) => this._getChunkKey(propKey));

    // do the children first
    return Promise.all(
      children.map((chunk, i, a) => {
        return this._setObject(
          this.getStore(),
          keys[i],
          {
            propKey,
            chunk,
          },
          expire ? expire + 1 : expire
        );
      })
    ).then((result) => {
      // if there were some children we need to write the key
      // otherwise this will contain just data
      return this._setObject(
        this.getStore(),
        propKey,
        children.length
          ? {
              ...meta,
              chunks: keys,
            }
          : {
              ...meta,
              chunk: lumps[0],
            },
        expire
      ).then((s) => result.reduce((p, c) => p + c, s));
    });
  };

  /**
   * crush for writing to cache.props
   * @param {string} crushThis the string to crush
   * @return {string} the b64 zipped version
   */
  zip(crushThis, uselz) {
    if (!uselz) {
      throw new Error("only uselz supported on node");
    } else {
      // using this mode makes it more transportable for cross platform
      return Compress.compressString(crushThis);
    }
  }

  /**
   * uncrush for writing to cache.props
   * @param {string} crushed the crushed string
   * @return {string} the uncrushed string
   */
  unzip(crushed, uselz) {
    if (!uselz) {
      throw new Error("only uselz supported on node");
    }
    const peed = Compress.decompressString(crushed);
    return peed;
  }
}

module.exports = {
  Chunker,
};
