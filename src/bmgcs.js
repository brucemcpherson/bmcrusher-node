const { Storage } = require("@google-cloud/storage");
const { Readable } = require("stream");

const getStorage = ({ credentials }) => {
  const { projectId } = credentials;
  return new Storage({
    projectId,
    credentials,
  });
};

const getBucket = ({ credentials, bucketName }) =>
  getStorage({ credentials }).bucket(bucketName);

/**
 * create the cloud storage stream
 * the credentials/bucket name and filename are in the secrets file
 * @param {object} options
 * @param {string} options.key the filename on cloud storage
 * @param {string} options.type the mimetype on cloud storage
 * @param {object} options.credentials the service account json
 * @param {string} options.bucketName
 * @param {string} options.prefix
 * @param {object} options.bucket the storage bucket
 */
const createWriteStream = ({ key, type = "plain/text", prefix, bucket }) => {
  const blob = getFile({ bucket, prefix, key });
  const stream = blob.createWriteStream({
    resumable: true,
    contentType: type,
  });
  return stream;
};

const stringToStream = ({ writeStream, content }) => {
  const s = new Readable();
  const str = typeof content === "object" ? JSON.stringify(content) : content;
  s.push(str);
  s.push(null);
  return new Promise((resolve, reject) => {
    s.pipe(writeStream)
      .on("finish", () => {
        resolve(s);
      })
      .on("error", (err) => {
        console.log("failed string to stream", err);
        reject(err);
      });
  });
};

const streamToString = ({ readStream }) => {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readStream.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    readStream.on("end", () =>
      resolve(Buffer.concat(chunks).toString("utf-8"))
    );
    readStream.on("error", (err) => {
      // this is ok as it may not exist
      if (err.code !== 404) console.log("failed stream to string", err);
      reject(err);
    });
  });
};

const createReadStream = ({ key, type = "plain/text", prefix, bucket }) => {
  const blob = getFile({ bucket, prefix, key });
  const stream = blob.createReadStream({
    resumable: true,
    contentType: type,
  });
  // this stream will be piped to

  return stream;
};

const makeName = ({ key, prefix }) =>
  (prefix + "/" + key).replace(/\/+/g, "/").replace(/^\//, "");

const getFile = ({ key, bucket, prefix }) =>
  bucket.file(makeName({ prefix, key }));

const removeFile = ({ key, prefix, bucket }) => {
  const blob = getFile({ bucket, prefix, key });
  return blob.delete().then((response) => {
    // dont know why this returns an array with 2 things in it
    return response && response[0];
  }).catch(err => {
    if (err.code === 404) {
      console.log('...couldnt find file to delete', key)
      return err
    } else {
      console.log('failed when deleting', key, err)
    }
    
    return err
  })
};

module.exports = {
  createWriteStream,
  getBucket,
  stringToStream,
  streamToString,
  createReadStream,
  removeFile,
};
