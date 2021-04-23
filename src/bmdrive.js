const { google } = require("googleapis");
const { Readable } = require("stream");
const FOLDER = "application/vnd.google-apps.folder";
// All returns are promises to the return value in the JSDOC

/**
 * get an auth object
 * @param {object} options
 * @param {object} options.credentials the content of the service accoutn JSON file
 * @param {string} options.subject the email address of the account to impersonate
 * @returns {GoogleAuth}
 */
const getAuth = async ({ credentials, subject }) => {
  // use JWT uth for serviec account with a subject for impersonation
  const auth = new google.auth.JWT({
    email: credentials.client_email,
    key: credentials.private_key,
    scopes: ["https://www.googleapis.com/auth/drive"],
    subject,
  });
  return auth.authorize().then(() => auth);
};

/**
 * get an authenticated client for Drive
 * @param {object} options
 * @param {object} options.credentials the content of the service accoutn JSON file
 * @param {string} options.subject the email address of the account to impersonate
 * @returns {Drive} a client
 */
const getClient = ({ credentials, subject }) =>
  getAuth({ credentials, subject }).then((auth) =>
    google.drive({
      version: "v3",
      auth,
    })
  );

/**
 * create a drive folder
 * @param {object} options createfile options
 * @returns {File} response
 */
const createFolder = (options) => createFile({ ...options, mimeType: FOLDER });

/**
 * create a drive file
 * @param {object} options createfile options
 * @param {string} options.name the file name
 * @param {string} [options.mimeType = "text/plain"] the mimetype
 * @param {Drive} options.client the authenticated client
 * @param {[string]} options.parents the id's of the parents (usually onlt 1)
 * @returns {File} response
 */
const createFile = ({
  name,
  mimeType = "text/plain",
  client,
  content,
  parents,
}) => {
  const requestBody = {
    name,
    mimeType,
  };
  if (parents) {
    if (!Array.isArray(parents)) parents = [parents];
    requestBody.parents = parents;
  }
  // we'll do this as a stream
  const options = {
    requestBody,
  };
  if (content) {
    const s = new Readable();
    s.push(content);
    s.push(null);

    options.media = {
      mimeType,
      body: s,
    };
  }

  return client.files.create(options);
};

/**
 *
 * @param {object} options
 * @param {string} options.path a path like '/'
 * @param {string} options.client the client to use
 * @param {boolean} options.createIfMissing whether to create missing folders if not in the path
 * @return {object} an iterator
 */
const folderIterator = ({ path = "", client, createIfMissing = false }) => {
  
  const extractFiles = (res) =>
    res &&
    res.data &&
    res.data.files &&
    res.data.files[0] &&
    res.data.files;
  
  const getItem = ({ name, parents }) => {
    q = `name='${name}' and mimeType = '${FOLDER}' and trashed = false`;
    const options = {
      q,
    };
    if (parents) options.q += ` and '${parents[0]}' in parents`;

    return client.files
      .list(options)
      .then((res) => {
        return res;
      })
      .catch((error) => {
        console.log(error);
        return Promise.reject(error)
      });
  };

  const paths = path.trim().replace(/^\//, "").replace(/\.$/, "").split("/");

  return {
    // will be selected in for await of..
    [Symbol.asyncIterator]() {
      return {
        paths,
        parents: null,
        ids: [],
        hasNext() {
          return this.paths.length;
        },

        next() {
          if (!this.hasNext())
            return Promise.resolve({
              done: true,
            });

          const name = this.paths.shift();
          const parents = this.parents && this.parents.map((f) => f.id);
          return getItem({ name, parents }).then((res) => {
            const value = extractFiles(res);
            this.parents = value;
            if (!value) {
              return (createIfMissing
                ? createFolder({
                    client,
                    name,
                    parents,
                  })
                : Promise.resolve(null)).then((res) => {
                    this.parents = [res.data];
                    if (!this.parents) {
                      console.log("...couldnt find/create folder", name);
                      return Promise.reject("giving up");
                    } else {
                      console.log("...created folder", name, this.parents)
                      return {
                        done: false,
                        value: this.parents ,
                      };
                    }
                })
            } else {
              return {
                done: false,
                value,
              };
            }
          });
        },
      };
    },
  };
};

/**
 * get files that match a given name
 * @param {object} options  options
 * @param {string} options.name the file name
 * @param {Drive} options.client the authenticated client
 * @param {[string]} options.parents the id's of the parents (usually onlt 1)
 * @returns {[File]} files
 */
const getFilesByName = ({ parents, client, name }) => {
  const options = {
    q: `name='${name}' and trashed = false`,
    orderBy: "modifiedTime desc",
  };
  if (parents) options.q += ` and '${parents[0]}' in parents`;
  return client.files.list(options).then((res) => {
    const files = res && res.data && res.data.files;
    // it's always possible there are multiple versions, even though they get cleaned up
    return files;
  });
};

/**
 * get file content for a given id
 * @param {object} options  options
 * @param {string} options.fileId the file id
 * @param {Drive} options.client the authenticated client
 * @returns {object} the {content, res, fileId}
 */
const getFile = ({ fileId, client }) => {
  return client.files
    .get(
      {
        alt: "media",
        fileId,
      },
      {
        responseType: "stream",
      }
    )
    .then((res) =>
      streamToString({ readStream: res.data }).then((content) => ({
        content,
        res,
        fileId,
      }))
    );
};

/**
 * get the id of a folder at the end of a path /a/b/c returns the drive file for c
 * @param {object} options  options
 * @param {string} options.path the path
 * @param {Drive} options.client the authenticated client
 * @returns {File} the parent folder at the end of the path
 */

const getFolder = async ({ client, path }) => {
  let parent = null;
  for await (let folder of folderIterator({ client, path, createIfMissing: true })) {
    parent = folder;
  }
  return parent && parent[0];
};
/**
 * get a string from a stream
 * @param {object} options  options
 * @param {Readable} options.readStream the input stream
 * @returns {string} the content
 */
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

/**
 * remove a  file for a given id
 * @param {object} options  options
 * @param {string} options.fileId the file id
 * @param {Drive} options.client the authenticated client
 * @returns {Response} the ressponse
 */
const removeFile = ({ fileId, client }) => {
  return client.files.delete({
    fileId,
  });
};

module.exports = {
  createFile,
  getClient,
  getFile,
  folderIterator,
  getFolder,
  removeFile,
  getFilesByName,
  createFolder,
};
