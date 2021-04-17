const {
  createWriteStream,
  getBucket,
  stringToStream,
  streamToString,
  createReadStream,
  removeFile
} = require("./src/bmgcs");
const { getGcpCreds } = require("../testcrusher/private/secrets");

const bucket = getBucket({
  credentials: getGcpCreds(),
  bucketName: "bmcrusher-test-bucket-store",
});

const key= "xxx"
const prefix= "/crusher/store/"
        
const writeStream = createWriteStream({
  key,
  bucket,
  prefix
});

stringToStream({ writeStream, content: "some stuff" })
  .then((s) => {
    const readStream = createReadStream({
      key,
      bucket,
      prefix,
    });
    return streamToString({readStream})
  }).then((content) => {
    console.log(content)
    return removeFile({bucket, prefix, key})
  }).then(r => {
    console.log(r.statusCode, r.statusMessage)
    
  })



