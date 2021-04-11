

# bmcrusher-node

For transferring crushed data between app script node using various plugins. 

## Installation

````
yarn add bmcrusher-node
````


## CrusherPluginUpstashService


Uses Upstash as a redis backend.

For setting up Upstash and Apps script see https://ramblings.mcpher.com/apps-script/apps-script-cache-crusher/upstash/

You'll need an Upstash account and credentials.

### Node usage

First get your upstash credentials. Mine are in a file like this. Choose the appropriate credential depending on whether you are reading or read/writing.
````
const upstashrw = "xxx";
const upstashr = "xxx";
````

#### Initialize the crusher

This is a similar pattern and options as described in the Apps Script writeup in https://ramblings.mcpher.com/apps-script/apps-script-cache-crusher/upstash/. At a minumum you should provide a token service function that areturns your upstash key. I also recommend a prefix to be applied to cache keys in case you want to use the same Upstash store for something else at some point.

````
const { CrusherPluginUpstashService } = require("bmcrusher-node");
const { upstashrw } = require("./private/secrets");

const crusher = new CrusherPluginUpstashService().init({
  tokenService: () => upstashrw,
  prefix: "/crusher/store"
});

````
Now you can use the store as a regular key/value store which will be shared with other Node apps or Apps Script.

### put

Put a value. If it's too big it'll compress it and then split it into pieces.

````
crusher.put(key, someValue [,expiryTimeInSeconds])
````

### get

Get a value. If it's in pieces it will reconstitute it to the original. If it's expired or doesnt exist, it'll return null

````
crusher.get(key)
````

### remove

Remove a value
````
crusher.remove(key)
````
