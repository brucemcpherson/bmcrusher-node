const { Utils } = require("./Utils");
function Fetcher({ fetcher, tokenService }) {
  /**
   * this is a standard result object to simply error checking etc.
   * @param {HTTPResponse} response the response from UrlFetchApp
   * @return {object} the result object
   */
  const makeResults = (response) => {
    const result = {
      success: false,
      data: null,
      code: null,
      extended: "",
      parsed: false,
    };

    // process the result


    if (response) {
      result.code = response.statusCode;
      result.headers = response.headers;
      result.content = response.body;

      result.success =
        result.code === 200 || result.code === 201 || result.code === 204;

      try {
        if (result.content) {
          result.data = JSON.parse(result.content);
          result.parsed = true;
        }
      } catch (err) {
        result.extended = err;
      }
    }

    return result;
  };
  /**
   * execute a urlfetch
   * @param {string} url the url
   * @param {object} options any additional options
   * @return {object} a standard response
   */
  this.got = (url, options = {}) => {
    options = { method: "GET", ...options };
    options.headers = options.headers || {};
    if (tokenService) {
      options.headers.authorization = "Bearer " + tokenService();
    }
    
    return fetcher(url, options).then((response) => {
      return makeResults(response);
    }).catch(err => {
      // simulate an error
      return makeResults(err.response)
    })
  };
}
module.exports = {
  Fetcher,
};
