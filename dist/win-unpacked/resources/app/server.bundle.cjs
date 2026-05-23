/* MeshSense server bundle */
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __commonJS = (cb, mod) => function __require() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// node_modules/ws/lib/constants.js
var require_constants = __commonJS({
  "node_modules/ws/lib/constants.js"(exports2, module2) {
    "use strict";
    var BINARY_TYPES = ["nodebuffer", "arraybuffer", "fragments"];
    var hasBlob = typeof Blob !== "undefined";
    if (hasBlob)
      BINARY_TYPES.push("blob");
    module2.exports = {
      BINARY_TYPES,
      CLOSE_TIMEOUT: 3e4,
      EMPTY_BUFFER: Buffer.alloc(0),
      GUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
      hasBlob,
      kForOnEventAttribute: Symbol("kIsForOnEventAttribute"),
      kListener: Symbol("kListener"),
      kStatusCode: Symbol("status-code"),
      kWebSocket: Symbol("websocket"),
      NOOP: () => {
      }
    };
  }
});

// node_modules/ws/lib/buffer-util.js
var require_buffer_util = __commonJS({
  "node_modules/ws/lib/buffer-util.js"(exports2, module2) {
    "use strict";
    var { EMPTY_BUFFER } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    function concat(list, totalLength) {
      if (list.length === 0)
        return EMPTY_BUFFER;
      if (list.length === 1)
        return list[0];
      const target = Buffer.allocUnsafe(totalLength);
      let offset = 0;
      for (let i = 0; i < list.length; i++) {
        const buf = list[i];
        target.set(buf, offset);
        offset += buf.length;
      }
      if (offset < totalLength) {
        return new FastBuffer(target.buffer, target.byteOffset, offset);
      }
      return target;
    }
    function _mask(source, mask, output, offset, length) {
      for (let i = 0; i < length; i++) {
        output[offset + i] = source[i] ^ mask[i & 3];
      }
    }
    function _unmask(buffer, mask) {
      for (let i = 0; i < buffer.length; i++) {
        buffer[i] ^= mask[i & 3];
      }
    }
    function toArrayBuffer(buf) {
      if (buf.length === buf.buffer.byteLength) {
        return buf.buffer;
      }
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
    }
    function toBuffer(data) {
      toBuffer.readOnly = true;
      if (Buffer.isBuffer(data))
        return data;
      let buf;
      if (data instanceof ArrayBuffer) {
        buf = new FastBuffer(data);
      } else if (ArrayBuffer.isView(data)) {
        buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength);
      } else {
        buf = Buffer.from(data);
        toBuffer.readOnly = false;
      }
      return buf;
    }
    module2.exports = {
      concat,
      mask: _mask,
      toArrayBuffer,
      toBuffer,
      unmask: _unmask
    };
    if (!process.env.WS_NO_BUFFER_UTIL) {
      try {
        const bufferUtil = require("bufferutil");
        module2.exports.mask = function(source, mask, output, offset, length) {
          if (length < 48)
            _mask(source, mask, output, offset, length);
          else
            bufferUtil.mask(source, mask, output, offset, length);
        };
        module2.exports.unmask = function(buffer, mask) {
          if (buffer.length < 32)
            _unmask(buffer, mask);
          else
            bufferUtil.unmask(buffer, mask);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/limiter.js
var require_limiter = __commonJS({
  "node_modules/ws/lib/limiter.js"(exports2, module2) {
    "use strict";
    var kDone = Symbol("kDone");
    var kRun = Symbol("kRun");
    var Limiter = class {
      /**
       * Creates a new `Limiter`.
       *
       * @param {Number} [concurrency=Infinity] The maximum number of jobs allowed
       *     to run concurrently
       */
      constructor(concurrency) {
        this[kDone] = () => {
          this.pending--;
          this[kRun]();
        };
        this.concurrency = concurrency || Infinity;
        this.jobs = [];
        this.pending = 0;
      }
      /**
       * Adds a job to the queue.
       *
       * @param {Function} job The job to run
       * @public
       */
      add(job) {
        this.jobs.push(job);
        this[kRun]();
      }
      /**
       * Removes a job from the queue and runs it if possible.
       *
       * @private
       */
      [kRun]() {
        if (this.pending === this.concurrency)
          return;
        if (this.jobs.length) {
          const job = this.jobs.shift();
          this.pending++;
          job(this[kDone]);
        }
      }
    };
    module2.exports = Limiter;
  }
});

// node_modules/ws/lib/permessage-deflate.js
var require_permessage_deflate = __commonJS({
  "node_modules/ws/lib/permessage-deflate.js"(exports2, module2) {
    "use strict";
    var zlib = require("zlib");
    var bufferUtil = require_buffer_util();
    var Limiter = require_limiter();
    var { kStatusCode } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    var TRAILER = Buffer.from([0, 0, 255, 255]);
    var kPerMessageDeflate = Symbol("permessage-deflate");
    var kTotalLength = Symbol("total-length");
    var kCallback = Symbol("callback");
    var kBuffers = Symbol("buffers");
    var kError = Symbol("error");
    var zlibLimiter;
    var PerMessageDeflate2 = class {
      /**
       * Creates a PerMessageDeflate instance.
       *
       * @param {Object} [options] Configuration options
       * @param {(Boolean|Number)} [options.clientMaxWindowBits] Advertise support
       *     for, or request, a custom client window size
       * @param {Boolean} [options.clientNoContextTakeover=false] Advertise/
       *     acknowledge disabling of client context takeover
       * @param {Number} [options.concurrencyLimit=10] The number of concurrent
       *     calls to zlib
       * @param {Boolean} [options.isServer=false] Create the instance in either
       *     server or client mode
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {(Boolean|Number)} [options.serverMaxWindowBits] Request/confirm the
       *     use of a custom server window size
       * @param {Boolean} [options.serverNoContextTakeover=false] Request/accept
       *     disabling of server context takeover
       * @param {Number} [options.threshold=1024] Size (in bytes) below which
       *     messages should not be compressed if context takeover is disabled
       * @param {Object} [options.zlibDeflateOptions] Options to pass to zlib on
       *     deflate
       * @param {Object} [options.zlibInflateOptions] Options to pass to zlib on
       *     inflate
       */
      constructor(options) {
        this._options = options || {};
        this._threshold = this._options.threshold !== void 0 ? this._options.threshold : 1024;
        this._maxPayload = this._options.maxPayload | 0;
        this._isServer = !!this._options.isServer;
        this._deflate = null;
        this._inflate = null;
        this.params = null;
        if (!zlibLimiter) {
          const concurrency = this._options.concurrencyLimit !== void 0 ? this._options.concurrencyLimit : 10;
          zlibLimiter = new Limiter(concurrency);
        }
      }
      /**
       * @type {String}
       */
      static get extensionName() {
        return "permessage-deflate";
      }
      /**
       * Create an extension negotiation offer.
       *
       * @return {Object} Extension parameters
       * @public
       */
      offer() {
        const params = {};
        if (this._options.serverNoContextTakeover) {
          params.server_no_context_takeover = true;
        }
        if (this._options.clientNoContextTakeover) {
          params.client_no_context_takeover = true;
        }
        if (this._options.serverMaxWindowBits) {
          params.server_max_window_bits = this._options.serverMaxWindowBits;
        }
        if (this._options.clientMaxWindowBits) {
          params.client_max_window_bits = this._options.clientMaxWindowBits;
        } else if (this._options.clientMaxWindowBits == null) {
          params.client_max_window_bits = true;
        }
        return params;
      }
      /**
       * Accept an extension negotiation offer/response.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Object} Accepted configuration
       * @public
       */
      accept(configurations) {
        configurations = this.normalizeParams(configurations);
        this.params = this._isServer ? this.acceptAsServer(configurations) : this.acceptAsClient(configurations);
        return this.params;
      }
      /**
       * Releases all resources used by the extension.
       *
       * @public
       */
      cleanup() {
        if (this._inflate) {
          this._inflate.close();
          this._inflate = null;
        }
        if (this._deflate) {
          const callback = this._deflate[kCallback];
          this._deflate.close();
          this._deflate = null;
          if (callback) {
            callback(
              new Error(
                "The deflate stream was closed while data was being processed"
              )
            );
          }
        }
      }
      /**
       *  Accept an extension negotiation offer.
       *
       * @param {Array} offers The extension negotiation offers
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsServer(offers) {
        const opts = this._options;
        const accepted = offers.find((params) => {
          if (opts.serverNoContextTakeover === false && params.server_no_context_takeover || params.server_max_window_bits && (opts.serverMaxWindowBits === false || typeof opts.serverMaxWindowBits === "number" && opts.serverMaxWindowBits > params.server_max_window_bits) || typeof opts.clientMaxWindowBits === "number" && !params.client_max_window_bits) {
            return false;
          }
          return true;
        });
        if (!accepted) {
          throw new Error("None of the extension offers can be accepted");
        }
        if (opts.serverNoContextTakeover) {
          accepted.server_no_context_takeover = true;
        }
        if (opts.clientNoContextTakeover) {
          accepted.client_no_context_takeover = true;
        }
        if (typeof opts.serverMaxWindowBits === "number") {
          accepted.server_max_window_bits = opts.serverMaxWindowBits;
        }
        if (typeof opts.clientMaxWindowBits === "number") {
          accepted.client_max_window_bits = opts.clientMaxWindowBits;
        } else if (accepted.client_max_window_bits === true || opts.clientMaxWindowBits === false) {
          delete accepted.client_max_window_bits;
        }
        return accepted;
      }
      /**
       * Accept the extension negotiation response.
       *
       * @param {Array} response The extension negotiation response
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsClient(response) {
        const params = response[0];
        if (this._options.clientNoContextTakeover === false && params.client_no_context_takeover) {
          throw new Error('Unexpected parameter "client_no_context_takeover"');
        }
        if (!params.client_max_window_bits) {
          if (typeof this._options.clientMaxWindowBits === "number") {
            params.client_max_window_bits = this._options.clientMaxWindowBits;
          }
        } else if (this._options.clientMaxWindowBits === false || typeof this._options.clientMaxWindowBits === "number" && params.client_max_window_bits > this._options.clientMaxWindowBits) {
          throw new Error(
            'Unexpected or invalid parameter "client_max_window_bits"'
          );
        }
        return params;
      }
      /**
       * Normalize parameters.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Array} The offers/response with normalized parameters
       * @private
       */
      normalizeParams(configurations) {
        configurations.forEach((params) => {
          Object.keys(params).forEach((key) => {
            let value = params[key];
            if (value.length > 1) {
              throw new Error(`Parameter "${key}" must have only a single value`);
            }
            value = value[0];
            if (key === "client_max_window_bits") {
              if (value !== true) {
                const num = +value;
                if (!Number.isInteger(num) || num < 8 || num > 15) {
                  throw new TypeError(
                    `Invalid value for parameter "${key}": ${value}`
                  );
                }
                value = num;
              } else if (!this._isServer) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else if (key === "server_max_window_bits") {
              const num = +value;
              if (!Number.isInteger(num) || num < 8 || num > 15) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
              value = num;
            } else if (key === "client_no_context_takeover" || key === "server_no_context_takeover") {
              if (value !== true) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else {
              throw new Error(`Unknown parameter "${key}"`);
            }
            params[key] = value;
          });
        });
        return configurations;
      }
      /**
       * Decompress data. Concurrency limited.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      decompress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._decompress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Compress data. Concurrency limited.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      compress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._compress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Decompress data.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _decompress(data, fin, callback) {
        const endpoint = this._isServer ? "client" : "server";
        if (!this._inflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._inflate = zlib.createInflateRaw({
            ...this._options.zlibInflateOptions,
            windowBits
          });
          this._inflate[kPerMessageDeflate] = this;
          this._inflate[kTotalLength] = 0;
          this._inflate[kBuffers] = [];
          this._inflate.on("error", inflateOnError);
          this._inflate.on("data", inflateOnData);
        }
        this._inflate[kCallback] = callback;
        this._inflate.write(data);
        if (fin)
          this._inflate.write(TRAILER);
        this._inflate.flush(() => {
          const err = this._inflate[kError];
          if (err) {
            this._inflate.close();
            this._inflate = null;
            callback(err);
            return;
          }
          const data2 = bufferUtil.concat(
            this._inflate[kBuffers],
            this._inflate[kTotalLength]
          );
          if (this._inflate._readableState.endEmitted) {
            this._inflate.close();
            this._inflate = null;
          } else {
            this._inflate[kTotalLength] = 0;
            this._inflate[kBuffers] = [];
            if (fin && this.params[`${endpoint}_no_context_takeover`]) {
              this._inflate.reset();
            }
          }
          callback(null, data2);
        });
      }
      /**
       * Compress data.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _compress(data, fin, callback) {
        const endpoint = this._isServer ? "server" : "client";
        if (!this._deflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._deflate = zlib.createDeflateRaw({
            ...this._options.zlibDeflateOptions,
            windowBits
          });
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          this._deflate.on("data", deflateOnData);
        }
        this._deflate[kCallback] = callback;
        this._deflate.write(data);
        this._deflate.flush(zlib.Z_SYNC_FLUSH, () => {
          if (!this._deflate) {
            return;
          }
          let data2 = bufferUtil.concat(
            this._deflate[kBuffers],
            this._deflate[kTotalLength]
          );
          if (fin) {
            data2 = new FastBuffer(data2.buffer, data2.byteOffset, data2.length - 4);
          }
          this._deflate[kCallback] = null;
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          if (fin && this.params[`${endpoint}_no_context_takeover`]) {
            this._deflate.reset();
          }
          callback(null, data2);
        });
      }
    };
    module2.exports = PerMessageDeflate2;
    function deflateOnData(chunk) {
      this[kBuffers].push(chunk);
      this[kTotalLength] += chunk.length;
    }
    function inflateOnData(chunk) {
      this[kTotalLength] += chunk.length;
      if (this[kPerMessageDeflate]._maxPayload < 1 || this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload) {
        this[kBuffers].push(chunk);
        return;
      }
      this[kError] = new RangeError("Max payload size exceeded");
      this[kError].code = "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH";
      this[kError][kStatusCode] = 1009;
      this.removeListener("data", inflateOnData);
      this.reset();
    }
    function inflateOnError(err) {
      this[kPerMessageDeflate]._inflate = null;
      if (this[kError]) {
        this[kCallback](this[kError]);
        return;
      }
      err[kStatusCode] = 1007;
      this[kCallback](err);
    }
  }
});

// node_modules/ws/lib/validation.js
var require_validation = __commonJS({
  "node_modules/ws/lib/validation.js"(exports2, module2) {
    "use strict";
    var { isUtf8 } = require("buffer");
    var { hasBlob } = require_constants();
    var tokenChars = [
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 0 - 15
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 16 - 31
      0,
      1,
      0,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      1,
      1,
      0,
      1,
      1,
      0,
      // 32 - 47
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      // 48 - 63
      0,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 64 - 79
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      1,
      1,
      // 80 - 95
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 96 - 111
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      1,
      0,
      1,
      0
      // 112 - 127
    ];
    function isValidStatusCode(code) {
      return code >= 1e3 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006 || code >= 3e3 && code <= 4999;
    }
    function _isValidUTF8(buf) {
      const len = buf.length;
      let i = 0;
      while (i < len) {
        if ((buf[i] & 128) === 0) {
          i++;
        } else if ((buf[i] & 224) === 192) {
          if (i + 1 === len || (buf[i + 1] & 192) !== 128 || (buf[i] & 254) === 192) {
            return false;
          }
          i += 2;
        } else if ((buf[i] & 240) === 224) {
          if (i + 2 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || buf[i] === 224 && (buf[i + 1] & 224) === 128 || // Overlong
          buf[i] === 237 && (buf[i + 1] & 224) === 160) {
            return false;
          }
          i += 3;
        } else if ((buf[i] & 248) === 240) {
          if (i + 3 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || (buf[i + 3] & 192) !== 128 || buf[i] === 240 && (buf[i + 1] & 240) === 128 || // Overlong
          buf[i] === 244 && buf[i + 1] > 143 || buf[i] > 244) {
            return false;
          }
          i += 4;
        } else {
          return false;
        }
      }
      return true;
    }
    function isBlob(value) {
      return hasBlob && typeof value === "object" && typeof value.arrayBuffer === "function" && typeof value.type === "string" && typeof value.stream === "function" && (value[Symbol.toStringTag] === "Blob" || value[Symbol.toStringTag] === "File");
    }
    module2.exports = {
      isBlob,
      isValidStatusCode,
      isValidUTF8: _isValidUTF8,
      tokenChars
    };
    if (isUtf8) {
      module2.exports.isValidUTF8 = function(buf) {
        return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
      };
    } else if (!process.env.WS_NO_UTF_8_VALIDATE) {
      try {
        const isValidUTF8 = require("utf-8-validate");
        module2.exports.isValidUTF8 = function(buf) {
          return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/receiver.js
var require_receiver = __commonJS({
  "node_modules/ws/lib/receiver.js"(exports2, module2) {
    "use strict";
    var { Writable } = require("stream");
    var PerMessageDeflate2 = require_permessage_deflate();
    var {
      BINARY_TYPES,
      EMPTY_BUFFER,
      kStatusCode,
      kWebSocket
    } = require_constants();
    var { concat, toArrayBuffer, unmask } = require_buffer_util();
    var { isValidStatusCode, isValidUTF8 } = require_validation();
    var FastBuffer = Buffer[Symbol.species];
    var GET_INFO = 0;
    var GET_PAYLOAD_LENGTH_16 = 1;
    var GET_PAYLOAD_LENGTH_64 = 2;
    var GET_MASK = 3;
    var GET_DATA = 4;
    var INFLATING = 5;
    var DEFER_EVENT = 6;
    var Receiver2 = class extends Writable {
      /**
       * Creates a Receiver instance.
       *
       * @param {Object} [options] Options object
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {String} [options.binaryType=nodebuffer] The type for binary data
       * @param {Object} [options.extensions] An object containing the negotiated
       *     extensions
       * @param {Boolean} [options.isServer=false] Specifies whether to operate in
       *     client or server mode
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       */
      constructor(options = {}) {
        super();
        this._allowSynchronousEvents = options.allowSynchronousEvents !== void 0 ? options.allowSynchronousEvents : true;
        this._binaryType = options.binaryType || BINARY_TYPES[0];
        this._extensions = options.extensions || {};
        this._isServer = !!options.isServer;
        this._maxPayload = options.maxPayload | 0;
        this._skipUTF8Validation = !!options.skipUTF8Validation;
        this[kWebSocket] = void 0;
        this._bufferedBytes = 0;
        this._buffers = [];
        this._compressed = false;
        this._payloadLength = 0;
        this._mask = void 0;
        this._fragmented = 0;
        this._masked = false;
        this._fin = false;
        this._opcode = 0;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._fragments = [];
        this._errored = false;
        this._loop = false;
        this._state = GET_INFO;
      }
      /**
       * Implements `Writable.prototype._write()`.
       *
       * @param {Buffer} chunk The chunk of data to write
       * @param {String} encoding The character encoding of `chunk`
       * @param {Function} cb Callback
       * @private
       */
      _write(chunk, encoding, cb) {
        if (this._opcode === 8 && this._state == GET_INFO)
          return cb();
        this._bufferedBytes += chunk.length;
        this._buffers.push(chunk);
        this.startLoop(cb);
      }
      /**
       * Consumes `n` bytes from the buffered data.
       *
       * @param {Number} n The number of bytes to consume
       * @return {Buffer} The consumed bytes
       * @private
       */
      consume(n) {
        this._bufferedBytes -= n;
        if (n === this._buffers[0].length)
          return this._buffers.shift();
        if (n < this._buffers[0].length) {
          const buf = this._buffers[0];
          this._buffers[0] = new FastBuffer(
            buf.buffer,
            buf.byteOffset + n,
            buf.length - n
          );
          return new FastBuffer(buf.buffer, buf.byteOffset, n);
        }
        const dst = Buffer.allocUnsafe(n);
        do {
          const buf = this._buffers[0];
          const offset = dst.length - n;
          if (n >= buf.length) {
            dst.set(this._buffers.shift(), offset);
          } else {
            dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset);
            this._buffers[0] = new FastBuffer(
              buf.buffer,
              buf.byteOffset + n,
              buf.length - n
            );
          }
          n -= buf.length;
        } while (n > 0);
        return dst;
      }
      /**
       * Starts the parsing loop.
       *
       * @param {Function} cb Callback
       * @private
       */
      startLoop(cb) {
        this._loop = true;
        do {
          switch (this._state) {
            case GET_INFO:
              this.getInfo(cb);
              break;
            case GET_PAYLOAD_LENGTH_16:
              this.getPayloadLength16(cb);
              break;
            case GET_PAYLOAD_LENGTH_64:
              this.getPayloadLength64(cb);
              break;
            case GET_MASK:
              this.getMask();
              break;
            case GET_DATA:
              this.getData(cb);
              break;
            case INFLATING:
            case DEFER_EVENT:
              this._loop = false;
              return;
          }
        } while (this._loop);
        if (!this._errored)
          cb();
      }
      /**
       * Reads the first two bytes of a frame.
       *
       * @param {Function} cb Callback
       * @private
       */
      getInfo(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        const buf = this.consume(2);
        if ((buf[0] & 48) !== 0) {
          const error = this.createError(
            RangeError,
            "RSV2 and RSV3 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_2_3"
          );
          cb(error);
          return;
        }
        const compressed = (buf[0] & 64) === 64;
        if (compressed && !this._extensions[PerMessageDeflate2.extensionName]) {
          const error = this.createError(
            RangeError,
            "RSV1 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_1"
          );
          cb(error);
          return;
        }
        this._fin = (buf[0] & 128) === 128;
        this._opcode = buf[0] & 15;
        this._payloadLength = buf[1] & 127;
        if (this._opcode === 0) {
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (!this._fragmented) {
            const error = this.createError(
              RangeError,
              "invalid opcode 0",
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._opcode = this._fragmented;
        } else if (this._opcode === 1 || this._opcode === 2) {
          if (this._fragmented) {
            const error = this.createError(
              RangeError,
              `invalid opcode ${this._opcode}`,
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._compressed = compressed;
        } else if (this._opcode > 7 && this._opcode < 11) {
          if (!this._fin) {
            const error = this.createError(
              RangeError,
              "FIN must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_FIN"
            );
            cb(error);
            return;
          }
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (this._payloadLength > 125 || this._opcode === 8 && this._payloadLength === 1) {
            const error = this.createError(
              RangeError,
              `invalid payload length ${this._payloadLength}`,
              true,
              1002,
              "WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH"
            );
            cb(error);
            return;
          }
        } else {
          const error = this.createError(
            RangeError,
            `invalid opcode ${this._opcode}`,
            true,
            1002,
            "WS_ERR_INVALID_OPCODE"
          );
          cb(error);
          return;
        }
        if (!this._fin && !this._fragmented)
          this._fragmented = this._opcode;
        this._masked = (buf[1] & 128) === 128;
        if (this._isServer) {
          if (!this._masked) {
            const error = this.createError(
              RangeError,
              "MASK must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_MASK"
            );
            cb(error);
            return;
          }
        } else if (this._masked) {
          const error = this.createError(
            RangeError,
            "MASK must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_MASK"
          );
          cb(error);
          return;
        }
        if (this._payloadLength === 126)
          this._state = GET_PAYLOAD_LENGTH_16;
        else if (this._payloadLength === 127)
          this._state = GET_PAYLOAD_LENGTH_64;
        else
          this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+16).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength16(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        this._payloadLength = this.consume(2).readUInt16BE(0);
        this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+64).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength64(cb) {
        if (this._bufferedBytes < 8) {
          this._loop = false;
          return;
        }
        const buf = this.consume(8);
        const num = buf.readUInt32BE(0);
        if (num > Math.pow(2, 53 - 32) - 1) {
          const error = this.createError(
            RangeError,
            "Unsupported WebSocket frame: payload length > 2^53 - 1",
            false,
            1009,
            "WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH"
          );
          cb(error);
          return;
        }
        this._payloadLength = num * Math.pow(2, 32) + buf.readUInt32BE(4);
        this.haveLength(cb);
      }
      /**
       * Payload length has been read.
       *
       * @param {Function} cb Callback
       * @private
       */
      haveLength(cb) {
        if (this._payloadLength && this._opcode < 8) {
          this._totalPayloadLength += this._payloadLength;
          if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
            const error = this.createError(
              RangeError,
              "Max payload size exceeded",
              false,
              1009,
              "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
            );
            cb(error);
            return;
          }
        }
        if (this._masked)
          this._state = GET_MASK;
        else
          this._state = GET_DATA;
      }
      /**
       * Reads mask bytes.
       *
       * @private
       */
      getMask() {
        if (this._bufferedBytes < 4) {
          this._loop = false;
          return;
        }
        this._mask = this.consume(4);
        this._state = GET_DATA;
      }
      /**
       * Reads data bytes.
       *
       * @param {Function} cb Callback
       * @private
       */
      getData(cb) {
        let data = EMPTY_BUFFER;
        if (this._payloadLength) {
          if (this._bufferedBytes < this._payloadLength) {
            this._loop = false;
            return;
          }
          data = this.consume(this._payloadLength);
          if (this._masked && (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0) {
            unmask(data, this._mask);
          }
        }
        if (this._opcode > 7) {
          this.controlMessage(data, cb);
          return;
        }
        if (this._compressed) {
          this._state = INFLATING;
          this.decompress(data, cb);
          return;
        }
        if (data.length) {
          this._messageLength = this._totalPayloadLength;
          this._fragments.push(data);
        }
        this.dataMessage(cb);
      }
      /**
       * Decompresses data.
       *
       * @param {Buffer} data Compressed data
       * @param {Function} cb Callback
       * @private
       */
      decompress(data, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        perMessageDeflate.decompress(data, this._fin, (err, buf) => {
          if (err)
            return cb(err);
          if (buf.length) {
            this._messageLength += buf.length;
            if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
              const error = this.createError(
                RangeError,
                "Max payload size exceeded",
                false,
                1009,
                "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
              );
              cb(error);
              return;
            }
            this._fragments.push(buf);
          }
          this.dataMessage(cb);
          if (this._state === GET_INFO)
            this.startLoop(cb);
        });
      }
      /**
       * Handles a data message.
       *
       * @param {Function} cb Callback
       * @private
       */
      dataMessage(cb) {
        if (!this._fin) {
          this._state = GET_INFO;
          return;
        }
        const messageLength = this._messageLength;
        const fragments = this._fragments;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._fragmented = 0;
        this._fragments = [];
        if (this._opcode === 2) {
          let data;
          if (this._binaryType === "nodebuffer") {
            data = concat(fragments, messageLength);
          } else if (this._binaryType === "arraybuffer") {
            data = toArrayBuffer(concat(fragments, messageLength));
          } else if (this._binaryType === "blob") {
            data = new Blob(fragments);
          } else {
            data = fragments;
          }
          if (this._allowSynchronousEvents) {
            this.emit("message", data, true);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", data, true);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        } else {
          const buf = concat(fragments, messageLength);
          if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
            const error = this.createError(
              Error,
              "invalid UTF-8 sequence",
              true,
              1007,
              "WS_ERR_INVALID_UTF8"
            );
            cb(error);
            return;
          }
          if (this._state === INFLATING || this._allowSynchronousEvents) {
            this.emit("message", buf, false);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", buf, false);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        }
      }
      /**
       * Handles a control message.
       *
       * @param {Buffer} data Data to handle
       * @return {(Error|RangeError|undefined)} A possible error
       * @private
       */
      controlMessage(data, cb) {
        if (this._opcode === 8) {
          if (data.length === 0) {
            this._loop = false;
            this.emit("conclude", 1005, EMPTY_BUFFER);
            this.end();
          } else {
            const code = data.readUInt16BE(0);
            if (!isValidStatusCode(code)) {
              const error = this.createError(
                RangeError,
                `invalid status code ${code}`,
                true,
                1002,
                "WS_ERR_INVALID_CLOSE_CODE"
              );
              cb(error);
              return;
            }
            const buf = new FastBuffer(
              data.buffer,
              data.byteOffset + 2,
              data.length - 2
            );
            if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
              const error = this.createError(
                Error,
                "invalid UTF-8 sequence",
                true,
                1007,
                "WS_ERR_INVALID_UTF8"
              );
              cb(error);
              return;
            }
            this._loop = false;
            this.emit("conclude", code, buf);
            this.end();
          }
          this._state = GET_INFO;
          return;
        }
        if (this._allowSynchronousEvents) {
          this.emit(this._opcode === 9 ? "ping" : "pong", data);
          this._state = GET_INFO;
        } else {
          this._state = DEFER_EVENT;
          setImmediate(() => {
            this.emit(this._opcode === 9 ? "ping" : "pong", data);
            this._state = GET_INFO;
            this.startLoop(cb);
          });
        }
      }
      /**
       * Builds an error object.
       *
       * @param {function(new:Error|RangeError)} ErrorCtor The error constructor
       * @param {String} message The error message
       * @param {Boolean} prefix Specifies whether or not to add a default prefix to
       *     `message`
       * @param {Number} statusCode The status code
       * @param {String} errorCode The exposed error code
       * @return {(Error|RangeError)} The error
       * @private
       */
      createError(ErrorCtor, message, prefix, statusCode, errorCode) {
        this._loop = false;
        this._errored = true;
        const err = new ErrorCtor(
          prefix ? `Invalid WebSocket frame: ${message}` : message
        );
        Error.captureStackTrace(err, this.createError);
        err.code = errorCode;
        err[kStatusCode] = statusCode;
        return err;
      }
    };
    module2.exports = Receiver2;
  }
});

// node_modules/ws/lib/sender.js
var require_sender = __commonJS({
  "node_modules/ws/lib/sender.js"(exports2, module2) {
    "use strict";
    var { Duplex } = require("stream");
    var { randomFillSync } = require("crypto");
    var PerMessageDeflate2 = require_permessage_deflate();
    var { EMPTY_BUFFER, kWebSocket, NOOP } = require_constants();
    var { isBlob, isValidStatusCode } = require_validation();
    var { mask: applyMask, toBuffer } = require_buffer_util();
    var kByteLength = Symbol("kByteLength");
    var maskBuffer = Buffer.alloc(4);
    var RANDOM_POOL_SIZE = 8 * 1024;
    var randomPool;
    var randomPoolPointer = RANDOM_POOL_SIZE;
    var DEFAULT = 0;
    var DEFLATING = 1;
    var GET_BLOB_DATA = 2;
    var Sender2 = class _Sender {
      /**
       * Creates a Sender instance.
       *
       * @param {Duplex} socket The connection socket
       * @param {Object} [extensions] An object containing the negotiated extensions
       * @param {Function} [generateMask] The function used to generate the masking
       *     key
       */
      constructor(socket, extensions, generateMask) {
        this._extensions = extensions || {};
        if (generateMask) {
          this._generateMask = generateMask;
          this._maskBuffer = Buffer.alloc(4);
        }
        this._socket = socket;
        this._firstFragment = true;
        this._compress = false;
        this._bufferedBytes = 0;
        this._queue = [];
        this._state = DEFAULT;
        this.onerror = NOOP;
        this[kWebSocket] = void 0;
      }
      /**
       * Frames a piece of data according to the HyBi WebSocket protocol.
       *
       * @param {(Buffer|String)} data The data to frame
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @return {(Buffer|String)[]} The framed data
       * @public
       */
      static frame(data, options) {
        let mask;
        let merge = false;
        let offset = 2;
        let skipMasking = false;
        if (options.mask) {
          mask = options.maskBuffer || maskBuffer;
          if (options.generateMask) {
            options.generateMask(mask);
          } else {
            if (randomPoolPointer === RANDOM_POOL_SIZE) {
              if (randomPool === void 0) {
                randomPool = Buffer.alloc(RANDOM_POOL_SIZE);
              }
              randomFillSync(randomPool, 0, RANDOM_POOL_SIZE);
              randomPoolPointer = 0;
            }
            mask[0] = randomPool[randomPoolPointer++];
            mask[1] = randomPool[randomPoolPointer++];
            mask[2] = randomPool[randomPoolPointer++];
            mask[3] = randomPool[randomPoolPointer++];
          }
          skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0;
          offset = 6;
        }
        let dataLength;
        if (typeof data === "string") {
          if ((!options.mask || skipMasking) && options[kByteLength] !== void 0) {
            dataLength = options[kByteLength];
          } else {
            data = Buffer.from(data);
            dataLength = data.length;
          }
        } else {
          dataLength = data.length;
          merge = options.mask && options.readOnly && !skipMasking;
        }
        let payloadLength = dataLength;
        if (dataLength >= 65536) {
          offset += 8;
          payloadLength = 127;
        } else if (dataLength > 125) {
          offset += 2;
          payloadLength = 126;
        }
        const target = Buffer.allocUnsafe(merge ? dataLength + offset : offset);
        target[0] = options.fin ? options.opcode | 128 : options.opcode;
        if (options.rsv1)
          target[0] |= 64;
        target[1] = payloadLength;
        if (payloadLength === 126) {
          target.writeUInt16BE(dataLength, 2);
        } else if (payloadLength === 127) {
          target[2] = target[3] = 0;
          target.writeUIntBE(dataLength, 4, 6);
        }
        if (!options.mask)
          return [target, data];
        target[1] |= 128;
        target[offset - 4] = mask[0];
        target[offset - 3] = mask[1];
        target[offset - 2] = mask[2];
        target[offset - 1] = mask[3];
        if (skipMasking)
          return [target, data];
        if (merge) {
          applyMask(data, mask, target, offset, dataLength);
          return [target];
        }
        applyMask(data, mask, data, 0, dataLength);
        return [target, data];
      }
      /**
       * Sends a close message to the other peer.
       *
       * @param {Number} [code] The status code component of the body
       * @param {(String|Buffer)} [data] The message component of the body
       * @param {Boolean} [mask=false] Specifies whether or not to mask the message
       * @param {Function} [cb] Callback
       * @public
       */
      close(code, data, mask, cb) {
        let buf;
        if (code === void 0) {
          buf = EMPTY_BUFFER;
        } else if (typeof code !== "number" || !isValidStatusCode(code)) {
          throw new TypeError("First argument must be a valid error code number");
        } else if (data === void 0 || !data.length) {
          buf = Buffer.allocUnsafe(2);
          buf.writeUInt16BE(code, 0);
        } else {
          const length = Buffer.byteLength(data);
          if (length > 123) {
            throw new RangeError("The message must not be greater than 123 bytes");
          }
          buf = Buffer.allocUnsafe(2 + length);
          buf.writeUInt16BE(code, 0);
          if (typeof data === "string") {
            buf.write(data, 2);
          } else {
            buf.set(data, 2);
          }
        }
        const options = {
          [kByteLength]: buf.length,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 8,
          readOnly: false,
          rsv1: false
        };
        if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, buf, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(buf, options), cb);
        }
      }
      /**
       * Sends a ping message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      ping(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 9,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a pong message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      pong(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 10,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a data message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Object} options Options object
       * @param {Boolean} [options.binary=false] Specifies whether `data` is binary
       *     or text
       * @param {Boolean} [options.compress=false] Specifies whether or not to
       *     compress `data`
       * @param {Boolean} [options.fin=false] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Function} [cb] Callback
       * @public
       */
      send(data, options, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        let opcode = options.binary ? 2 : 1;
        let rsv1 = options.compress;
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (this._firstFragment) {
          this._firstFragment = false;
          if (rsv1 && perMessageDeflate && perMessageDeflate.params[perMessageDeflate._isServer ? "server_no_context_takeover" : "client_no_context_takeover"]) {
            rsv1 = byteLength >= perMessageDeflate._threshold;
          }
          this._compress = rsv1;
        } else {
          rsv1 = false;
          opcode = 0;
        }
        if (options.fin)
          this._firstFragment = true;
        const opts = {
          [kByteLength]: byteLength,
          fin: options.fin,
          generateMask: this._generateMask,
          mask: options.mask,
          maskBuffer: this._maskBuffer,
          opcode,
          readOnly,
          rsv1
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, this._compress, opts, cb]);
          } else {
            this.getBlobData(data, this._compress, opts, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, this._compress, opts, cb]);
        } else {
          this.dispatch(data, this._compress, opts, cb);
        }
      }
      /**
       * Gets the contents of a blob as binary data.
       *
       * @param {Blob} blob The blob
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     the data
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      getBlobData(blob, compress, options, cb) {
        this._bufferedBytes += options[kByteLength];
        this._state = GET_BLOB_DATA;
        blob.arrayBuffer().then((arrayBuffer) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while the blob was being read"
            );
            process.nextTick(callCallbacks, this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          const data = toBuffer(arrayBuffer);
          if (!compress) {
            this._state = DEFAULT;
            this.sendFrame(_Sender.frame(data, options), cb);
            this.dequeue();
          } else {
            this.dispatch(data, compress, options, cb);
          }
        }).catch((err) => {
          process.nextTick(onError, this, err, cb);
        });
      }
      /**
       * Dispatches a message.
       *
       * @param {(Buffer|String)} data The message to send
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     `data`
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      dispatch(data, compress, options, cb) {
        if (!compress) {
          this.sendFrame(_Sender.frame(data, options), cb);
          return;
        }
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        this._bufferedBytes += options[kByteLength];
        this._state = DEFLATING;
        perMessageDeflate.compress(data, options.fin, (_, buf) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while data was being compressed"
            );
            callCallbacks(this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          this._state = DEFAULT;
          options.readOnly = false;
          this.sendFrame(_Sender.frame(buf, options), cb);
          this.dequeue();
        });
      }
      /**
       * Executes queued send operations.
       *
       * @private
       */
      dequeue() {
        while (this._state === DEFAULT && this._queue.length) {
          const params = this._queue.shift();
          this._bufferedBytes -= params[3][kByteLength];
          Reflect.apply(params[0], this, params.slice(1));
        }
      }
      /**
       * Enqueues a send operation.
       *
       * @param {Array} params Send operation parameters.
       * @private
       */
      enqueue(params) {
        this._bufferedBytes += params[3][kByteLength];
        this._queue.push(params);
      }
      /**
       * Sends a frame.
       *
       * @param {(Buffer | String)[]} list The frame to send
       * @param {Function} [cb] Callback
       * @private
       */
      sendFrame(list, cb) {
        if (list.length === 2) {
          this._socket.cork();
          this._socket.write(list[0]);
          this._socket.write(list[1], cb);
          this._socket.uncork();
        } else {
          this._socket.write(list[0], cb);
        }
      }
    };
    module2.exports = Sender2;
    function callCallbacks(sender, err, cb) {
      if (typeof cb === "function")
        cb(err);
      for (let i = 0; i < sender._queue.length; i++) {
        const params = sender._queue[i];
        const callback = params[params.length - 1];
        if (typeof callback === "function")
          callback(err);
      }
    }
    function onError(sender, err, cb) {
      callCallbacks(sender, err, cb);
      sender.onerror(err);
    }
  }
});

// node_modules/ws/lib/event-target.js
var require_event_target = __commonJS({
  "node_modules/ws/lib/event-target.js"(exports2, module2) {
    "use strict";
    var { kForOnEventAttribute, kListener } = require_constants();
    var kCode = Symbol("kCode");
    var kData = Symbol("kData");
    var kError = Symbol("kError");
    var kMessage = Symbol("kMessage");
    var kReason = Symbol("kReason");
    var kTarget = Symbol("kTarget");
    var kType = Symbol("kType");
    var kWasClean = Symbol("kWasClean");
    var Event = class {
      /**
       * Create a new `Event`.
       *
       * @param {String} type The name of the event
       * @throws {TypeError} If the `type` argument is not specified
       */
      constructor(type) {
        this[kTarget] = null;
        this[kType] = type;
      }
      /**
       * @type {*}
       */
      get target() {
        return this[kTarget];
      }
      /**
       * @type {String}
       */
      get type() {
        return this[kType];
      }
    };
    Object.defineProperty(Event.prototype, "target", { enumerable: true });
    Object.defineProperty(Event.prototype, "type", { enumerable: true });
    var CloseEvent = class extends Event {
      /**
       * Create a new `CloseEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {Number} [options.code=0] The status code explaining why the
       *     connection was closed
       * @param {String} [options.reason=''] A human-readable string explaining why
       *     the connection was closed
       * @param {Boolean} [options.wasClean=false] Indicates whether or not the
       *     connection was cleanly closed
       */
      constructor(type, options = {}) {
        super(type);
        this[kCode] = options.code === void 0 ? 0 : options.code;
        this[kReason] = options.reason === void 0 ? "" : options.reason;
        this[kWasClean] = options.wasClean === void 0 ? false : options.wasClean;
      }
      /**
       * @type {Number}
       */
      get code() {
        return this[kCode];
      }
      /**
       * @type {String}
       */
      get reason() {
        return this[kReason];
      }
      /**
       * @type {Boolean}
       */
      get wasClean() {
        return this[kWasClean];
      }
    };
    Object.defineProperty(CloseEvent.prototype, "code", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "reason", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "wasClean", { enumerable: true });
    var ErrorEvent = class extends Event {
      /**
       * Create a new `ErrorEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.error=null] The error that generated this event
       * @param {String} [options.message=''] The error message
       */
      constructor(type, options = {}) {
        super(type);
        this[kError] = options.error === void 0 ? null : options.error;
        this[kMessage] = options.message === void 0 ? "" : options.message;
      }
      /**
       * @type {*}
       */
      get error() {
        return this[kError];
      }
      /**
       * @type {String}
       */
      get message() {
        return this[kMessage];
      }
    };
    Object.defineProperty(ErrorEvent.prototype, "error", { enumerable: true });
    Object.defineProperty(ErrorEvent.prototype, "message", { enumerable: true });
    var MessageEvent = class extends Event {
      /**
       * Create a new `MessageEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.data=null] The message content
       */
      constructor(type, options = {}) {
        super(type);
        this[kData] = options.data === void 0 ? null : options.data;
      }
      /**
       * @type {*}
       */
      get data() {
        return this[kData];
      }
    };
    Object.defineProperty(MessageEvent.prototype, "data", { enumerable: true });
    var EventTarget = {
      /**
       * Register an event listener.
       *
       * @param {String} type A string representing the event type to listen for
       * @param {(Function|Object)} handler The listener to add
       * @param {Object} [options] An options object specifies characteristics about
       *     the event listener
       * @param {Boolean} [options.once=false] A `Boolean` indicating that the
       *     listener should be invoked at most once after being added. If `true`,
       *     the listener would be automatically removed when invoked.
       * @public
       */
      addEventListener(type, handler, options = {}) {
        for (const listener of this.listeners(type)) {
          if (!options[kForOnEventAttribute] && listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            return;
          }
        }
        let wrapper;
        if (type === "message") {
          wrapper = function onMessage(data, isBinary) {
            const event = new MessageEvent("message", {
              data: isBinary ? data : data.toString()
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "close") {
          wrapper = function onClose(code, message) {
            const event = new CloseEvent("close", {
              code,
              reason: message.toString(),
              wasClean: this._closeFrameReceived && this._closeFrameSent
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "error") {
          wrapper = function onError(error) {
            const event = new ErrorEvent("error", {
              error,
              message: error.message
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "open") {
          wrapper = function onOpen() {
            const event = new Event("open");
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else {
          return;
        }
        wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute];
        wrapper[kListener] = handler;
        if (options.once) {
          this.once(type, wrapper);
        } else {
          this.on(type, wrapper);
        }
      },
      /**
       * Remove an event listener.
       *
       * @param {String} type A string representing the event type to remove
       * @param {(Function|Object)} handler The listener to remove
       * @public
       */
      removeEventListener(type, handler) {
        for (const listener of this.listeners(type)) {
          if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            this.removeListener(type, listener);
            break;
          }
        }
      }
    };
    module2.exports = {
      CloseEvent,
      ErrorEvent,
      Event,
      EventTarget,
      MessageEvent
    };
    function callListener(listener, thisArg, event) {
      if (typeof listener === "object" && listener.handleEvent) {
        listener.handleEvent.call(listener, event);
      } else {
        listener.call(thisArg, event);
      }
    }
  }
});

// node_modules/ws/lib/extension.js
var require_extension = __commonJS({
  "node_modules/ws/lib/extension.js"(exports2, module2) {
    "use strict";
    var { tokenChars } = require_validation();
    function push(dest, name, elem) {
      if (dest[name] === void 0)
        dest[name] = [elem];
      else
        dest[name].push(elem);
    }
    function parse(header) {
      const offers = /* @__PURE__ */ Object.create(null);
      let params = /* @__PURE__ */ Object.create(null);
      let mustUnescape = false;
      let isEscaping = false;
      let inQuotes = false;
      let extensionName;
      let paramName;
      let start = -1;
      let code = -1;
      let end = -1;
      let i = 0;
      for (; i < header.length; i++) {
        code = header.charCodeAt(i);
        if (extensionName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1)
              start = i;
          } else if (i !== 0 && (code === 32 || code === 9)) {
            if (end === -1 && start !== -1)
              end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1)
              end = i;
            const name = header.slice(start, end);
            if (code === 44) {
              push(offers, name, params);
              params = /* @__PURE__ */ Object.create(null);
            } else {
              extensionName = name;
            }
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else if (paramName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1)
              start = i;
          } else if (code === 32 || code === 9) {
            if (end === -1 && start !== -1)
              end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1)
              end = i;
            push(params, header.slice(start, end), true);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            start = end = -1;
          } else if (code === 61 && start !== -1 && end === -1) {
            paramName = header.slice(start, i);
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else {
          if (isEscaping) {
            if (tokenChars[code] !== 1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (start === -1)
              start = i;
            else if (!mustUnescape)
              mustUnescape = true;
            isEscaping = false;
          } else if (inQuotes) {
            if (tokenChars[code] === 1) {
              if (start === -1)
                start = i;
            } else if (code === 34 && start !== -1) {
              inQuotes = false;
              end = i;
            } else if (code === 92) {
              isEscaping = true;
            } else {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
          } else if (code === 34 && header.charCodeAt(i - 1) === 61) {
            inQuotes = true;
          } else if (end === -1 && tokenChars[code] === 1) {
            if (start === -1)
              start = i;
          } else if (start !== -1 && (code === 32 || code === 9)) {
            if (end === -1)
              end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1)
              end = i;
            let value = header.slice(start, end);
            if (mustUnescape) {
              value = value.replace(/\\/g, "");
              mustUnescape = false;
            }
            push(params, paramName, value);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            paramName = void 0;
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        }
      }
      if (start === -1 || inQuotes || code === 32 || code === 9) {
        throw new SyntaxError("Unexpected end of input");
      }
      if (end === -1)
        end = i;
      const token = header.slice(start, end);
      if (extensionName === void 0) {
        push(offers, token, params);
      } else {
        if (paramName === void 0) {
          push(params, token, true);
        } else if (mustUnescape) {
          push(params, paramName, token.replace(/\\/g, ""));
        } else {
          push(params, paramName, token);
        }
        push(offers, extensionName, params);
      }
      return offers;
    }
    function format(extensions) {
      return Object.keys(extensions).map((extension2) => {
        let configurations = extensions[extension2];
        if (!Array.isArray(configurations))
          configurations = [configurations];
        return configurations.map((params) => {
          return [extension2].concat(
            Object.keys(params).map((k) => {
              let values = params[k];
              if (!Array.isArray(values))
                values = [values];
              return values.map((v) => v === true ? k : `${k}=${v}`).join("; ");
            })
          ).join("; ");
        }).join(", ");
      }).join(", ");
    }
    module2.exports = { format, parse };
  }
});

// node_modules/ws/lib/websocket.js
var require_websocket = __commonJS({
  "node_modules/ws/lib/websocket.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events");
    var https = require("https");
    var http = require("http");
    var net = require("net");
    var tls = require("tls");
    var { randomBytes, createHash } = require("crypto");
    var { Duplex, Readable } = require("stream");
    var { URL: URL2 } = require("url");
    var PerMessageDeflate2 = require_permessage_deflate();
    var Receiver2 = require_receiver();
    var Sender2 = require_sender();
    var { isBlob } = require_validation();
    var {
      BINARY_TYPES,
      CLOSE_TIMEOUT,
      EMPTY_BUFFER,
      GUID,
      kForOnEventAttribute,
      kListener,
      kStatusCode,
      kWebSocket,
      NOOP
    } = require_constants();
    var {
      EventTarget: { addEventListener, removeEventListener }
    } = require_event_target();
    var { format, parse } = require_extension();
    var { toBuffer } = require_buffer_util();
    var kAborted = Symbol("kAborted");
    var protocolVersions = [8, 13];
    var readyStates = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"];
    var subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/;
    var WebSocket2 = class _WebSocket extends EventEmitter {
      /**
       * Create a new `WebSocket`.
       *
       * @param {(String|URL)} address The URL to which to connect
       * @param {(String|String[])} [protocols] The subprotocols
       * @param {Object} [options] Connection options
       */
      constructor(address, protocols, options) {
        super();
        this._binaryType = BINARY_TYPES[0];
        this._closeCode = 1006;
        this._closeFrameReceived = false;
        this._closeFrameSent = false;
        this._closeMessage = EMPTY_BUFFER;
        this._closeTimer = null;
        this._errorEmitted = false;
        this._extensions = {};
        this._paused = false;
        this._protocol = "";
        this._readyState = _WebSocket.CONNECTING;
        this._receiver = null;
        this._sender = null;
        this._socket = null;
        if (address !== null) {
          this._bufferedAmount = 0;
          this._isServer = false;
          this._redirects = 0;
          if (protocols === void 0) {
            protocols = [];
          } else if (!Array.isArray(protocols)) {
            if (typeof protocols === "object" && protocols !== null) {
              options = protocols;
              protocols = [];
            } else {
              protocols = [protocols];
            }
          }
          initAsClient(this, address, protocols, options);
        } else {
          this._autoPong = options.autoPong;
          this._closeTimeout = options.closeTimeout;
          this._isServer = true;
        }
      }
      /**
       * For historical reasons, the custom "nodebuffer" type is used by the default
       * instead of "blob".
       *
       * @type {String}
       */
      get binaryType() {
        return this._binaryType;
      }
      set binaryType(type) {
        if (!BINARY_TYPES.includes(type))
          return;
        this._binaryType = type;
        if (this._receiver)
          this._receiver._binaryType = type;
      }
      /**
       * @type {Number}
       */
      get bufferedAmount() {
        if (!this._socket)
          return this._bufferedAmount;
        return this._socket._writableState.length + this._sender._bufferedBytes;
      }
      /**
       * @type {String}
       */
      get extensions() {
        return Object.keys(this._extensions).join();
      }
      /**
       * @type {Boolean}
       */
      get isPaused() {
        return this._paused;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onclose() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onerror() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onopen() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onmessage() {
        return null;
      }
      /**
       * @type {String}
       */
      get protocol() {
        return this._protocol;
      }
      /**
       * @type {Number}
       */
      get readyState() {
        return this._readyState;
      }
      /**
       * @type {String}
       */
      get url() {
        return this._url;
      }
      /**
       * Set up the socket and the internal resources.
       *
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Object} options Options object
       * @param {Boolean} [options.allowSynchronousEvents=false] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Number} [options.maxPayload=0] The maximum allowed message size
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @private
       */
      setSocket(socket, head, options) {
        const receiver = new Receiver2({
          allowSynchronousEvents: options.allowSynchronousEvents,
          binaryType: this.binaryType,
          extensions: this._extensions,
          isServer: this._isServer,
          maxPayload: options.maxPayload,
          skipUTF8Validation: options.skipUTF8Validation
        });
        const sender = new Sender2(socket, this._extensions, options.generateMask);
        this._receiver = receiver;
        this._sender = sender;
        this._socket = socket;
        receiver[kWebSocket] = this;
        sender[kWebSocket] = this;
        socket[kWebSocket] = this;
        receiver.on("conclude", receiverOnConclude);
        receiver.on("drain", receiverOnDrain);
        receiver.on("error", receiverOnError);
        receiver.on("message", receiverOnMessage);
        receiver.on("ping", receiverOnPing);
        receiver.on("pong", receiverOnPong);
        sender.onerror = senderOnError;
        if (socket.setTimeout)
          socket.setTimeout(0);
        if (socket.setNoDelay)
          socket.setNoDelay();
        if (head.length > 0)
          socket.unshift(head);
        socket.on("close", socketOnClose);
        socket.on("data", socketOnData);
        socket.on("end", socketOnEnd);
        socket.on("error", socketOnError);
        this._readyState = _WebSocket.OPEN;
        this.emit("open");
      }
      /**
       * Emit the `'close'` event.
       *
       * @private
       */
      emitClose() {
        if (!this._socket) {
          this._readyState = _WebSocket.CLOSED;
          this.emit("close", this._closeCode, this._closeMessage);
          return;
        }
        if (this._extensions[PerMessageDeflate2.extensionName]) {
          this._extensions[PerMessageDeflate2.extensionName].cleanup();
        }
        this._receiver.removeAllListeners();
        this._readyState = _WebSocket.CLOSED;
        this.emit("close", this._closeCode, this._closeMessage);
      }
      /**
       * Start a closing handshake.
       *
       *          +----------+   +-----------+   +----------+
       *     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
       *    |     +----------+   +-----------+   +----------+     |
       *          +----------+   +-----------+         |
       * CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
       *          +----------+   +-----------+   |
       *    |           |                        |   +---+        |
       *                +------------------------+-->|fin| - - - -
       *    |         +---+                      |   +---+
       *     - - - - -|fin|<---------------------+
       *              +---+
       *
       * @param {Number} [code] Status code explaining why the connection is closing
       * @param {(String|Buffer)} [data] The reason why the connection is
       *     closing
       * @public
       */
      close(code, data) {
        if (this.readyState === _WebSocket.CLOSED)
          return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this.readyState === _WebSocket.CLOSING) {
          if (this._closeFrameSent && (this._closeFrameReceived || this._receiver._writableState.errorEmitted)) {
            this._socket.end();
          }
          return;
        }
        this._readyState = _WebSocket.CLOSING;
        this._sender.close(code, data, !this._isServer, (err) => {
          if (err)
            return;
          this._closeFrameSent = true;
          if (this._closeFrameReceived || this._receiver._writableState.errorEmitted) {
            this._socket.end();
          }
        });
        setCloseTimer(this);
      }
      /**
       * Pause the socket.
       *
       * @public
       */
      pause() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = true;
        this._socket.pause();
      }
      /**
       * Send a ping.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the ping is sent
       * @public
       */
      ping(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number")
          data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0)
          mask = !this._isServer;
        this._sender.ping(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Send a pong.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the pong is sent
       * @public
       */
      pong(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number")
          data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0)
          mask = !this._isServer;
        this._sender.pong(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Resume the socket.
       *
       * @public
       */
      resume() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = false;
        if (!this._receiver._writableState.needDrain)
          this._socket.resume();
      }
      /**
       * Send a data message.
       *
       * @param {*} data The message to send
       * @param {Object} [options] Options object
       * @param {Boolean} [options.binary] Specifies whether `data` is binary or
       *     text
       * @param {Boolean} [options.compress] Specifies whether or not to compress
       *     `data`
       * @param {Boolean} [options.fin=true] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when data is written out
       * @public
       */
      send(data, options, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof options === "function") {
          cb = options;
          options = {};
        }
        if (typeof data === "number")
          data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        const opts = {
          binary: typeof data !== "string",
          mask: !this._isServer,
          compress: true,
          fin: true,
          ...options
        };
        if (!this._extensions[PerMessageDeflate2.extensionName]) {
          opts.compress = false;
        }
        this._sender.send(data || EMPTY_BUFFER, opts, cb);
      }
      /**
       * Forcibly close the connection.
       *
       * @public
       */
      terminate() {
        if (this.readyState === _WebSocket.CLOSED)
          return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this._socket) {
          this._readyState = _WebSocket.CLOSING;
          this._socket.destroy();
        }
      }
    };
    Object.defineProperty(WebSocket2, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2.prototype, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2.prototype, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    [
      "binaryType",
      "bufferedAmount",
      "extensions",
      "isPaused",
      "protocol",
      "readyState",
      "url"
    ].forEach((property) => {
      Object.defineProperty(WebSocket2.prototype, property, { enumerable: true });
    });
    ["open", "error", "close", "message"].forEach((method) => {
      Object.defineProperty(WebSocket2.prototype, `on${method}`, {
        enumerable: true,
        get() {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute])
              return listener[kListener];
          }
          return null;
        },
        set(handler) {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) {
              this.removeListener(method, listener);
              break;
            }
          }
          if (typeof handler !== "function")
            return;
          this.addEventListener(method, handler, {
            [kForOnEventAttribute]: true
          });
        }
      });
    });
    WebSocket2.prototype.addEventListener = addEventListener;
    WebSocket2.prototype.removeEventListener = removeEventListener;
    module2.exports = WebSocket2;
    function initAsClient(websocket, address, protocols, options) {
      const opts = {
        allowSynchronousEvents: true,
        autoPong: true,
        closeTimeout: CLOSE_TIMEOUT,
        protocolVersion: protocolVersions[1],
        maxPayload: 100 * 1024 * 1024,
        skipUTF8Validation: false,
        perMessageDeflate: true,
        followRedirects: false,
        maxRedirects: 10,
        ...options,
        socketPath: void 0,
        hostname: void 0,
        protocol: void 0,
        timeout: void 0,
        method: "GET",
        host: void 0,
        path: void 0,
        port: void 0
      };
      websocket._autoPong = opts.autoPong;
      websocket._closeTimeout = opts.closeTimeout;
      if (!protocolVersions.includes(opts.protocolVersion)) {
        throw new RangeError(
          `Unsupported protocol version: ${opts.protocolVersion} (supported versions: ${protocolVersions.join(", ")})`
        );
      }
      let parsedUrl;
      if (address instanceof URL2) {
        parsedUrl = address;
      } else {
        try {
          parsedUrl = new URL2(address);
        } catch {
          throw new SyntaxError(`Invalid URL: ${address}`);
        }
      }
      if (parsedUrl.protocol === "http:") {
        parsedUrl.protocol = "ws:";
      } else if (parsedUrl.protocol === "https:") {
        parsedUrl.protocol = "wss:";
      }
      websocket._url = parsedUrl.href;
      const isSecure = parsedUrl.protocol === "wss:";
      const isIpcUrl = parsedUrl.protocol === "ws+unix:";
      let invalidUrlMessage;
      if (parsedUrl.protocol !== "ws:" && !isSecure && !isIpcUrl) {
        invalidUrlMessage = `The URL's protocol must be one of "ws:", "wss:", "http:", "https:", or "ws+unix:"`;
      } else if (isIpcUrl && !parsedUrl.pathname) {
        invalidUrlMessage = "The URL's pathname is empty";
      } else if (parsedUrl.hash) {
        invalidUrlMessage = "The URL contains a fragment identifier";
      }
      if (invalidUrlMessage) {
        const err = new SyntaxError(invalidUrlMessage);
        if (websocket._redirects === 0) {
          throw err;
        } else {
          emitErrorAndClose(websocket, err);
          return;
        }
      }
      const defaultPort = isSecure ? 443 : 80;
      const key = randomBytes(16).toString("base64");
      const request = isSecure ? https.request : http.request;
      const protocolSet = /* @__PURE__ */ new Set();
      let perMessageDeflate;
      opts.createConnection = opts.createConnection || (isSecure ? tlsConnect : netConnect);
      opts.defaultPort = opts.defaultPort || defaultPort;
      opts.port = parsedUrl.port || defaultPort;
      opts.host = parsedUrl.hostname.startsWith("[") ? parsedUrl.hostname.slice(1, -1) : parsedUrl.hostname;
      opts.headers = {
        ...opts.headers,
        "Sec-WebSocket-Version": opts.protocolVersion,
        "Sec-WebSocket-Key": key,
        Connection: "Upgrade",
        Upgrade: "websocket"
      };
      opts.path = parsedUrl.pathname + parsedUrl.search;
      opts.timeout = opts.handshakeTimeout;
      if (opts.perMessageDeflate) {
        perMessageDeflate = new PerMessageDeflate2({
          ...opts.perMessageDeflate,
          isServer: false,
          maxPayload: opts.maxPayload
        });
        opts.headers["Sec-WebSocket-Extensions"] = format({
          [PerMessageDeflate2.extensionName]: perMessageDeflate.offer()
        });
      }
      if (protocols.length) {
        for (const protocol of protocols) {
          if (typeof protocol !== "string" || !subprotocolRegex.test(protocol) || protocolSet.has(protocol)) {
            throw new SyntaxError(
              "An invalid or duplicated subprotocol was specified"
            );
          }
          protocolSet.add(protocol);
        }
        opts.headers["Sec-WebSocket-Protocol"] = protocols.join(",");
      }
      if (opts.origin) {
        if (opts.protocolVersion < 13) {
          opts.headers["Sec-WebSocket-Origin"] = opts.origin;
        } else {
          opts.headers.Origin = opts.origin;
        }
      }
      if (parsedUrl.username || parsedUrl.password) {
        opts.auth = `${parsedUrl.username}:${parsedUrl.password}`;
      }
      if (isIpcUrl) {
        const parts = opts.path.split(":");
        opts.socketPath = parts[0];
        opts.path = parts[1];
      }
      let req;
      if (opts.followRedirects) {
        if (websocket._redirects === 0) {
          websocket._originalIpc = isIpcUrl;
          websocket._originalSecure = isSecure;
          websocket._originalHostOrSocketPath = isIpcUrl ? opts.socketPath : parsedUrl.host;
          const headers = options && options.headers;
          options = { ...options, headers: {} };
          if (headers) {
            for (const [key2, value] of Object.entries(headers)) {
              options.headers[key2.toLowerCase()] = value;
            }
          }
        } else if (websocket.listenerCount("redirect") === 0) {
          const isSameHost = isIpcUrl ? websocket._originalIpc ? opts.socketPath === websocket._originalHostOrSocketPath : false : websocket._originalIpc ? false : parsedUrl.host === websocket._originalHostOrSocketPath;
          if (!isSameHost || websocket._originalSecure && !isSecure) {
            delete opts.headers.authorization;
            delete opts.headers.cookie;
            if (!isSameHost)
              delete opts.headers.host;
            opts.auth = void 0;
          }
        }
        if (opts.auth && !options.headers.authorization) {
          options.headers.authorization = "Basic " + Buffer.from(opts.auth).toString("base64");
        }
        req = websocket._req = request(opts);
        if (websocket._redirects) {
          websocket.emit("redirect", websocket.url, req);
        }
      } else {
        req = websocket._req = request(opts);
      }
      if (opts.timeout) {
        req.on("timeout", () => {
          abortHandshake(websocket, req, "Opening handshake has timed out");
        });
      }
      req.on("error", (err) => {
        if (req === null || req[kAborted])
          return;
        req = websocket._req = null;
        emitErrorAndClose(websocket, err);
      });
      req.on("response", (res) => {
        const location = res.headers.location;
        const statusCode = res.statusCode;
        if (location && opts.followRedirects && statusCode >= 300 && statusCode < 400) {
          if (++websocket._redirects > opts.maxRedirects) {
            abortHandshake(websocket, req, "Maximum redirects exceeded");
            return;
          }
          req.abort();
          let addr;
          try {
            addr = new URL2(location, address);
          } catch (e) {
            const err = new SyntaxError(`Invalid URL: ${location}`);
            emitErrorAndClose(websocket, err);
            return;
          }
          initAsClient(websocket, addr, protocols, options);
        } else if (!websocket.emit("unexpected-response", req, res)) {
          abortHandshake(
            websocket,
            req,
            `Unexpected server response: ${res.statusCode}`
          );
        }
      });
      req.on("upgrade", (res, socket, head) => {
        websocket.emit("upgrade", res);
        if (websocket.readyState !== WebSocket2.CONNECTING)
          return;
        req = websocket._req = null;
        const upgrade = res.headers.upgrade;
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          abortHandshake(websocket, socket, "Invalid Upgrade header");
          return;
        }
        const digest = createHash("sha1").update(key + GUID).digest("base64");
        if (res.headers["sec-websocket-accept"] !== digest) {
          abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Accept header");
          return;
        }
        const serverProt = res.headers["sec-websocket-protocol"];
        let protError;
        if (serverProt !== void 0) {
          if (!protocolSet.size) {
            protError = "Server sent a subprotocol but none was requested";
          } else if (!protocolSet.has(serverProt)) {
            protError = "Server sent an invalid subprotocol";
          }
        } else if (protocolSet.size) {
          protError = "Server sent no subprotocol";
        }
        if (protError) {
          abortHandshake(websocket, socket, protError);
          return;
        }
        if (serverProt)
          websocket._protocol = serverProt;
        const secWebSocketExtensions = res.headers["sec-websocket-extensions"];
        if (secWebSocketExtensions !== void 0) {
          if (!perMessageDeflate) {
            const message = "Server sent a Sec-WebSocket-Extensions header but no extension was requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          let extensions;
          try {
            extensions = parse(secWebSocketExtensions);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          const extensionNames = Object.keys(extensions);
          if (extensionNames.length !== 1 || extensionNames[0] !== PerMessageDeflate2.extensionName) {
            const message = "Server indicated an extension that was not requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          try {
            perMessageDeflate.accept(extensions[PerMessageDeflate2.extensionName]);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          websocket._extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
        }
        websocket.setSocket(socket, head, {
          allowSynchronousEvents: opts.allowSynchronousEvents,
          generateMask: opts.generateMask,
          maxPayload: opts.maxPayload,
          skipUTF8Validation: opts.skipUTF8Validation
        });
      });
      if (opts.finishRequest) {
        opts.finishRequest(req, websocket);
      } else {
        req.end();
      }
    }
    function emitErrorAndClose(websocket, err) {
      websocket._readyState = WebSocket2.CLOSING;
      websocket._errorEmitted = true;
      websocket.emit("error", err);
      websocket.emitClose();
    }
    function netConnect(options) {
      options.path = options.socketPath;
      return net.connect(options);
    }
    function tlsConnect(options) {
      options.path = void 0;
      if (!options.servername && options.servername !== "") {
        options.servername = net.isIP(options.host) ? "" : options.host;
      }
      return tls.connect(options);
    }
    function abortHandshake(websocket, stream, message) {
      websocket._readyState = WebSocket2.CLOSING;
      const err = new Error(message);
      Error.captureStackTrace(err, abortHandshake);
      if (stream.setHeader) {
        stream[kAborted] = true;
        stream.abort();
        if (stream.socket && !stream.socket.destroyed) {
          stream.socket.destroy();
        }
        process.nextTick(emitErrorAndClose, websocket, err);
      } else {
        stream.destroy(err);
        stream.once("error", websocket.emit.bind(websocket, "error"));
        stream.once("close", websocket.emitClose.bind(websocket));
      }
    }
    function sendAfterClose(websocket, data, cb) {
      if (data) {
        const length = isBlob(data) ? data.size : toBuffer(data).length;
        if (websocket._socket)
          websocket._sender._bufferedBytes += length;
        else
          websocket._bufferedAmount += length;
      }
      if (cb) {
        const err = new Error(
          `WebSocket is not open: readyState ${websocket.readyState} (${readyStates[websocket.readyState]})`
        );
        process.nextTick(cb, err);
      }
    }
    function receiverOnConclude(code, reason) {
      const websocket = this[kWebSocket];
      websocket._closeFrameReceived = true;
      websocket._closeMessage = reason;
      websocket._closeCode = code;
      if (websocket._socket[kWebSocket] === void 0)
        return;
      websocket._socket.removeListener("data", socketOnData);
      process.nextTick(resume, websocket._socket);
      if (code === 1005)
        websocket.close();
      else
        websocket.close(code, reason);
    }
    function receiverOnDrain() {
      const websocket = this[kWebSocket];
      if (!websocket.isPaused)
        websocket._socket.resume();
    }
    function receiverOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket._socket[kWebSocket] !== void 0) {
        websocket._socket.removeListener("data", socketOnData);
        process.nextTick(resume, websocket._socket);
        websocket.close(err[kStatusCode]);
      }
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function receiverOnFinish() {
      this[kWebSocket].emitClose();
    }
    function receiverOnMessage(data, isBinary) {
      this[kWebSocket].emit("message", data, isBinary);
    }
    function receiverOnPing(data) {
      const websocket = this[kWebSocket];
      if (websocket._autoPong)
        websocket.pong(data, !this._isServer, NOOP);
      websocket.emit("ping", data);
    }
    function receiverOnPong(data) {
      this[kWebSocket].emit("pong", data);
    }
    function resume(stream) {
      stream.resume();
    }
    function senderOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket.readyState === WebSocket2.CLOSED)
        return;
      if (websocket.readyState === WebSocket2.OPEN) {
        websocket._readyState = WebSocket2.CLOSING;
        setCloseTimer(websocket);
      }
      this._socket.end();
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function setCloseTimer(websocket) {
      websocket._closeTimer = setTimeout(
        websocket._socket.destroy.bind(websocket._socket),
        websocket._closeTimeout
      );
    }
    function socketOnClose() {
      const websocket = this[kWebSocket];
      this.removeListener("close", socketOnClose);
      this.removeListener("data", socketOnData);
      this.removeListener("end", socketOnEnd);
      websocket._readyState = WebSocket2.CLOSING;
      if (!this._readableState.endEmitted && !websocket._closeFrameReceived && !websocket._receiver._writableState.errorEmitted && this._readableState.length !== 0) {
        const chunk = this.read(this._readableState.length);
        websocket._receiver.write(chunk);
      }
      websocket._receiver.end();
      this[kWebSocket] = void 0;
      clearTimeout(websocket._closeTimer);
      if (websocket._receiver._writableState.finished || websocket._receiver._writableState.errorEmitted) {
        websocket.emitClose();
      } else {
        websocket._receiver.on("error", receiverOnFinish);
        websocket._receiver.on("finish", receiverOnFinish);
      }
    }
    function socketOnData(chunk) {
      if (!this[kWebSocket]._receiver.write(chunk)) {
        this.pause();
      }
    }
    function socketOnEnd() {
      const websocket = this[kWebSocket];
      websocket._readyState = WebSocket2.CLOSING;
      websocket._receiver.end();
      this.end();
    }
    function socketOnError() {
      const websocket = this[kWebSocket];
      this.removeListener("error", socketOnError);
      this.on("error", NOOP);
      if (websocket) {
        websocket._readyState = WebSocket2.CLOSING;
        this.destroy();
      }
    }
  }
});

// node_modules/ws/lib/stream.js
var require_stream = __commonJS({
  "node_modules/ws/lib/stream.js"(exports2, module2) {
    "use strict";
    var WebSocket2 = require_websocket();
    var { Duplex } = require("stream");
    function emitClose(stream) {
      stream.emit("close");
    }
    function duplexOnEnd() {
      if (!this.destroyed && this._writableState.finished) {
        this.destroy();
      }
    }
    function duplexOnError(err) {
      this.removeListener("error", duplexOnError);
      this.destroy();
      if (this.listenerCount("error") === 0) {
        this.emit("error", err);
      }
    }
    function createWebSocketStream2(ws, options) {
      let terminateOnDestroy = true;
      const duplex = new Duplex({
        ...options,
        autoDestroy: false,
        emitClose: false,
        objectMode: false,
        writableObjectMode: false
      });
      ws.on("message", function message(msg, isBinary) {
        const data = !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;
        if (!duplex.push(data))
          ws.pause();
      });
      ws.once("error", function error(err) {
        if (duplex.destroyed)
          return;
        terminateOnDestroy = false;
        duplex.destroy(err);
      });
      ws.once("close", function close() {
        if (duplex.destroyed)
          return;
        duplex.push(null);
      });
      duplex._destroy = function(err, callback) {
        if (ws.readyState === ws.CLOSED) {
          callback(err);
          process.nextTick(emitClose, duplex);
          return;
        }
        let called = false;
        ws.once("error", function error(err2) {
          called = true;
          callback(err2);
        });
        ws.once("close", function close() {
          if (!called)
            callback(err);
          process.nextTick(emitClose, duplex);
        });
        if (terminateOnDestroy)
          ws.terminate();
      };
      duplex._final = function(callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._final(callback);
          });
          return;
        }
        if (ws._socket === null)
          return;
        if (ws._socket._writableState.finished) {
          callback();
          if (duplex._readableState.endEmitted)
            duplex.destroy();
        } else {
          ws._socket.once("finish", function finish() {
            callback();
          });
          ws.close();
        }
      };
      duplex._read = function() {
        if (ws.isPaused)
          ws.resume();
      };
      duplex._write = function(chunk, encoding, callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._write(chunk, encoding, callback);
          });
          return;
        }
        ws.send(chunk, callback);
      };
      duplex.on("end", duplexOnEnd);
      duplex.on("error", duplexOnError);
      return duplex;
    }
    module2.exports = createWebSocketStream2;
  }
});

// node_modules/ws/lib/subprotocol.js
var require_subprotocol = __commonJS({
  "node_modules/ws/lib/subprotocol.js"(exports2, module2) {
    "use strict";
    var { tokenChars } = require_validation();
    function parse(header) {
      const protocols = /* @__PURE__ */ new Set();
      let start = -1;
      let end = -1;
      let i = 0;
      for (i; i < header.length; i++) {
        const code = header.charCodeAt(i);
        if (end === -1 && tokenChars[code] === 1) {
          if (start === -1)
            start = i;
        } else if (i !== 0 && (code === 32 || code === 9)) {
          if (end === -1 && start !== -1)
            end = i;
        } else if (code === 44) {
          if (start === -1) {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
          if (end === -1)
            end = i;
          const protocol2 = header.slice(start, end);
          if (protocols.has(protocol2)) {
            throw new SyntaxError(`The "${protocol2}" subprotocol is duplicated`);
          }
          protocols.add(protocol2);
          start = end = -1;
        } else {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
      }
      if (start === -1 || end !== -1) {
        throw new SyntaxError("Unexpected end of input");
      }
      const protocol = header.slice(start, i);
      if (protocols.has(protocol)) {
        throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
      }
      protocols.add(protocol);
      return protocols;
    }
    module2.exports = { parse };
  }
});

// node_modules/ws/lib/websocket-server.js
var require_websocket_server = __commonJS({
  "node_modules/ws/lib/websocket-server.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events");
    var http = require("http");
    var { Duplex } = require("stream");
    var { createHash } = require("crypto");
    var extension2 = require_extension();
    var PerMessageDeflate2 = require_permessage_deflate();
    var subprotocol2 = require_subprotocol();
    var WebSocket2 = require_websocket();
    var { CLOSE_TIMEOUT, GUID, kWebSocket } = require_constants();
    var keyRegex = /^[+/0-9A-Za-z]{22}==$/;
    var RUNNING = 0;
    var CLOSING = 1;
    var CLOSED = 2;
    var WebSocketServer2 = class extends EventEmitter {
      /**
       * Create a `WebSocketServer` instance.
       *
       * @param {Object} options Configuration options
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Boolean} [options.autoPong=true] Specifies whether or not to
       *     automatically send a pong in response to a ping
       * @param {Number} [options.backlog=511] The maximum length of the queue of
       *     pending connections
       * @param {Boolean} [options.clientTracking=true] Specifies whether or not to
       *     track clients
       * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to
       *     wait for the closing handshake to finish after `websocket.close()` is
       *     called
       * @param {Function} [options.handleProtocols] A hook to handle protocols
       * @param {String} [options.host] The hostname where to bind the server
       * @param {Number} [options.maxPayload=104857600] The maximum allowed message
       *     size
       * @param {Boolean} [options.noServer=false] Enable no server mode
       * @param {String} [options.path] Accept only connections matching this path
       * @param {(Boolean|Object)} [options.perMessageDeflate=false] Enable/disable
       *     permessage-deflate
       * @param {Number} [options.port] The port where to bind the server
       * @param {(http.Server|https.Server)} [options.server] A pre-created HTTP/S
       *     server to use
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @param {Function} [options.verifyClient] A hook to reject connections
       * @param {Function} [options.WebSocket=WebSocket] Specifies the `WebSocket`
       *     class to use. It must be the `WebSocket` class or class that extends it
       * @param {Function} [callback] A listener for the `listening` event
       */
      constructor(options, callback) {
        super();
        options = {
          allowSynchronousEvents: true,
          autoPong: true,
          maxPayload: 100 * 1024 * 1024,
          skipUTF8Validation: false,
          perMessageDeflate: false,
          handleProtocols: null,
          clientTracking: true,
          closeTimeout: CLOSE_TIMEOUT,
          verifyClient: null,
          noServer: false,
          backlog: null,
          // use default (511 as implemented in net.js)
          server: null,
          host: null,
          path: null,
          port: null,
          WebSocket: WebSocket2,
          ...options
        };
        if (options.port == null && !options.server && !options.noServer || options.port != null && (options.server || options.noServer) || options.server && options.noServer) {
          throw new TypeError(
            'One and only one of the "port", "server", or "noServer" options must be specified'
          );
        }
        if (options.port != null) {
          this._server = http.createServer((req, res) => {
            const body = http.STATUS_CODES[426];
            res.writeHead(426, {
              "Content-Length": body.length,
              "Content-Type": "text/plain"
            });
            res.end(body);
          });
          this._server.listen(
            options.port,
            options.host,
            options.backlog,
            callback
          );
        } else if (options.server) {
          this._server = options.server;
        }
        if (this._server) {
          const emitConnection = this.emit.bind(this, "connection");
          this._removeListeners = addListeners(this._server, {
            listening: this.emit.bind(this, "listening"),
            error: this.emit.bind(this, "error"),
            upgrade: (req, socket, head) => {
              this.handleUpgrade(req, socket, head, emitConnection);
            }
          });
        }
        if (options.perMessageDeflate === true)
          options.perMessageDeflate = {};
        if (options.clientTracking) {
          this.clients = /* @__PURE__ */ new Set();
          this._shouldEmitClose = false;
        }
        this.options = options;
        this._state = RUNNING;
      }
      /**
       * Returns the bound address, the address family name, and port of the server
       * as reported by the operating system if listening on an IP socket.
       * If the server is listening on a pipe or UNIX domain socket, the name is
       * returned as a string.
       *
       * @return {(Object|String|null)} The address of the server
       * @public
       */
      address() {
        if (this.options.noServer) {
          throw new Error('The server is operating in "noServer" mode');
        }
        if (!this._server)
          return null;
        return this._server.address();
      }
      /**
       * Stop the server from accepting new connections and emit the `'close'` event
       * when all existing connections are closed.
       *
       * @param {Function} [cb] A one-time listener for the `'close'` event
       * @public
       */
      close(cb) {
        if (this._state === CLOSED) {
          if (cb) {
            this.once("close", () => {
              cb(new Error("The server is not running"));
            });
          }
          process.nextTick(emitClose, this);
          return;
        }
        if (cb)
          this.once("close", cb);
        if (this._state === CLOSING)
          return;
        this._state = CLOSING;
        if (this.options.noServer || this.options.server) {
          if (this._server) {
            this._removeListeners();
            this._removeListeners = this._server = null;
          }
          if (this.clients) {
            if (!this.clients.size) {
              process.nextTick(emitClose, this);
            } else {
              this._shouldEmitClose = true;
            }
          } else {
            process.nextTick(emitClose, this);
          }
        } else {
          const server = this._server;
          this._removeListeners();
          this._removeListeners = this._server = null;
          server.close(() => {
            emitClose(this);
          });
        }
      }
      /**
       * See if a given request should be handled by this server instance.
       *
       * @param {http.IncomingMessage} req Request object to inspect
       * @return {Boolean} `true` if the request is valid, else `false`
       * @public
       */
      shouldHandle(req) {
        if (this.options.path) {
          const index = req.url.indexOf("?");
          const pathname = index !== -1 ? req.url.slice(0, index) : req.url;
          if (pathname !== this.options.path)
            return false;
        }
        return true;
      }
      /**
       * Handle a HTTP Upgrade request.
       *
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @public
       */
      handleUpgrade(req, socket, head, cb) {
        socket.on("error", socketOnError);
        const key = req.headers["sec-websocket-key"];
        const upgrade = req.headers.upgrade;
        const version = +req.headers["sec-websocket-version"];
        if (req.method !== "GET") {
          const message = "Invalid HTTP method";
          abortHandshakeOrEmitwsClientError(this, req, socket, 405, message);
          return;
        }
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          const message = "Invalid Upgrade header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (key === void 0 || !keyRegex.test(key)) {
          const message = "Missing or invalid Sec-WebSocket-Key header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (version !== 13 && version !== 8) {
          const message = "Missing or invalid Sec-WebSocket-Version header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message, {
            "Sec-WebSocket-Version": "13, 8"
          });
          return;
        }
        if (!this.shouldHandle(req)) {
          abortHandshake(socket, 400);
          return;
        }
        const secWebSocketProtocol = req.headers["sec-websocket-protocol"];
        let protocols = /* @__PURE__ */ new Set();
        if (secWebSocketProtocol !== void 0) {
          try {
            protocols = subprotocol2.parse(secWebSocketProtocol);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Protocol header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        const secWebSocketExtensions = req.headers["sec-websocket-extensions"];
        const extensions = {};
        if (this.options.perMessageDeflate && secWebSocketExtensions !== void 0) {
          const perMessageDeflate = new PerMessageDeflate2({
            ...this.options.perMessageDeflate,
            isServer: true,
            maxPayload: this.options.maxPayload
          });
          try {
            const offers = extension2.parse(secWebSocketExtensions);
            if (offers[PerMessageDeflate2.extensionName]) {
              perMessageDeflate.accept(offers[PerMessageDeflate2.extensionName]);
              extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
            }
          } catch (err) {
            const message = "Invalid or unacceptable Sec-WebSocket-Extensions header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        if (this.options.verifyClient) {
          const info = {
            origin: req.headers[`${version === 8 ? "sec-websocket-origin" : "origin"}`],
            secure: !!(req.socket.authorized || req.socket.encrypted),
            req
          };
          if (this.options.verifyClient.length === 2) {
            this.options.verifyClient(info, (verified, code, message, headers) => {
              if (!verified) {
                return abortHandshake(socket, code || 401, message, headers);
              }
              this.completeUpgrade(
                extensions,
                key,
                protocols,
                req,
                socket,
                head,
                cb
              );
            });
            return;
          }
          if (!this.options.verifyClient(info))
            return abortHandshake(socket, 401);
        }
        this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
      }
      /**
       * Upgrade the connection to WebSocket.
       *
       * @param {Object} extensions The accepted extensions
       * @param {String} key The value of the `Sec-WebSocket-Key` header
       * @param {Set} protocols The subprotocols
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @throws {Error} If called more than once with the same socket
       * @private
       */
      completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
        if (!socket.readable || !socket.writable)
          return socket.destroy();
        if (socket[kWebSocket]) {
          throw new Error(
            "server.handleUpgrade() was called more than once with the same socket, possibly due to a misconfiguration"
          );
        }
        if (this._state > RUNNING)
          return abortHandshake(socket, 503);
        const digest = createHash("sha1").update(key + GUID).digest("base64");
        const headers = [
          "HTTP/1.1 101 Switching Protocols",
          "Upgrade: websocket",
          "Connection: Upgrade",
          `Sec-WebSocket-Accept: ${digest}`
        ];
        const ws = new this.options.WebSocket(null, void 0, this.options);
        if (protocols.size) {
          const protocol = this.options.handleProtocols ? this.options.handleProtocols(protocols, req) : protocols.values().next().value;
          if (protocol) {
            headers.push(`Sec-WebSocket-Protocol: ${protocol}`);
            ws._protocol = protocol;
          }
        }
        if (extensions[PerMessageDeflate2.extensionName]) {
          const params = extensions[PerMessageDeflate2.extensionName].params;
          const value = extension2.format({
            [PerMessageDeflate2.extensionName]: [params]
          });
          headers.push(`Sec-WebSocket-Extensions: ${value}`);
          ws._extensions = extensions;
        }
        this.emit("headers", headers, req);
        socket.write(headers.concat("\r\n").join("\r\n"));
        socket.removeListener("error", socketOnError);
        ws.setSocket(socket, head, {
          allowSynchronousEvents: this.options.allowSynchronousEvents,
          maxPayload: this.options.maxPayload,
          skipUTF8Validation: this.options.skipUTF8Validation
        });
        if (this.clients) {
          this.clients.add(ws);
          ws.on("close", () => {
            this.clients.delete(ws);
            if (this._shouldEmitClose && !this.clients.size) {
              process.nextTick(emitClose, this);
            }
          });
        }
        cb(ws, req);
      }
    };
    module2.exports = WebSocketServer2;
    function addListeners(server, map) {
      for (const event of Object.keys(map))
        server.on(event, map[event]);
      return function removeListeners() {
        for (const event of Object.keys(map)) {
          server.removeListener(event, map[event]);
        }
      };
    }
    function emitClose(server) {
      server._state = CLOSED;
      server.emit("close");
    }
    function socketOnError() {
      this.destroy();
    }
    function abortHandshake(socket, code, message, headers) {
      message = message || http.STATUS_CODES[code];
      headers = {
        Connection: "close",
        "Content-Type": "text/html",
        "Content-Length": Buffer.byteLength(message),
        ...headers
      };
      socket.once("finish", socket.destroy);
      socket.end(
        `HTTP/1.1 ${code} ${http.STATUS_CODES[code]}\r
` + Object.keys(headers).map((h) => `${h}: ${headers[h]}`).join("\r\n") + "\r\n\r\n" + message
      );
    }
    function abortHandshakeOrEmitwsClientError(server, req, socket, code, message, headers) {
      if (server.listenerCount("wsClientError")) {
        const err = new Error(message);
        Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError);
        server.emit("wsClientError", err, socket, req);
      } else {
        abortHandshake(socket, code, message, headers);
      }
    }
  }
});

// server/server.js
var import_http = require("http");

// node_modules/ws/wrapper.mjs
var import_stream = __toESM(require_stream(), 1);
var import_extension = __toESM(require_extension(), 1);
var import_permessage_deflate = __toESM(require_permessage_deflate(), 1);
var import_receiver = __toESM(require_receiver(), 1);
var import_sender = __toESM(require_sender(), 1);
var import_subprotocol = __toESM(require_subprotocol(), 1);
var import_websocket = __toESM(require_websocket(), 1);
var import_websocket_server = __toESM(require_websocket_server(), 1);

// node_modules/@bufbuild/protobuf/dist/esm/is-message.js
function isMessage(arg, schema) {
  const isMessage3 = arg !== null && typeof arg == "object" && "$typeName" in arg && typeof arg.$typeName == "string";
  if (!isMessage3) {
    return false;
  }
  if (schema === void 0) {
    return true;
  }
  return schema.typeName === arg.$typeName;
}

// node_modules/@bufbuild/protobuf/dist/esm/descriptors.js
var ScalarType;
(function(ScalarType3) {
  ScalarType3[ScalarType3["DOUBLE"] = 1] = "DOUBLE";
  ScalarType3[ScalarType3["FLOAT"] = 2] = "FLOAT";
  ScalarType3[ScalarType3["INT64"] = 3] = "INT64";
  ScalarType3[ScalarType3["UINT64"] = 4] = "UINT64";
  ScalarType3[ScalarType3["INT32"] = 5] = "INT32";
  ScalarType3[ScalarType3["FIXED64"] = 6] = "FIXED64";
  ScalarType3[ScalarType3["FIXED32"] = 7] = "FIXED32";
  ScalarType3[ScalarType3["BOOL"] = 8] = "BOOL";
  ScalarType3[ScalarType3["STRING"] = 9] = "STRING";
  ScalarType3[ScalarType3["BYTES"] = 12] = "BYTES";
  ScalarType3[ScalarType3["UINT32"] = 13] = "UINT32";
  ScalarType3[ScalarType3["SFIXED32"] = 15] = "SFIXED32";
  ScalarType3[ScalarType3["SFIXED64"] = 16] = "SFIXED64";
  ScalarType3[ScalarType3["SINT32"] = 17] = "SINT32";
  ScalarType3[ScalarType3["SINT64"] = 18] = "SINT64";
})(ScalarType || (ScalarType = {}));

// node_modules/@bufbuild/protobuf/dist/esm/wire/varint.js
function varint64read() {
  let lowBits = 0;
  let highBits = 0;
  for (let shift = 0; shift < 28; shift += 7) {
    let b = this.buf[this.pos++];
    lowBits |= (b & 127) << shift;
    if ((b & 128) == 0) {
      this.assertBounds();
      return [lowBits, highBits];
    }
  }
  let middleByte = this.buf[this.pos++];
  lowBits |= (middleByte & 15) << 28;
  highBits = (middleByte & 112) >> 4;
  if ((middleByte & 128) == 0) {
    this.assertBounds();
    return [lowBits, highBits];
  }
  for (let shift = 3; shift <= 31; shift += 7) {
    let b = this.buf[this.pos++];
    highBits |= (b & 127) << shift;
    if ((b & 128) == 0) {
      this.assertBounds();
      return [lowBits, highBits];
    }
  }
  throw new Error("invalid varint");
}
function varint64write(lo, hi, bytes) {
  for (let i = 0; i < 28; i = i + 7) {
    const shift = lo >>> i;
    const hasNext = !(shift >>> 7 == 0 && hi == 0);
    const byte = (hasNext ? shift | 128 : shift) & 255;
    bytes.push(byte);
    if (!hasNext) {
      return;
    }
  }
  const splitBits = lo >>> 28 & 15 | (hi & 7) << 4;
  const hasMoreBits = !(hi >> 3 == 0);
  bytes.push((hasMoreBits ? splitBits | 128 : splitBits) & 255);
  if (!hasMoreBits) {
    return;
  }
  for (let i = 3; i < 31; i = i + 7) {
    const shift = hi >>> i;
    const hasNext = !(shift >>> 7 == 0);
    const byte = (hasNext ? shift | 128 : shift) & 255;
    bytes.push(byte);
    if (!hasNext) {
      return;
    }
  }
  bytes.push(hi >>> 31 & 1);
}
var TWO_PWR_32_DBL = 4294967296;
function int64FromString(dec) {
  const minus = dec[0] === "-";
  if (minus) {
    dec = dec.slice(1);
  }
  const base = 1e6;
  let lowBits = 0;
  let highBits = 0;
  function add1e6digit(begin, end) {
    const digit1e6 = Number(dec.slice(begin, end));
    highBits *= base;
    lowBits = lowBits * base + digit1e6;
    if (lowBits >= TWO_PWR_32_DBL) {
      highBits = highBits + (lowBits / TWO_PWR_32_DBL | 0);
      lowBits = lowBits % TWO_PWR_32_DBL;
    }
  }
  add1e6digit(-24, -18);
  add1e6digit(-18, -12);
  add1e6digit(-12, -6);
  add1e6digit(-6);
  return minus ? negate(lowBits, highBits) : newBits(lowBits, highBits);
}
function int64ToString(lo, hi) {
  let bits = newBits(lo, hi);
  const negative = bits.hi & 2147483648;
  if (negative) {
    bits = negate(bits.lo, bits.hi);
  }
  const result = uInt64ToString(bits.lo, bits.hi);
  return negative ? "-" + result : result;
}
function uInt64ToString(lo, hi) {
  ({ lo, hi } = toUnsigned(lo, hi));
  if (hi <= 2097151) {
    return String(TWO_PWR_32_DBL * hi + lo);
  }
  const low = lo & 16777215;
  const mid = (lo >>> 24 | hi << 8) & 16777215;
  const high = hi >> 16 & 65535;
  let digitA = low + mid * 6777216 + high * 6710656;
  let digitB = mid + high * 8147497;
  let digitC = high * 2;
  const base = 1e7;
  if (digitA >= base) {
    digitB += Math.floor(digitA / base);
    digitA %= base;
  }
  if (digitB >= base) {
    digitC += Math.floor(digitB / base);
    digitB %= base;
  }
  return digitC.toString() + decimalFrom1e7WithLeadingZeros(digitB) + decimalFrom1e7WithLeadingZeros(digitA);
}
function toUnsigned(lo, hi) {
  return { lo: lo >>> 0, hi: hi >>> 0 };
}
function newBits(lo, hi) {
  return { lo: lo | 0, hi: hi | 0 };
}
function negate(lowBits, highBits) {
  highBits = ~highBits;
  if (lowBits) {
    lowBits = ~lowBits + 1;
  } else {
    highBits += 1;
  }
  return newBits(lowBits, highBits);
}
var decimalFrom1e7WithLeadingZeros = (digit1e7) => {
  const partial = String(digit1e7);
  return "0000000".slice(partial.length) + partial;
};
function varint32write(value, bytes) {
  if (value >= 0) {
    while (value > 127) {
      bytes.push(value & 127 | 128);
      value = value >>> 7;
    }
    bytes.push(value);
  } else {
    for (let i = 0; i < 9; i++) {
      bytes.push(value & 127 | 128);
      value = value >> 7;
    }
    bytes.push(1);
  }
}
function varint32read() {
  let b = this.buf[this.pos++];
  let result = b & 127;
  if ((b & 128) == 0) {
    this.assertBounds();
    return result;
  }
  b = this.buf[this.pos++];
  result |= (b & 127) << 7;
  if ((b & 128) == 0) {
    this.assertBounds();
    return result;
  }
  b = this.buf[this.pos++];
  result |= (b & 127) << 14;
  if ((b & 128) == 0) {
    this.assertBounds();
    return result;
  }
  b = this.buf[this.pos++];
  result |= (b & 127) << 21;
  if ((b & 128) == 0) {
    this.assertBounds();
    return result;
  }
  b = this.buf[this.pos++];
  result |= (b & 15) << 28;
  for (let readBytes = 5; (b & 128) !== 0 && readBytes < 10; readBytes++)
    b = this.buf[this.pos++];
  if ((b & 128) != 0)
    throw new Error("invalid varint");
  this.assertBounds();
  return result >>> 0;
}

// node_modules/@bufbuild/protobuf/dist/esm/proto-int64.js
var protoInt64 = /* @__PURE__ */ makeInt64Support();
function makeInt64Support() {
  const dv = new DataView(new ArrayBuffer(8));
  const ok = typeof BigInt === "function" && typeof dv.getBigInt64 === "function" && typeof dv.getBigUint64 === "function" && typeof dv.setBigInt64 === "function" && typeof dv.setBigUint64 === "function" && (!!globalThis.Deno || typeof process != "object" || typeof process.env != "object" || process.env.BUF_BIGINT_DISABLE !== "1");
  if (ok) {
    const MIN = BigInt("-9223372036854775808");
    const MAX = BigInt("9223372036854775807");
    const UMIN = BigInt("0");
    const UMAX = BigInt("18446744073709551615");
    return {
      zero: BigInt(0),
      supported: true,
      parse(value) {
        const bi = typeof value == "bigint" ? value : BigInt(value);
        if (bi > MAX || bi < MIN) {
          throw new Error(`invalid int64: ${value}`);
        }
        return bi;
      },
      uParse(value) {
        const bi = typeof value == "bigint" ? value : BigInt(value);
        if (bi > UMAX || bi < UMIN) {
          throw new Error(`invalid uint64: ${value}`);
        }
        return bi;
      },
      enc(value) {
        dv.setBigInt64(0, this.parse(value), true);
        return {
          lo: dv.getInt32(0, true),
          hi: dv.getInt32(4, true)
        };
      },
      uEnc(value) {
        dv.setBigInt64(0, this.uParse(value), true);
        return {
          lo: dv.getInt32(0, true),
          hi: dv.getInt32(4, true)
        };
      },
      dec(lo, hi) {
        dv.setInt32(0, lo, true);
        dv.setInt32(4, hi, true);
        return dv.getBigInt64(0, true);
      },
      uDec(lo, hi) {
        dv.setInt32(0, lo, true);
        dv.setInt32(4, hi, true);
        return dv.getBigUint64(0, true);
      }
    };
  }
  return {
    zero: "0",
    supported: false,
    parse(value) {
      if (typeof value != "string") {
        value = value.toString();
      }
      assertInt64String(value);
      return value;
    },
    uParse(value) {
      if (typeof value != "string") {
        value = value.toString();
      }
      assertUInt64String(value);
      return value;
    },
    enc(value) {
      if (typeof value != "string") {
        value = value.toString();
      }
      assertInt64String(value);
      return int64FromString(value);
    },
    uEnc(value) {
      if (typeof value != "string") {
        value = value.toString();
      }
      assertUInt64String(value);
      return int64FromString(value);
    },
    dec(lo, hi) {
      return int64ToString(lo, hi);
    },
    uDec(lo, hi) {
      return uInt64ToString(lo, hi);
    }
  };
}
function assertInt64String(value) {
  if (!/^-?[0-9]+$/.test(value)) {
    throw new Error("invalid int64: " + value);
  }
}
function assertUInt64String(value) {
  if (!/^[0-9]+$/.test(value)) {
    throw new Error("invalid uint64: " + value);
  }
}

// node_modules/@bufbuild/protobuf/dist/esm/reflect/scalar.js
function scalarZeroValue(type, longAsString) {
  switch (type) {
    case ScalarType.STRING:
      return "";
    case ScalarType.BOOL:
      return false;
    case ScalarType.DOUBLE:
    case ScalarType.FLOAT:
      return 0;
    case ScalarType.INT64:
    case ScalarType.UINT64:
    case ScalarType.SFIXED64:
    case ScalarType.FIXED64:
    case ScalarType.SINT64:
      return longAsString ? "0" : protoInt64.zero;
    case ScalarType.BYTES:
      return new Uint8Array(0);
    default:
      return 0;
  }
}
function isScalarZeroValue(type, value) {
  switch (type) {
    case ScalarType.BOOL:
      return value === false;
    case ScalarType.STRING:
      return value === "";
    case ScalarType.BYTES:
      return value instanceof Uint8Array && !value.byteLength;
    default:
      return value == 0;
  }
}

// node_modules/@bufbuild/protobuf/dist/esm/reflect/unsafe.js
var IMPLICIT = 2;
var unsafeLocal = Symbol.for("reflect unsafe local");
function unsafeOneofCase(target, oneof) {
  const c = target[oneof.localName].case;
  if (c === void 0) {
    return c;
  }
  return oneof.fields.find((f) => f.localName === c);
}
function unsafeIsSet(target, field) {
  const name = field.localName;
  if (field.oneof) {
    return target[field.oneof.localName].case === name;
  }
  if (field.presence != IMPLICIT) {
    return target[name] !== void 0 && Object.prototype.hasOwnProperty.call(target, name);
  }
  switch (field.fieldKind) {
    case "list":
      return target[name].length > 0;
    case "map":
      return Object.keys(target[name]).length > 0;
    case "scalar":
      return !isScalarZeroValue(field.scalar, target[name]);
    case "enum":
      return target[name] !== field.enum.values[0].number;
  }
  throw new Error("message field with implicit presence");
}
function unsafeGet(target, field) {
  if (field.oneof) {
    const oneof = target[field.oneof.localName];
    if (oneof.case === field.localName) {
      return oneof.value;
    }
    return void 0;
  }
  return target[field.localName];
}
function unsafeSet(target, field, value) {
  if (field.oneof) {
    target[field.oneof.localName] = {
      case: field.localName,
      value
    };
  } else {
    target[field.localName] = value;
  }
}
function unsafeClear(target, field) {
  const name = field.localName;
  if (field.oneof) {
    const oneofLocalName = field.oneof.localName;
    if (target[oneofLocalName].case === name) {
      target[oneofLocalName] = { case: void 0 };
    }
  } else if (field.presence != IMPLICIT) {
    delete target[name];
  } else {
    switch (field.fieldKind) {
      case "map":
        target[name] = {};
        break;
      case "list":
        target[name] = [];
        break;
      case "enum":
        target[name] = field.enum.values[0].number;
        break;
      case "scalar":
        target[name] = scalarZeroValue(field.scalar, field.longAsString);
        break;
    }
  }
}

// node_modules/@bufbuild/protobuf/dist/esm/reflect/guard.js
function isObject(arg) {
  return arg !== null && typeof arg == "object" && !Array.isArray(arg);
}
function isReflectList(arg, field) {
  var _a, _b, _c, _d;
  if (isObject(arg) && unsafeLocal in arg && "add" in arg && "field" in arg && typeof arg.field == "function") {
    if (field !== void 0) {
      const a = field;
      const b = arg.field();
      return a.listKind == b.listKind && a.scalar === b.scalar && ((_a = a.message) === null || _a === void 0 ? void 0 : _a.typeName) === ((_b = b.message) === null || _b === void 0 ? void 0 : _b.typeName) && ((_c = a.enum) === null || _c === void 0 ? void 0 : _c.typeName) === ((_d = b.enum) === null || _d === void 0 ? void 0 : _d.typeName);
    }
    return true;
  }
  return false;
}
function isReflectMap(arg, field) {
  var _a, _b, _c, _d;
  if (isObject(arg) && unsafeLocal in arg && "has" in arg && "field" in arg && typeof arg.field == "function") {
    if (field !== void 0) {
      const a = field, b = arg.field();
      return a.mapKey === b.mapKey && a.mapKind == b.mapKind && a.scalar === b.scalar && ((_a = a.message) === null || _a === void 0 ? void 0 : _a.typeName) === ((_b = b.message) === null || _b === void 0 ? void 0 : _b.typeName) && ((_c = a.enum) === null || _c === void 0 ? void 0 : _c.typeName) === ((_d = b.enum) === null || _d === void 0 ? void 0 : _d.typeName);
    }
    return true;
  }
  return false;
}
function isReflectMessage(arg, messageDesc2) {
  return isObject(arg) && unsafeLocal in arg && "desc" in arg && isObject(arg.desc) && arg.desc.kind === "message" && (messageDesc2 === void 0 || arg.desc.typeName == messageDesc2.typeName);
}

// node_modules/@bufbuild/protobuf/dist/esm/wkt/wrappers.js
function isWrapper(arg) {
  return isWrapperTypeName(arg.$typeName);
}
function isWrapperDesc(messageDesc2) {
  const f = messageDesc2.fields[0];
  return isWrapperTypeName(messageDesc2.typeName) && f !== void 0 && f.fieldKind == "scalar" && f.name == "value" && f.number == 1;
}
function isWrapperTypeName(name) {
  return name.startsWith("google.protobuf.") && [
    "DoubleValue",
    "FloatValue",
    "Int64Value",
    "UInt64Value",
    "Int32Value",
    "UInt32Value",
    "BoolValue",
    "StringValue",
    "BytesValue"
  ].includes(name.substring(16));
}

// node_modules/@bufbuild/protobuf/dist/esm/create.js
var EDITION_PROTO3 = 999;
var EDITION_PROTO2 = 998;
var IMPLICIT2 = 2;
function create(schema, init) {
  if (isMessage(init, schema)) {
    return init;
  }
  const message = createZeroMessage(schema);
  if (init !== void 0) {
    initMessage(schema, message, init);
  }
  return message;
}
function initMessage(messageDesc2, message, init) {
  for (const member of messageDesc2.members) {
    let value = init[member.localName];
    if (value == null) {
      continue;
    }
    let field;
    if (member.kind == "oneof") {
      const oneofField = unsafeOneofCase(init, member);
      if (!oneofField) {
        continue;
      }
      field = oneofField;
      value = unsafeGet(init, oneofField);
    } else {
      field = member;
    }
    switch (field.fieldKind) {
      case "message":
        value = toMessage(field, value);
        break;
      case "scalar":
        value = initScalar(field, value);
        break;
      case "list":
        value = initList(field, value);
        break;
      case "map":
        value = initMap(field, value);
        break;
    }
    unsafeSet(message, field, value);
  }
  return message;
}
function initScalar(field, value) {
  if (field.scalar == ScalarType.BYTES) {
    return toU8Arr(value);
  }
  return value;
}
function initMap(field, value) {
  if (isObject(value)) {
    if (field.scalar == ScalarType.BYTES) {
      return convertObjectValues(value, toU8Arr);
    }
    if (field.mapKind == "message") {
      return convertObjectValues(value, (val) => toMessage(field, val));
    }
  }
  return value;
}
function initList(field, value) {
  if (Array.isArray(value)) {
    if (field.scalar == ScalarType.BYTES) {
      return value.map(toU8Arr);
    }
    if (field.listKind == "message") {
      return value.map((item) => toMessage(field, item));
    }
  }
  return value;
}
function toMessage(field, value) {
  if (field.fieldKind == "message" && !field.oneof && isWrapperDesc(field.message)) {
    return initScalar(field.message.fields[0], value);
  }
  if (isObject(value)) {
    if (field.message.typeName == "google.protobuf.Struct" && field.parent.typeName !== "google.protobuf.Value") {
      return value;
    }
    if (!isMessage(value, field.message)) {
      return create(field.message, value);
    }
  }
  return value;
}
function toU8Arr(value) {
  return Array.isArray(value) ? new Uint8Array(value) : value;
}
function convertObjectValues(obj, fn) {
  const ret = {};
  for (const entry of Object.entries(obj)) {
    ret[entry[0]] = fn(entry[1]);
  }
  return ret;
}
var tokenZeroMessageField = Symbol();
var messagePrototypes = /* @__PURE__ */ new WeakMap();
function createZeroMessage(desc) {
  let msg;
  if (!needsPrototypeChain(desc)) {
    msg = {
      $typeName: desc.typeName
    };
    for (const member of desc.members) {
      if (member.kind == "oneof" || member.presence == IMPLICIT2) {
        msg[member.localName] = createZeroField(member);
      }
    }
  } else {
    const cached = messagePrototypes.get(desc);
    let prototype;
    let members;
    if (cached) {
      ({ prototype, members } = cached);
    } else {
      prototype = {};
      members = /* @__PURE__ */ new Set();
      for (const member of desc.members) {
        if (member.kind == "oneof") {
          continue;
        }
        if (member.fieldKind != "scalar" && member.fieldKind != "enum") {
          continue;
        }
        if (member.presence == IMPLICIT2) {
          continue;
        }
        members.add(member);
        prototype[member.localName] = createZeroField(member);
      }
      messagePrototypes.set(desc, { prototype, members });
    }
    msg = Object.create(prototype);
    msg.$typeName = desc.typeName;
    for (const member of desc.members) {
      if (members.has(member)) {
        continue;
      }
      if (member.kind == "field") {
        if (member.fieldKind == "message") {
          continue;
        }
        if (member.fieldKind == "scalar" || member.fieldKind == "enum") {
          if (member.presence != IMPLICIT2) {
            continue;
          }
        }
      }
      msg[member.localName] = createZeroField(member);
    }
  }
  return msg;
}
function needsPrototypeChain(desc) {
  switch (desc.file.edition) {
    case EDITION_PROTO3:
      return false;
    case EDITION_PROTO2:
      return true;
    default:
      return desc.fields.some((f) => f.presence != IMPLICIT2 && f.fieldKind != "message" && !f.oneof);
  }
}
function createZeroField(field) {
  if (field.kind == "oneof") {
    return { case: void 0 };
  }
  if (field.fieldKind == "list") {
    return [];
  }
  if (field.fieldKind == "map") {
    return {};
  }
  if (field.fieldKind == "message") {
    return tokenZeroMessageField;
  }
  const defaultValue = field.getDefaultValue();
  if (defaultValue !== void 0) {
    return field.fieldKind == "scalar" && field.longAsString ? defaultValue.toString() : defaultValue;
  }
  return field.fieldKind == "scalar" ? scalarZeroValue(field.scalar, field.longAsString) : field.enum.values[0].number;
}

// node_modules/@bufbuild/protobuf/dist/esm/reflect/error.js
var FieldError = class extends Error {
  constructor(fieldOrOneof, message, name = "FieldValueInvalidError") {
    super(message);
    this.name = name;
    this.field = () => fieldOrOneof;
  }
};

// node_modules/@bufbuild/protobuf/dist/esm/wire/text-encoding.js
var symbol = Symbol.for("@bufbuild/protobuf/text-encoding");
function getTextEncoding() {
  if (globalThis[symbol] == void 0) {
    const te = new globalThis.TextEncoder();
    const td = new globalThis.TextDecoder();
    let tdStrict;
    globalThis[symbol] = {
      encodeUtf8(text) {
        return te.encode(text);
      },
      decodeUtf8(bytes, strict) {
        if (strict) {
          if (tdStrict === void 0) {
            tdStrict = new globalThis.TextDecoder("utf-8", { fatal: true });
          }
          return tdStrict.decode(bytes);
        }
        return td.decode(bytes);
      },
      checkUtf8(text) {
        try {
          encodeURIComponent(text);
          return true;
        } catch (_) {
          return false;
        }
      }
    };
  }
  return globalThis[symbol];
}

// node_modules/@bufbuild/protobuf/dist/esm/wire/binary-encoding.js
var WireType;
(function(WireType3) {
  WireType3[WireType3["Varint"] = 0] = "Varint";
  WireType3[WireType3["Bit64"] = 1] = "Bit64";
  WireType3[WireType3["LengthDelimited"] = 2] = "LengthDelimited";
  WireType3[WireType3["StartGroup"] = 3] = "StartGroup";
  WireType3[WireType3["EndGroup"] = 4] = "EndGroup";
  WireType3[WireType3["Bit32"] = 5] = "Bit32";
})(WireType || (WireType = {}));
var FLOAT32_MAX = 34028234663852886e22;
var FLOAT32_MIN = -34028234663852886e22;
var UINT32_MAX = 4294967295;
var INT32_MAX = 2147483647;
var INT32_MIN = -2147483648;
var BinaryWriter = class {
  constructor(encodeUtf8 = getTextEncoding().encodeUtf8) {
    this.encodeUtf8 = encodeUtf8;
    this.stack = [];
    this.chunks = [];
    this.buf = [];
  }
  /**
   * Return all bytes written and reset this writer.
   */
  finish() {
    if (this.buf.length) {
      this.chunks.push(new Uint8Array(this.buf));
      this.buf = [];
    }
    let len = 0;
    for (let i = 0; i < this.chunks.length; i++)
      len += this.chunks[i].length;
    let bytes = new Uint8Array(len);
    let offset = 0;
    for (let i = 0; i < this.chunks.length; i++) {
      bytes.set(this.chunks[i], offset);
      offset += this.chunks[i].length;
    }
    this.chunks = [];
    return bytes;
  }
  /**
   * Start a new fork for length-delimited data like a message
   * or a packed repeated field.
   *
   * Must be joined later with `join()`.
   */
  fork() {
    this.stack.push({ chunks: this.chunks, buf: this.buf });
    this.chunks = [];
    this.buf = [];
    return this;
  }
  /**
   * Join the last fork. Write its length and bytes, then
   * return to the previous state.
   */
  join() {
    let chunk = this.finish();
    let prev = this.stack.pop();
    if (!prev)
      throw new Error("invalid state, fork stack empty");
    this.chunks = prev.chunks;
    this.buf = prev.buf;
    this.uint32(chunk.byteLength);
    return this.raw(chunk);
  }
  /**
   * Writes a tag (field number and wire type).
   *
   * Equivalent to `uint32( (fieldNo << 3 | type) >>> 0 )`.
   *
   * Generated code should compute the tag ahead of time and call `uint32()`.
   */
  tag(fieldNo, type) {
    return this.uint32((fieldNo << 3 | type) >>> 0);
  }
  /**
   * Write a chunk of raw bytes.
   */
  raw(chunk) {
    if (this.buf.length) {
      this.chunks.push(new Uint8Array(this.buf));
      this.buf = [];
    }
    this.chunks.push(chunk);
    return this;
  }
  /**
   * Write a `uint32` value, an unsigned 32 bit varint.
   */
  uint32(value) {
    assertUInt32(value);
    while (value > 127) {
      this.buf.push(value & 127 | 128);
      value = value >>> 7;
    }
    this.buf.push(value);
    return this;
  }
  /**
   * Write a `int32` value, a signed 32 bit varint.
   */
  int32(value) {
    assertInt32(value);
    varint32write(value, this.buf);
    return this;
  }
  /**
   * Write a `bool` value, a varint.
   */
  bool(value) {
    this.buf.push(value ? 1 : 0);
    return this;
  }
  /**
   * Write a `bytes` value, length-delimited arbitrary data.
   */
  bytes(value) {
    this.uint32(value.byteLength);
    return this.raw(value);
  }
  /**
   * Write a `string` value, length-delimited data converted to UTF-8 text.
   */
  string(value) {
    let chunk = this.encodeUtf8(value);
    this.uint32(chunk.byteLength);
    return this.raw(chunk);
  }
  /**
   * Write a `float` value, 32-bit floating point number.
   */
  float(value) {
    assertFloat32(value);
    let chunk = new Uint8Array(4);
    new DataView(chunk.buffer).setFloat32(0, value, true);
    return this.raw(chunk);
  }
  /**
   * Write a `double` value, a 64-bit floating point number.
   */
  double(value) {
    let chunk = new Uint8Array(8);
    new DataView(chunk.buffer).setFloat64(0, value, true);
    return this.raw(chunk);
  }
  /**
   * Write a `fixed32` value, an unsigned, fixed-length 32-bit integer.
   */
  fixed32(value) {
    assertUInt32(value);
    let chunk = new Uint8Array(4);
    new DataView(chunk.buffer).setUint32(0, value, true);
    return this.raw(chunk);
  }
  /**
   * Write a `sfixed32` value, a signed, fixed-length 32-bit integer.
   */
  sfixed32(value) {
    assertInt32(value);
    let chunk = new Uint8Array(4);
    new DataView(chunk.buffer).setInt32(0, value, true);
    return this.raw(chunk);
  }
  /**
   * Write a `sint32` value, a signed, zigzag-encoded 32-bit varint.
   */
  sint32(value) {
    assertInt32(value);
    value = (value << 1 ^ value >> 31) >>> 0;
    varint32write(value, this.buf);
    return this;
  }
  /**
   * Write a `sfixed64` value, a signed, fixed-length 64-bit integer.
   */
  sfixed64(value) {
    let chunk = new Uint8Array(8), view = new DataView(chunk.buffer), tc = protoInt64.enc(value);
    view.setInt32(0, tc.lo, true);
    view.setInt32(4, tc.hi, true);
    return this.raw(chunk);
  }
  /**
   * Write a `fixed64` value, an unsigned, fixed-length 64 bit integer.
   */
  fixed64(value) {
    let chunk = new Uint8Array(8), view = new DataView(chunk.buffer), tc = protoInt64.uEnc(value);
    view.setInt32(0, tc.lo, true);
    view.setInt32(4, tc.hi, true);
    return this.raw(chunk);
  }
  /**
   * Write a `int64` value, a signed 64-bit varint.
   */
  int64(value) {
    let tc = protoInt64.enc(value);
    varint64write(tc.lo, tc.hi, this.buf);
    return this;
  }
  /**
   * Write a `sint64` value, a signed, zig-zag-encoded 64-bit varint.
   */
  sint64(value) {
    const tc = protoInt64.enc(value), sign = tc.hi >> 31, lo = tc.lo << 1 ^ sign, hi = (tc.hi << 1 | tc.lo >>> 31) ^ sign;
    varint64write(lo, hi, this.buf);
    return this;
  }
  /**
   * Write a `uint64` value, an unsigned 64-bit varint.
   */
  uint64(value) {
    const tc = protoInt64.uEnc(value);
    varint64write(tc.lo, tc.hi, this.buf);
    return this;
  }
};
var BinaryReader = class {
  constructor(buf, decodeUtf8 = getTextEncoding().decodeUtf8) {
    this.decodeUtf8 = decodeUtf8;
    this.varint64 = varint64read;
    this.uint32 = varint32read;
    this.buf = buf;
    this.len = buf.length;
    this.pos = 0;
    this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }
  /**
   * Reads a tag - field number and wire type. Tags are uint32 varints; values
   * that do not fit in uint32 are rejected.
   */
  tag() {
    const start = this.pos;
    const tag = this.uint32();
    const bytesRead = this.pos - start;
    if (bytesRead > 5 || bytesRead == 5 && this.buf[this.pos - 1] > 15) {
      throw new Error("illegal tag: varint overflows uint32");
    }
    const fieldNo = tag >>> 3;
    const wireType = tag & 7;
    if (fieldNo <= 0 || wireType > 5) {
      throw new Error("illegal tag: field no " + fieldNo + " wire type " + wireType);
    }
    return [fieldNo, wireType];
  }
  /**
   * Skip one element and return the skipped data.
   *
   * When skipping StartGroup, provide the tags field number to check for
   * matching field number in the EndGroup tag.
   */
  skip(wireType, fieldNo) {
    let start = this.pos;
    switch (wireType) {
      case WireType.Varint:
        while (this.buf[this.pos++] & 128) {
        }
        break;
      case WireType.Bit64:
        this.pos += 4;
      case WireType.Bit32:
        this.pos += 4;
        break;
      case WireType.LengthDelimited:
        let len = this.uint32();
        this.pos += len;
        break;
      case WireType.StartGroup:
        for (; ; ) {
          const [fn, wt] = this.tag();
          if (wt === WireType.EndGroup) {
            if (fieldNo !== void 0 && fn !== fieldNo) {
              throw new Error("invalid end group tag");
            }
            break;
          }
          this.skip(wt, fn);
        }
        break;
      default:
        throw new Error("cant skip wire type " + wireType);
    }
    this.assertBounds();
    return this.buf.subarray(start, this.pos);
  }
  /**
   * Throws error if position in byte array is out of range.
   */
  assertBounds() {
    if (this.pos > this.len)
      throw new RangeError("premature EOF");
  }
  /**
   * Read a `int32` field, a signed 32 bit varint.
   */
  int32() {
    return this.uint32() | 0;
  }
  /**
   * Read a `sint32` field, a signed, zigzag-encoded 32-bit varint.
   */
  sint32() {
    let zze = this.uint32();
    return zze >>> 1 ^ -(zze & 1);
  }
  /**
   * Read a `int64` field, a signed 64-bit varint.
   */
  int64() {
    return protoInt64.dec(...this.varint64());
  }
  /**
   * Read a `uint64` field, an unsigned 64-bit varint.
   */
  uint64() {
    return protoInt64.uDec(...this.varint64());
  }
  /**
   * Read a `sint64` field, a signed, zig-zag-encoded 64-bit varint.
   */
  sint64() {
    let [lo, hi] = this.varint64();
    let s = -(lo & 1);
    lo = (lo >>> 1 | (hi & 1) << 31) ^ s;
    hi = hi >>> 1 ^ s;
    return protoInt64.dec(lo, hi);
  }
  /**
   * Read a `bool` field, a variant.
   */
  bool() {
    let [lo, hi] = this.varint64();
    return lo !== 0 || hi !== 0;
  }
  /**
   * Read a `fixed32` field, an unsigned, fixed-length 32-bit integer.
   */
  fixed32() {
    return this.view.getUint32((this.pos += 4) - 4, true);
  }
  /**
   * Read a `sfixed32` field, a signed, fixed-length 32-bit integer.
   */
  sfixed32() {
    return this.view.getInt32((this.pos += 4) - 4, true);
  }
  /**
   * Read a `fixed64` field, an unsigned, fixed-length 64 bit integer.
   */
  fixed64() {
    return protoInt64.uDec(this.sfixed32(), this.sfixed32());
  }
  /**
   * Read a `fixed64` field, a signed, fixed-length 64-bit integer.
   */
  sfixed64() {
    return protoInt64.dec(this.sfixed32(), this.sfixed32());
  }
  /**
   * Read a `float` field, 32-bit floating point number.
   */
  float() {
    return this.view.getFloat32((this.pos += 4) - 4, true);
  }
  /**
   * Read a `double` field, a 64-bit floating point number.
   */
  double() {
    return this.view.getFloat64((this.pos += 8) - 8, true);
  }
  /**
   * Read a `bytes` field, length-delimited arbitrary data.
   */
  bytes() {
    let len = this.uint32(), start = this.pos;
    this.pos += len;
    this.assertBounds();
    return this.buf.subarray(start, start + len);
  }
  /**
   * Read a `string` field, length-delimited data converted to UTF-8 text. If
   * `strict` is true, throw on invalid UTF-8 instead of substituting U+FFFD.
   */
  string(strict) {
    return this.decodeUtf8(this.bytes(), strict);
  }
};
function assertInt32(arg) {
  if (typeof arg == "string") {
    arg = Number(arg);
  } else if (typeof arg != "number") {
    throw new Error("invalid int32: " + typeof arg);
  }
  if (!Number.isInteger(arg) || arg > INT32_MAX || arg < INT32_MIN)
    throw new Error("invalid int32: " + arg);
}
function assertUInt32(arg) {
  if (typeof arg == "string") {
    arg = Number(arg);
  } else if (typeof arg != "number") {
    throw new Error("invalid uint32: " + typeof arg);
  }
  if (!Number.isInteger(arg) || arg > UINT32_MAX || arg < 0)
    throw new Error("invalid uint32: " + arg);
}
function assertFloat32(arg) {
  if (typeof arg == "string") {
    const o = arg;
    arg = Number(arg);
    if (Number.isNaN(arg) && o !== "NaN") {
      throw new Error("invalid float32: " + o);
    }
  } else if (typeof arg != "number") {
    throw new Error("invalid float32: " + typeof arg);
  }
  if (Number.isFinite(arg) && (arg > FLOAT32_MAX || arg < FLOAT32_MIN))
    throw new Error("invalid float32: " + arg);
}

// node_modules/@bufbuild/protobuf/dist/esm/reflect/reflect-check.js
function checkField(field, value) {
  const check = field.fieldKind == "list" ? isReflectList(value, field) : field.fieldKind == "map" ? isReflectMap(value, field) : checkSingular(field, value);
  if (check === true) {
    return void 0;
  }
  let reason;
  switch (field.fieldKind) {
    case "list":
      reason = `expected ${formatReflectList(field)}, got ${formatVal(value)}`;
      break;
    case "map":
      reason = `expected ${formatReflectMap(field)}, got ${formatVal(value)}`;
      break;
    default: {
      reason = reasonSingular(field, value, check);
    }
  }
  return new FieldError(field, reason);
}
function checkListItem(field, index, value) {
  const check = checkSingular(field, value);
  if (check !== true) {
    return new FieldError(field, `list item #${index + 1}: ${reasonSingular(field, value, check)}`);
  }
  return void 0;
}
function checkMapEntry(field, key, value) {
  const checkKey = checkScalarValue(key, field.mapKey);
  if (checkKey !== true) {
    return new FieldError(field, `invalid map key: ${reasonSingular({ scalar: field.mapKey }, key, checkKey)}`);
  }
  const checkVal = checkSingular(field, value);
  if (checkVal !== true) {
    return new FieldError(field, `map entry ${formatVal(key)}: ${reasonSingular(field, value, checkVal)}`);
  }
  return void 0;
}
function checkSingular(field, value) {
  if (field.scalar !== void 0) {
    return checkScalarValue(value, field.scalar);
  }
  if (field.enum !== void 0) {
    if (field.enum.open) {
      return Number.isInteger(value);
    }
    return field.enum.values.some((v) => v.number === value);
  }
  return isReflectMessage(value, field.message);
}
function checkScalarValue(value, scalar) {
  switch (scalar) {
    case ScalarType.DOUBLE:
      return typeof value == "number";
    case ScalarType.FLOAT:
      if (typeof value != "number") {
        return false;
      }
      if (Number.isNaN(value) || !Number.isFinite(value)) {
        return true;
      }
      if (value > FLOAT32_MAX || value < FLOAT32_MIN) {
        return `${value.toFixed()} out of range`;
      }
      return true;
    case ScalarType.INT32:
    case ScalarType.SFIXED32:
    case ScalarType.SINT32:
      if (typeof value !== "number" || !Number.isInteger(value)) {
        return false;
      }
      if (value > INT32_MAX || value < INT32_MIN) {
        return `${value.toFixed()} out of range`;
      }
      return true;
    case ScalarType.FIXED32:
    case ScalarType.UINT32:
      if (typeof value !== "number" || !Number.isInteger(value)) {
        return false;
      }
      if (value > UINT32_MAX || value < 0) {
        return `${value.toFixed()} out of range`;
      }
      return true;
    case ScalarType.BOOL:
      return typeof value == "boolean";
    case ScalarType.STRING:
      if (typeof value != "string") {
        return false;
      }
      return getTextEncoding().checkUtf8(value) || "invalid UTF8";
    case ScalarType.BYTES:
      return value instanceof Uint8Array;
    case ScalarType.INT64:
    case ScalarType.SFIXED64:
    case ScalarType.SINT64:
      if (typeof value == "bigint" || typeof value == "number" || typeof value == "string" && value.length > 0) {
        try {
          protoInt64.parse(value);
          return true;
        } catch (_) {
          return `${value} out of range`;
        }
      }
      return false;
    case ScalarType.FIXED64:
    case ScalarType.UINT64:
      if (typeof value == "bigint" || typeof value == "number" || typeof value == "string" && value.length > 0) {
        try {
          protoInt64.uParse(value);
          return true;
        } catch (_) {
          return `${value} out of range`;
        }
      }
      return false;
  }
}
function reasonSingular(field, val, details) {
  details = typeof details == "string" ? `: ${details}` : `, got ${formatVal(val)}`;
  if (field.scalar !== void 0) {
    return `expected ${scalarTypeDescription(field.scalar)}` + details;
  }
  if (field.enum !== void 0) {
    return `expected ${field.enum.toString()}` + details;
  }
  return `expected ${formatReflectMessage(field.message)}` + details;
}
function formatVal(val) {
  switch (typeof val) {
    case "object":
      if (val === null) {
        return "null";
      }
      if (val instanceof Uint8Array) {
        return `Uint8Array(${val.length})`;
      }
      if (Array.isArray(val)) {
        return `Array(${val.length})`;
      }
      if (isReflectList(val)) {
        return formatReflectList(val.field());
      }
      if (isReflectMap(val)) {
        return formatReflectMap(val.field());
      }
      if (isReflectMessage(val)) {
        return formatReflectMessage(val.desc);
      }
      if (isMessage(val)) {
        return `message ${val.$typeName}`;
      }
      return "object";
    case "string":
      return val.length > 30 ? "string" : `"${val.split('"').join('\\"')}"`;
    case "boolean":
      return String(val);
    case "number":
      return String(val);
    case "bigint":
      return String(val) + "n";
    default:
      return typeof val;
  }
}
function formatReflectMessage(desc) {
  return `ReflectMessage (${desc.typeName})`;
}
function formatReflectList(field) {
  switch (field.listKind) {
    case "message":
      return `ReflectList (${field.message.toString()})`;
    case "enum":
      return `ReflectList (${field.enum.toString()})`;
    case "scalar":
      return `ReflectList (${ScalarType[field.scalar]})`;
  }
}
function formatReflectMap(field) {
  switch (field.mapKind) {
    case "message":
      return `ReflectMap (${ScalarType[field.mapKey]}, ${field.message.toString()})`;
    case "enum":
      return `ReflectMap (${ScalarType[field.mapKey]}, ${field.enum.toString()})`;
    case "scalar":
      return `ReflectMap (${ScalarType[field.mapKey]}, ${ScalarType[field.scalar]})`;
  }
}
function scalarTypeDescription(scalar) {
  switch (scalar) {
    case ScalarType.STRING:
      return "string";
    case ScalarType.BOOL:
      return "boolean";
    case ScalarType.INT64:
    case ScalarType.SINT64:
    case ScalarType.SFIXED64:
      return "bigint (int64)";
    case ScalarType.UINT64:
    case ScalarType.FIXED64:
      return "bigint (uint64)";
    case ScalarType.BYTES:
      return "Uint8Array";
    case ScalarType.DOUBLE:
      return "number (float64)";
    case ScalarType.FLOAT:
      return "number (float32)";
    case ScalarType.FIXED32:
    case ScalarType.UINT32:
      return "number (uint32)";
    case ScalarType.INT32:
    case ScalarType.SFIXED32:
    case ScalarType.SINT32:
      return "number (int32)";
  }
}

// node_modules/@bufbuild/protobuf/dist/esm/reflect/reflect.js
function reflect(messageDesc2, message, check = true) {
  return new ReflectMessageImpl(messageDesc2, message, check);
}
var messageSortedFields = /* @__PURE__ */ new WeakMap();
var ReflectMessageImpl = class {
  get sortedFields() {
    const cached = messageSortedFields.get(this.desc);
    if (cached) {
      return cached;
    }
    const sortedFields = this.desc.fields.concat().sort((a, b) => a.number - b.number);
    messageSortedFields.set(this.desc, sortedFields);
    return sortedFields;
  }
  constructor(messageDesc2, message, check = true) {
    this.lists = /* @__PURE__ */ new Map();
    this.maps = /* @__PURE__ */ new Map();
    this.check = check;
    this.desc = messageDesc2;
    this.message = this[unsafeLocal] = message !== null && message !== void 0 ? message : create(messageDesc2);
    this.fields = messageDesc2.fields;
    this.oneofs = messageDesc2.oneofs;
    this.members = messageDesc2.members;
  }
  findNumber(number) {
    if (!this._fieldsByNumber) {
      this._fieldsByNumber = new Map(this.desc.fields.map((f) => [f.number, f]));
    }
    return this._fieldsByNumber.get(number);
  }
  oneofCase(oneof) {
    assertOwn(this.message, oneof);
    return unsafeOneofCase(this.message, oneof);
  }
  isSet(field) {
    assertOwn(this.message, field);
    return unsafeIsSet(this.message, field);
  }
  clear(field) {
    assertOwn(this.message, field);
    unsafeClear(this.message, field);
  }
  get(field) {
    assertOwn(this.message, field);
    const value = unsafeGet(this.message, field);
    switch (field.fieldKind) {
      case "list":
        let list = this.lists.get(field);
        if (!list || list[unsafeLocal] !== value) {
          this.lists.set(
            field,
            // biome-ignore lint/suspicious/noAssignInExpressions: no
            list = new ReflectListImpl(field, value, this.check)
          );
        }
        return list;
      case "map":
        let map = this.maps.get(field);
        if (!map || map[unsafeLocal] !== value) {
          this.maps.set(
            field,
            // biome-ignore lint/suspicious/noAssignInExpressions: no
            map = new ReflectMapImpl(field, value, this.check)
          );
        }
        return map;
      case "message":
        return messageToReflect(field, value, this.check);
      case "scalar":
        return value === void 0 ? scalarZeroValue(field.scalar, false) : longToReflect(field, value);
      case "enum":
        return value !== null && value !== void 0 ? value : field.enum.values[0].number;
    }
  }
  set(field, value) {
    assertOwn(this.message, field);
    if (this.check) {
      const err = checkField(field, value);
      if (err) {
        throw err;
      }
    }
    let local;
    if (field.fieldKind == "message") {
      local = messageToLocal(field, value);
    } else if (isReflectMap(value) || isReflectList(value)) {
      local = value[unsafeLocal];
    } else {
      local = longToLocal(field, value);
    }
    unsafeSet(this.message, field, local);
  }
  getUnknown() {
    return this.message.$unknown;
  }
  setUnknown(value) {
    this.message.$unknown = value;
  }
};
function assertOwn(owner, member) {
  if (member.parent.typeName !== owner.$typeName) {
    throw new FieldError(member, `cannot use ${member.toString()} with message ${owner.$typeName}`, "ForeignFieldError");
  }
}
var ReflectListImpl = class {
  field() {
    return this._field;
  }
  get size() {
    return this._arr.length;
  }
  constructor(field, unsafeInput, check) {
    this._field = field;
    this._arr = this[unsafeLocal] = unsafeInput;
    this.check = check;
  }
  get(index) {
    const item = this._arr[index];
    return item === void 0 ? void 0 : listItemToReflect(this._field, item, this.check);
  }
  set(index, item) {
    if (index < 0 || index >= this._arr.length) {
      throw new FieldError(this._field, `list item #${index + 1}: out of range`);
    }
    if (this.check) {
      const err = checkListItem(this._field, index, item);
      if (err) {
        throw err;
      }
    }
    this._arr[index] = listItemToLocal(this._field, item);
  }
  add(item) {
    if (this.check) {
      const err = checkListItem(this._field, this._arr.length, item);
      if (err) {
        throw err;
      }
    }
    this._arr.push(listItemToLocal(this._field, item));
    return void 0;
  }
  clear() {
    this._arr.splice(0, this._arr.length);
  }
  [Symbol.iterator]() {
    return this.values();
  }
  keys() {
    return this._arr.keys();
  }
  *values() {
    for (const item of this._arr) {
      yield listItemToReflect(this._field, item, this.check);
    }
  }
  *entries() {
    for (let i = 0; i < this._arr.length; i++) {
      yield [i, listItemToReflect(this._field, this._arr[i], this.check)];
    }
  }
};
var ReflectMapImpl = class {
  constructor(field, unsafeInput, check = true) {
    this.obj = this[unsafeLocal] = unsafeInput !== null && unsafeInput !== void 0 ? unsafeInput : {};
    this.check = check;
    this._field = field;
  }
  field() {
    return this._field;
  }
  set(key, value) {
    if (this.check) {
      const err = checkMapEntry(this._field, key, value);
      if (err) {
        throw err;
      }
    }
    this.obj[mapKeyToLocal(key)] = mapValueToLocal(this._field, value);
    return this;
  }
  delete(key) {
    const k = mapKeyToLocal(key);
    const has = Object.prototype.hasOwnProperty.call(this.obj, k);
    if (has) {
      delete this.obj[k];
    }
    return has;
  }
  clear() {
    for (const key of Object.keys(this.obj)) {
      delete this.obj[key];
    }
  }
  get(key) {
    let val = this.obj[mapKeyToLocal(key)];
    if (val !== void 0) {
      val = mapValueToReflect(this._field, val, this.check);
    }
    return val;
  }
  has(key) {
    return Object.prototype.hasOwnProperty.call(this.obj, mapKeyToLocal(key));
  }
  *keys() {
    for (const objKey of Object.keys(this.obj)) {
      yield mapKeyToReflect(objKey, this._field.mapKey);
    }
  }
  *entries() {
    for (const objEntry of Object.entries(this.obj)) {
      yield [
        mapKeyToReflect(objEntry[0], this._field.mapKey),
        mapValueToReflect(this._field, objEntry[1], this.check)
      ];
    }
  }
  [Symbol.iterator]() {
    return this.entries();
  }
  get size() {
    return Object.keys(this.obj).length;
  }
  *values() {
    for (const val of Object.values(this.obj)) {
      yield mapValueToReflect(this._field, val, this.check);
    }
  }
  forEach(callbackfn, thisArg) {
    for (const mapEntry of this.entries()) {
      callbackfn.call(thisArg, mapEntry[1], mapEntry[0], this);
    }
  }
};
function messageToLocal(field, value) {
  if (!isReflectMessage(value)) {
    return value;
  }
  if (isWrapper(value.message) && !field.oneof && field.fieldKind == "message") {
    return value.message.value;
  }
  if (value.desc.typeName == "google.protobuf.Struct" && field.parent.typeName != "google.protobuf.Value") {
    return wktStructToLocal(value.message);
  }
  return value.message;
}
function messageToReflect(field, value, check) {
  if (value !== void 0) {
    if (isWrapperDesc(field.message) && !field.oneof && field.fieldKind == "message") {
      value = {
        $typeName: field.message.typeName,
        value: longToReflect(field.message.fields[0], value)
      };
    } else if (field.message.typeName == "google.protobuf.Struct" && field.parent.typeName != "google.protobuf.Value" && isObject(value)) {
      value = wktStructToReflect(value);
    }
  }
  return new ReflectMessageImpl(field.message, value, check);
}
function listItemToLocal(field, value) {
  if (field.listKind == "message") {
    return messageToLocal(field, value);
  }
  return longToLocal(field, value);
}
function listItemToReflect(field, value, check) {
  if (field.listKind == "message") {
    return messageToReflect(field, value, check);
  }
  return longToReflect(field, value);
}
function mapValueToLocal(field, value) {
  if (field.mapKind == "message") {
    return messageToLocal(field, value);
  }
  return longToLocal(field, value);
}
function mapValueToReflect(field, value, check) {
  if (field.mapKind == "message") {
    return messageToReflect(field, value, check);
  }
  return value;
}
function mapKeyToLocal(key) {
  return typeof key == "string" || typeof key == "number" ? key : String(key);
}
function mapKeyToReflect(key, type) {
  switch (type) {
    case ScalarType.STRING:
      return key;
    case ScalarType.INT32:
    case ScalarType.FIXED32:
    case ScalarType.UINT32:
    case ScalarType.SFIXED32:
    case ScalarType.SINT32: {
      const n = Number.parseInt(key);
      if (Number.isFinite(n)) {
        return n;
      }
      break;
    }
    case ScalarType.BOOL:
      switch (key) {
        case "true":
          return true;
        case "false":
          return false;
      }
      break;
    case ScalarType.UINT64:
    case ScalarType.FIXED64:
      try {
        return protoInt64.uParse(key);
      } catch (_a) {
      }
      break;
    default:
      try {
        return protoInt64.parse(key);
      } catch (_b) {
      }
      break;
  }
  return key;
}
function longToReflect(field, value) {
  switch (field.scalar) {
    case ScalarType.INT64:
    case ScalarType.SFIXED64:
    case ScalarType.SINT64:
      if ("longAsString" in field && field.longAsString && typeof value == "string") {
        value = protoInt64.parse(value);
      }
      break;
    case ScalarType.FIXED64:
    case ScalarType.UINT64:
      if ("longAsString" in field && field.longAsString && typeof value == "string") {
        value = protoInt64.uParse(value);
      }
      break;
  }
  return value;
}
function longToLocal(field, value) {
  switch (field.scalar) {
    case ScalarType.INT64:
    case ScalarType.SFIXED64:
    case ScalarType.SINT64:
      if ("longAsString" in field && field.longAsString) {
        value = String(value);
      } else if (typeof value == "string" || typeof value == "number") {
        value = protoInt64.parse(value);
      }
      break;
    case ScalarType.FIXED64:
    case ScalarType.UINT64:
      if ("longAsString" in field && field.longAsString) {
        value = String(value);
      } else if (typeof value == "string" || typeof value == "number") {
        value = protoInt64.uParse(value);
      }
      break;
  }
  return value;
}
function wktStructToReflect(json) {
  const struct = {
    $typeName: "google.protobuf.Struct",
    fields: {}
  };
  if (isObject(json)) {
    for (const [k, v] of Object.entries(json)) {
      struct.fields[k] = wktValueToReflect(v);
    }
  }
  return struct;
}
function wktStructToLocal(val) {
  const json = {};
  for (const [k, v] of Object.entries(val.fields)) {
    json[k] = wktValueToLocal(v);
  }
  return json;
}
function wktValueToLocal(val) {
  switch (val.kind.case) {
    case "structValue":
      return wktStructToLocal(val.kind.value);
    case "listValue":
      return val.kind.value.values.map(wktValueToLocal);
    case "nullValue":
    case void 0:
      return null;
    default:
      return val.kind.value;
  }
}
function wktValueToReflect(json) {
  const value = {
    $typeName: "google.protobuf.Value",
    kind: { case: void 0 }
  };
  switch (typeof json) {
    case "number":
      value.kind = { case: "numberValue", value: json };
      break;
    case "string":
      value.kind = { case: "stringValue", value: json };
      break;
    case "boolean":
      value.kind = { case: "boolValue", value: json };
      break;
    case "object":
      if (json === null) {
        const nullValue = 0;
        value.kind = { case: "nullValue", value: nullValue };
      } else if (Array.isArray(json)) {
        const listValue = {
          $typeName: "google.protobuf.ListValue",
          values: []
        };
        if (Array.isArray(json)) {
          for (const e of json) {
            listValue.values.push(wktValueToReflect(e));
          }
        }
        value.kind = {
          case: "listValue",
          value: listValue
        };
      } else {
        value.kind = {
          case: "structValue",
          value: wktStructToReflect(json)
        };
      }
      break;
  }
  return value;
}

// node_modules/@bufbuild/protobuf/dist/esm/from-binary.js
var readDefaults = {
  readUnknownFields: true
};
function makeReadOptions(options) {
  return options ? Object.assign(Object.assign({}, readDefaults), options) : readDefaults;
}
function fromBinary(schema, bytes, options) {
  const msg = reflect(schema, void 0, false);
  readMessage(msg, new BinaryReader(bytes), makeReadOptions(options), false, bytes.byteLength);
  return msg.message;
}
function readMessage(message, reader, options, delimited, lengthOrDelimitedFieldNo) {
  var _a;
  const end = delimited ? reader.len : reader.pos + lengthOrDelimitedFieldNo;
  let fieldNo;
  let wireType;
  const unknownFields = (_a = message.getUnknown()) !== null && _a !== void 0 ? _a : [];
  while (reader.pos < end) {
    [fieldNo, wireType] = reader.tag();
    if (delimited && wireType == WireType.EndGroup) {
      break;
    }
    const field = message.findNumber(fieldNo);
    if (!field) {
      const data = reader.skip(wireType, fieldNo);
      if (options.readUnknownFields) {
        unknownFields.push({ no: fieldNo, wireType, data });
      }
      continue;
    }
    readField(message, reader, field, wireType, options);
  }
  if (delimited) {
    if (wireType != WireType.EndGroup || fieldNo !== lengthOrDelimitedFieldNo) {
      throw new Error("invalid end group tag");
    }
  }
  if (unknownFields.length > 0) {
    message.setUnknown(unknownFields);
  }
}
function readField(message, reader, field, wireType, options) {
  var _a;
  switch (field.fieldKind) {
    case "scalar":
      message.set(field, readScalar(reader, field.scalar, field.utf8Validation));
      break;
    case "enum":
      const val = readScalar(reader, ScalarType.INT32);
      if (field.enum.open) {
        message.set(field, val);
      } else {
        const ok = field.enum.values.some((v) => v.number === val);
        if (ok) {
          message.set(field, val);
        } else if (options.readUnknownFields) {
          const bytes = [];
          varint32write(val, bytes);
          const unknownFields = (_a = message.getUnknown()) !== null && _a !== void 0 ? _a : [];
          unknownFields.push({
            no: field.number,
            wireType,
            data: new Uint8Array(bytes)
          });
          message.setUnknown(unknownFields);
        }
      }
      break;
    case "message":
      message.set(field, readMessageField(reader, options, field, message.get(field)));
      break;
    case "list":
      readListField(reader, wireType, message.get(field), options);
      break;
    case "map":
      readMapEntry(reader, message.get(field), options);
      break;
  }
}
function readMapEntry(reader, map, options) {
  const field = map.field();
  let key;
  let val;
  const len = reader.uint32();
  const end = reader.pos + len;
  while (reader.pos < end) {
    const [fieldNo] = reader.tag();
    switch (fieldNo) {
      case 1:
        key = readScalar(reader, field.mapKey, field.utf8Validation);
        break;
      case 2:
        switch (field.mapKind) {
          case "scalar":
            val = readScalar(reader, field.scalar, field.utf8Validation);
            break;
          case "enum":
            val = reader.int32();
            break;
          case "message":
            val = readMessageField(reader, options, field);
            break;
        }
        break;
    }
  }
  if (key === void 0) {
    key = scalarZeroValue(field.mapKey, false);
  }
  if (val === void 0) {
    switch (field.mapKind) {
      case "scalar":
        val = scalarZeroValue(field.scalar, false);
        break;
      case "enum":
        val = field.enum.values[0].number;
        break;
      case "message":
        val = reflect(field.message, void 0, false);
        break;
    }
  }
  map.set(key, val);
}
function readListField(reader, wireType, list, options) {
  var _a;
  const field = list.field();
  if (field.listKind === "message") {
    list.add(readMessageField(reader, options, field));
    return;
  }
  const scalarType = (_a = field.scalar) !== null && _a !== void 0 ? _a : ScalarType.INT32;
  const packed = wireType == WireType.LengthDelimited && scalarType != ScalarType.STRING && scalarType != ScalarType.BYTES;
  if (!packed) {
    list.add(readScalar(reader, scalarType, field.utf8Validation));
    return;
  }
  const e = reader.uint32() + reader.pos;
  while (reader.pos < e) {
    list.add(readScalar(reader, scalarType, field.utf8Validation));
  }
}
function readMessageField(reader, options, field, mergeMessage) {
  const delimited = field.delimitedEncoding;
  const message = mergeMessage !== null && mergeMessage !== void 0 ? mergeMessage : reflect(field.message, void 0, false);
  readMessage(message, reader, options, delimited, delimited ? field.number : reader.uint32());
  return message;
}
function readScalar(reader, type, validateUtf8 = false) {
  switch (type) {
    case ScalarType.STRING:
      return reader.string(validateUtf8);
    case ScalarType.BOOL:
      return reader.bool();
    case ScalarType.DOUBLE:
      return reader.double();
    case ScalarType.FLOAT:
      return reader.float();
    case ScalarType.INT32:
      return reader.int32();
    case ScalarType.INT64:
      return reader.int64();
    case ScalarType.UINT64:
      return reader.uint64();
    case ScalarType.FIXED64:
      return reader.fixed64();
    case ScalarType.BYTES:
      return reader.bytes();
    case ScalarType.FIXED32:
      return reader.fixed32();
    case ScalarType.SFIXED32:
      return reader.sfixed32();
    case ScalarType.SFIXED64:
      return reader.sfixed64();
    case ScalarType.SINT64:
      return reader.sint64();
    case ScalarType.UINT32:
      return reader.uint32();
    case ScalarType.SINT32:
      return reader.sint32();
  }
}

// node_modules/@bufbuild/protobuf/dist/esm/to-binary.js
var LEGACY_REQUIRED = 3;
var writeDefaults = {
  writeUnknownFields: true
};
function makeWriteOptions(options) {
  return options ? Object.assign(Object.assign({}, writeDefaults), options) : writeDefaults;
}
function toBinary(schema, message, options) {
  return writeFields(new BinaryWriter(), makeWriteOptions(options), reflect(schema, message)).finish();
}
function writeFields(writer, opts, msg) {
  var _a;
  for (const f of msg.sortedFields) {
    if (!msg.isSet(f)) {
      if (f.presence == LEGACY_REQUIRED) {
        throw new Error(`cannot encode ${f} to binary: required field not set`);
      }
      continue;
    }
    writeField(writer, opts, msg, f);
  }
  if (opts.writeUnknownFields) {
    for (const { no, wireType, data } of (_a = msg.getUnknown()) !== null && _a !== void 0 ? _a : []) {
      writer.tag(no, wireType).raw(data);
    }
  }
  return writer;
}
function writeField(writer, opts, msg, field) {
  var _a;
  switch (field.fieldKind) {
    case "scalar":
    case "enum":
      writeScalar(writer, msg.desc.typeName, field.name, (_a = field.scalar) !== null && _a !== void 0 ? _a : ScalarType.INT32, field.number, msg.get(field));
      break;
    case "list":
      writeListField(writer, opts, field, msg.get(field));
      break;
    case "message":
      writeMessageField(writer, opts, field, msg.get(field));
      break;
    case "map":
      for (const [key, val] of msg.get(field)) {
        writeMapEntry(writer, opts, field, key, val);
      }
      break;
  }
}
function writeScalar(writer, msgName, fieldName, scalarType, fieldNo, value) {
  writeScalarValue(writer.tag(fieldNo, writeTypeOfScalar(scalarType)), msgName, fieldName, scalarType, value);
}
function writeMessageField(writer, opts, field, message) {
  if (field.delimitedEncoding) {
    writeFields(writer.tag(field.number, WireType.StartGroup), opts, message).tag(field.number, WireType.EndGroup);
  } else {
    writeFields(writer.tag(field.number, WireType.LengthDelimited).fork(), opts, message).join();
  }
}
function writeListField(writer, opts, field, list) {
  var _a;
  if (field.listKind == "message") {
    for (const item of list) {
      writeMessageField(writer, opts, field, item);
    }
    return;
  }
  const scalarType = (_a = field.scalar) !== null && _a !== void 0 ? _a : ScalarType.INT32;
  if (field.packed) {
    if (!list.size) {
      return;
    }
    writer.tag(field.number, WireType.LengthDelimited).fork();
    for (const item of list) {
      writeScalarValue(writer, field.parent.typeName, field.name, scalarType, item);
    }
    writer.join();
    return;
  }
  for (const item of list) {
    writeScalar(writer, field.parent.typeName, field.name, scalarType, field.number, item);
  }
}
function writeMapEntry(writer, opts, field, key, value) {
  var _a;
  writer.tag(field.number, WireType.LengthDelimited).fork();
  writeScalar(writer, field.parent.typeName, field.name, field.mapKey, 1, key);
  switch (field.mapKind) {
    case "scalar":
    case "enum":
      writeScalar(writer, field.parent.typeName, field.name, (_a = field.scalar) !== null && _a !== void 0 ? _a : ScalarType.INT32, 2, value);
      break;
    case "message":
      writeFields(writer.tag(2, WireType.LengthDelimited).fork(), opts, value).join();
      break;
  }
  writer.join();
}
function writeScalarValue(writer, msgName, fieldName, type, value) {
  try {
    switch (type) {
      case ScalarType.STRING:
        writer.string(value);
        break;
      case ScalarType.BOOL:
        writer.bool(value);
        break;
      case ScalarType.DOUBLE:
        writer.double(value);
        break;
      case ScalarType.FLOAT:
        writer.float(value);
        break;
      case ScalarType.INT32:
        writer.int32(value);
        break;
      case ScalarType.INT64:
        writer.int64(value);
        break;
      case ScalarType.UINT64:
        writer.uint64(value);
        break;
      case ScalarType.FIXED64:
        writer.fixed64(value);
        break;
      case ScalarType.BYTES:
        writer.bytes(value);
        break;
      case ScalarType.FIXED32:
        writer.fixed32(value);
        break;
      case ScalarType.SFIXED32:
        writer.sfixed32(value);
        break;
      case ScalarType.SFIXED64:
        writer.sfixed64(value);
        break;
      case ScalarType.SINT64:
        writer.sint64(value);
        break;
      case ScalarType.UINT32:
        writer.uint32(value);
        break;
      case ScalarType.SINT32:
        writer.sint32(value);
        break;
    }
  } catch (e) {
    if (e instanceof Error) {
      throw new Error(`cannot encode field ${msgName}.${fieldName} to binary: ${e.message}`);
    }
    throw e;
  }
}
function writeTypeOfScalar(type) {
  switch (type) {
    case ScalarType.BYTES:
    case ScalarType.STRING:
      return WireType.LengthDelimited;
    case ScalarType.DOUBLE:
    case ScalarType.FIXED64:
    case ScalarType.SFIXED64:
      return WireType.Bit64;
    case ScalarType.FIXED32:
    case ScalarType.SFIXED32:
    case ScalarType.FLOAT:
      return WireType.Bit32;
    default:
      return WireType.Varint;
  }
}

// node_modules/@meshtastic/core/dist/chunk-DbKvDyjX.js
var __create2 = Object.create;
var __defProp2 = Object.defineProperty;
var __getOwnPropDesc2 = Object.getOwnPropertyDescriptor;
var __getOwnPropNames2 = Object.getOwnPropertyNames;
var __getProtoOf2 = Object.getPrototypeOf;
var __hasOwnProp2 = Object.prototype.hasOwnProperty;
var __commonJS2 = (cb, mod) => function() {
  return mod || (0, cb[__getOwnPropNames2(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};
var __export = (all) => {
  let target = {};
  for (var name in all)
    __defProp2(target, name, {
      get: all[name],
      enumerable: true
    });
  return target;
};
var __copyProps2 = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function")
    for (var keys = __getOwnPropNames2(from), i = 0, n = keys.length, key; i < n; i++) {
      key = keys[i];
      if (!__hasOwnProp2.call(to, key) && key !== except)
        __defProp2(to, key, {
          get: ((k) => from[k]).bind(null, key),
          enumerable: !(desc = __getOwnPropDesc2(from, key)) || desc.enumerable
        });
    }
  return to;
};
var __toESM2 = (mod, isNodeMode, target) => (target = mod != null ? __create2(__getProtoOf2(mod)) : {}, __copyProps2(isNodeMode || !mod || !mod.__esModule ? __defProp2(target, "default", {
  value: mod,
  enumerable: true
}) : target, mod));

// node_modules/@meshtastic/core/dist/mod.js
var import_os = require("os");

// node_modules/crc/mjs/calculators/crc16ccitt.js
var TABLE = [
  0,
  4129,
  8258,
  12387,
  16516,
  20645,
  24774,
  28903,
  33032,
  37161,
  41290,
  45419,
  49548,
  53677,
  57806,
  61935,
  4657,
  528,
  12915,
  8786,
  21173,
  17044,
  29431,
  25302,
  37689,
  33560,
  45947,
  41818,
  54205,
  50076,
  62463,
  58334,
  9314,
  13379,
  1056,
  5121,
  25830,
  29895,
  17572,
  21637,
  42346,
  46411,
  34088,
  38153,
  58862,
  62927,
  50604,
  54669,
  13907,
  9842,
  5649,
  1584,
  30423,
  26358,
  22165,
  18100,
  46939,
  42874,
  38681,
  34616,
  63455,
  59390,
  55197,
  51132,
  18628,
  22757,
  26758,
  30887,
  2112,
  6241,
  10242,
  14371,
  51660,
  55789,
  59790,
  63919,
  35144,
  39273,
  43274,
  47403,
  23285,
  19156,
  31415,
  27286,
  6769,
  2640,
  14899,
  10770,
  56317,
  52188,
  64447,
  60318,
  39801,
  35672,
  47931,
  43802,
  27814,
  31879,
  19684,
  23749,
  11298,
  15363,
  3168,
  7233,
  60846,
  64911,
  52716,
  56781,
  44330,
  48395,
  36200,
  40265,
  32407,
  28342,
  24277,
  20212,
  15891,
  11826,
  7761,
  3696,
  65439,
  61374,
  57309,
  53244,
  48923,
  44858,
  40793,
  36728,
  37256,
  33193,
  45514,
  41451,
  53516,
  49453,
  61774,
  57711,
  4224,
  161,
  12482,
  8419,
  20484,
  16421,
  28742,
  24679,
  33721,
  37784,
  41979,
  46042,
  49981,
  54044,
  58239,
  62302,
  689,
  4752,
  8947,
  13010,
  16949,
  21012,
  25207,
  29270,
  46570,
  42443,
  38312,
  34185,
  62830,
  58703,
  54572,
  50445,
  13538,
  9411,
  5280,
  1153,
  29798,
  25671,
  21540,
  17413,
  42971,
  47098,
  34713,
  38840,
  59231,
  63358,
  50973,
  55100,
  9939,
  14066,
  1681,
  5808,
  26199,
  30326,
  17941,
  22068,
  55628,
  51565,
  63758,
  59695,
  39368,
  35305,
  47498,
  43435,
  22596,
  18533,
  30726,
  26663,
  6336,
  2273,
  14466,
  10403,
  52093,
  56156,
  60223,
  64286,
  35833,
  39896,
  43963,
  48026,
  19061,
  23124,
  27191,
  31254,
  2801,
  6864,
  10931,
  14994,
  64814,
  60687,
  56684,
  52557,
  48554,
  44427,
  40424,
  36297,
  31782,
  27655,
  23652,
  19525,
  15522,
  11395,
  7392,
  3265,
  61215,
  65342,
  53085,
  57212,
  44955,
  49082,
  36825,
  40952,
  28183,
  32310,
  20053,
  24180,
  11923,
  16050,
  3793,
  7920
];
if (typeof Int32Array !== "undefined") {
  TABLE = new Int32Array(TABLE);
}
var crc16ccitt = (current, previous) => {
  let crc = typeof previous !== "undefined" ? ~~previous : 65535;
  for (let index = 0; index < current.length; index++) {
    crc = (TABLE[(crc >> 8 ^ current[index]) & 255] ^ crc << 8) & 65535;
  }
  return crc;
};
var crc16ccitt_default = crc16ccitt;

// node_modules/@meshtastic/core/dist/mod.js
var __defProp3 = Object.defineProperty;
var __export2 = (all) => {
  let target = {};
  for (var name in all)
    __defProp3(target, name, {
      get: all[name],
      enumerable: true
    });
  return target;
};
function protoCamelCase(snakeCase) {
  let capNext = false;
  const b = [];
  for (let i = 0; i < snakeCase.length; i++) {
    let c = snakeCase.charAt(i);
    switch (c) {
      case "_":
        capNext = true;
        break;
      case "0":
      case "1":
      case "2":
      case "3":
      case "4":
      case "5":
      case "6":
      case "7":
      case "8":
      case "9":
        b.push(c);
        capNext = false;
        break;
      default:
        if (capNext) {
          capNext = false;
          c = c.toUpperCase();
        }
        b.push(c);
        break;
    }
  }
  return b.join("");
}
var reservedObjectProperties = /* @__PURE__ */ new Set([
  "constructor",
  "toString",
  "toJSON",
  "valueOf"
]);
function safeObjectProperty(name) {
  return reservedObjectProperties.has(name) ? name + "$" : name;
}
function varint64read2() {
  let lowBits = 0;
  let highBits = 0;
  for (let shift = 0; shift < 28; shift += 7) {
    let b = this.buf[this.pos++];
    lowBits |= (b & 127) << shift;
    if ((b & 128) == 0) {
      this.assertBounds();
      return [lowBits, highBits];
    }
  }
  let middleByte = this.buf[this.pos++];
  lowBits |= (middleByte & 15) << 28;
  highBits = (middleByte & 112) >> 4;
  if ((middleByte & 128) == 0) {
    this.assertBounds();
    return [lowBits, highBits];
  }
  for (let shift = 3; shift <= 31; shift += 7) {
    let b = this.buf[this.pos++];
    highBits |= (b & 127) << shift;
    if ((b & 128) == 0) {
      this.assertBounds();
      return [lowBits, highBits];
    }
  }
  throw new Error("invalid varint");
}
function varint64write2(lo, hi, bytes) {
  for (let i = 0; i < 28; i = i + 7) {
    const shift = lo >>> i;
    const hasNext = !(shift >>> 7 == 0 && hi == 0);
    const byte = (hasNext ? shift | 128 : shift) & 255;
    bytes.push(byte);
    if (!hasNext)
      return;
  }
  const splitBits = lo >>> 28 & 15 | (hi & 7) << 4;
  const hasMoreBits = !(hi >> 3 == 0);
  bytes.push((hasMoreBits ? splitBits | 128 : splitBits) & 255);
  if (!hasMoreBits)
    return;
  for (let i = 3; i < 31; i = i + 7) {
    const shift = hi >>> i;
    const hasNext = !(shift >>> 7 == 0);
    const byte = (hasNext ? shift | 128 : shift) & 255;
    bytes.push(byte);
    if (!hasNext)
      return;
  }
  bytes.push(hi >>> 31 & 1);
}
var TWO_PWR_32_DBL2 = 4294967296;
function int64FromString2(dec) {
  const minus = dec[0] === "-";
  if (minus)
    dec = dec.slice(1);
  const base = 1e6;
  let lowBits = 0;
  let highBits = 0;
  function add1e6digit(begin, end) {
    const digit1e6 = Number(dec.slice(begin, end));
    highBits *= base;
    lowBits = lowBits * base + digit1e6;
    if (lowBits >= TWO_PWR_32_DBL2) {
      highBits = highBits + (lowBits / TWO_PWR_32_DBL2 | 0);
      lowBits = lowBits % TWO_PWR_32_DBL2;
    }
  }
  add1e6digit(-24, -18);
  add1e6digit(-18, -12);
  add1e6digit(-12, -6);
  add1e6digit(-6);
  return minus ? negate2(lowBits, highBits) : newBits2(lowBits, highBits);
}
function int64ToString2(lo, hi) {
  let bits = newBits2(lo, hi);
  const negative = bits.hi & 2147483648;
  if (negative)
    bits = negate2(bits.lo, bits.hi);
  const result = uInt64ToString2(bits.lo, bits.hi);
  return negative ? "-" + result : result;
}
function uInt64ToString2(lo, hi) {
  ({ lo, hi } = toUnsigned2(lo, hi));
  if (hi <= 2097151)
    return String(TWO_PWR_32_DBL2 * hi + lo);
  const low = lo & 16777215;
  const mid = (lo >>> 24 | hi << 8) & 16777215;
  const high = hi >> 16 & 65535;
  let digitA = low + mid * 6777216 + high * 6710656;
  let digitB = mid + high * 8147497;
  let digitC = high * 2;
  const base = 1e7;
  if (digitA >= base) {
    digitB += Math.floor(digitA / base);
    digitA %= base;
  }
  if (digitB >= base) {
    digitC += Math.floor(digitB / base);
    digitB %= base;
  }
  return digitC.toString() + decimalFrom1e7WithLeadingZeros2(digitB) + decimalFrom1e7WithLeadingZeros2(digitA);
}
function toUnsigned2(lo, hi) {
  return {
    lo: lo >>> 0,
    hi: hi >>> 0
  };
}
function newBits2(lo, hi) {
  return {
    lo: lo | 0,
    hi: hi | 0
  };
}
function negate2(lowBits, highBits) {
  highBits = ~highBits;
  if (lowBits)
    lowBits = ~lowBits + 1;
  else
    highBits += 1;
  return newBits2(lowBits, highBits);
}
var decimalFrom1e7WithLeadingZeros2 = (digit1e7) => {
  const partial = String(digit1e7);
  return "0000000".slice(partial.length) + partial;
};
function varint32write2(value, bytes) {
  if (value >= 0) {
    while (value > 127) {
      bytes.push(value & 127 | 128);
      value = value >>> 7;
    }
    bytes.push(value);
  } else {
    for (let i = 0; i < 9; i++) {
      bytes.push(value & 127 | 128);
      value = value >> 7;
    }
    bytes.push(1);
  }
}
function varint32read2() {
  let b = this.buf[this.pos++];
  let result = b & 127;
  if ((b & 128) == 0) {
    this.assertBounds();
    return result;
  }
  b = this.buf[this.pos++];
  result |= (b & 127) << 7;
  if ((b & 128) == 0) {
    this.assertBounds();
    return result;
  }
  b = this.buf[this.pos++];
  result |= (b & 127) << 14;
  if ((b & 128) == 0) {
    this.assertBounds();
    return result;
  }
  b = this.buf[this.pos++];
  result |= (b & 127) << 21;
  if ((b & 128) == 0) {
    this.assertBounds();
    return result;
  }
  b = this.buf[this.pos++];
  result |= (b & 15) << 28;
  for (let readBytes = 5; (b & 128) !== 0 && readBytes < 10; readBytes++)
    b = this.buf[this.pos++];
  if ((b & 128) != 0)
    throw new Error("invalid varint");
  this.assertBounds();
  return result >>> 0;
}
var protoInt642 = /* @__PURE__ */ makeInt64Support2();
function makeInt64Support2() {
  const dv = /* @__PURE__ */ new DataView(/* @__PURE__ */ new ArrayBuffer(8));
  if (typeof BigInt === "function" && typeof dv.getBigInt64 === "function" && typeof dv.getBigUint64 === "function" && typeof dv.setBigInt64 === "function" && typeof dv.setBigUint64 === "function" && (typeof process != "object" || typeof process.env != "object" || process.env.BUF_BIGINT_DISABLE !== "1")) {
    const MIN = BigInt("-9223372036854775808");
    const MAX = BigInt("9223372036854775807");
    const UMIN = BigInt("0");
    const UMAX = BigInt("18446744073709551615");
    return {
      zero: BigInt(0),
      supported: true,
      parse(value) {
        const bi = typeof value == "bigint" ? value : BigInt(value);
        if (bi > MAX || bi < MIN)
          throw new Error(`invalid int64: ${value}`);
        return bi;
      },
      uParse(value) {
        const bi = typeof value == "bigint" ? value : BigInt(value);
        if (bi > UMAX || bi < UMIN)
          throw new Error(`invalid uint64: ${value}`);
        return bi;
      },
      enc(value) {
        dv.setBigInt64(0, this.parse(value), true);
        return {
          lo: dv.getInt32(0, true),
          hi: dv.getInt32(4, true)
        };
      },
      uEnc(value) {
        dv.setBigInt64(0, this.uParse(value), true);
        return {
          lo: dv.getInt32(0, true),
          hi: dv.getInt32(4, true)
        };
      },
      dec(lo, hi) {
        dv.setInt32(0, lo, true);
        dv.setInt32(4, hi, true);
        return dv.getBigInt64(0, true);
      },
      uDec(lo, hi) {
        dv.setInt32(0, lo, true);
        dv.setInt32(4, hi, true);
        return dv.getBigUint64(0, true);
      }
    };
  }
  return {
    zero: "0",
    supported: false,
    parse(value) {
      if (typeof value != "string")
        value = value.toString();
      assertInt64String2(value);
      return value;
    },
    uParse(value) {
      if (typeof value != "string")
        value = value.toString();
      assertUInt64String2(value);
      return value;
    },
    enc(value) {
      if (typeof value != "string")
        value = value.toString();
      assertInt64String2(value);
      return int64FromString2(value);
    },
    uEnc(value) {
      if (typeof value != "string")
        value = value.toString();
      assertUInt64String2(value);
      return int64FromString2(value);
    },
    dec(lo, hi) {
      return int64ToString2(lo, hi);
    },
    uDec(lo, hi) {
      return uInt64ToString2(lo, hi);
    }
  };
}
function assertInt64String2(value) {
  if (!/^-?[0-9]+$/.test(value))
    throw new Error("invalid int64: " + value);
}
function assertUInt64String2(value) {
  if (!/^[0-9]+$/.test(value))
    throw new Error("invalid uint64: " + value);
}
var ScalarType2;
(function(ScalarType$1) {
  ScalarType$1[ScalarType$1["DOUBLE"] = 1] = "DOUBLE";
  ScalarType$1[ScalarType$1["FLOAT"] = 2] = "FLOAT";
  ScalarType$1[ScalarType$1["INT64"] = 3] = "INT64";
  ScalarType$1[ScalarType$1["UINT64"] = 4] = "UINT64";
  ScalarType$1[ScalarType$1["INT32"] = 5] = "INT32";
  ScalarType$1[ScalarType$1["FIXED64"] = 6] = "FIXED64";
  ScalarType$1[ScalarType$1["FIXED32"] = 7] = "FIXED32";
  ScalarType$1[ScalarType$1["BOOL"] = 8] = "BOOL";
  ScalarType$1[ScalarType$1["STRING"] = 9] = "STRING";
  ScalarType$1[ScalarType$1["BYTES"] = 12] = "BYTES";
  ScalarType$1[ScalarType$1["UINT32"] = 13] = "UINT32";
  ScalarType$1[ScalarType$1["SFIXED32"] = 15] = "SFIXED32";
  ScalarType$1[ScalarType$1["SFIXED64"] = 16] = "SFIXED64";
  ScalarType$1[ScalarType$1["SINT32"] = 17] = "SINT32";
  ScalarType$1[ScalarType$1["SINT64"] = 18] = "SINT64";
})(ScalarType2 || (ScalarType2 = {}));
function scalarZeroValue2(type, longAsString) {
  switch (type) {
    case ScalarType2.STRING:
      return "";
    case ScalarType2.BOOL:
      return false;
    case ScalarType2.DOUBLE:
    case ScalarType2.FLOAT:
      return 0;
    case ScalarType2.INT64:
    case ScalarType2.UINT64:
    case ScalarType2.SFIXED64:
    case ScalarType2.FIXED64:
    case ScalarType2.SINT64:
      return longAsString ? "0" : protoInt642.zero;
    case ScalarType2.BYTES:
      return new Uint8Array(0);
    default:
      return 0;
  }
}
function isScalarZeroValue2(type, value) {
  switch (type) {
    case ScalarType2.BOOL:
      return value === false;
    case ScalarType2.STRING:
      return value === "";
    case ScalarType2.BYTES:
      return value instanceof Uint8Array && !value.byteLength;
    default:
      return value == 0;
  }
}
var IMPLICIT$2 = 2;
var unsafeLocal2 = Symbol.for("reflect unsafe local");
function unsafeOneofCase2(target, oneof) {
  const c = target[oneof.localName].case;
  if (c === void 0)
    return c;
  return oneof.fields.find((f) => f.localName === c);
}
function unsafeIsSet2(target, field) {
  const name = field.localName;
  if (field.oneof)
    return target[field.oneof.localName].case === name;
  if (field.presence != IMPLICIT$2)
    return target[name] !== void 0 && Object.prototype.hasOwnProperty.call(target, name);
  switch (field.fieldKind) {
    case "list":
      return target[name].length > 0;
    case "map":
      return Object.keys(target[name]).length > 0;
    case "scalar":
      return !isScalarZeroValue2(field.scalar, target[name]);
    case "enum":
      return target[name] !== field.enum.values[0].number;
  }
  throw new Error("message field with implicit presence");
}
function unsafeIsSetExplicit(target, localName) {
  return Object.prototype.hasOwnProperty.call(target, localName) && target[localName] !== void 0;
}
function unsafeGet2(target, field) {
  if (field.oneof) {
    const oneof = target[field.oneof.localName];
    if (oneof.case === field.localName)
      return oneof.value;
    return;
  }
  return target[field.localName];
}
function unsafeSet2(target, field, value) {
  if (field.oneof)
    target[field.oneof.localName] = {
      case: field.localName,
      value
    };
  else
    target[field.localName] = value;
}
function unsafeClear2(target, field) {
  const name = field.localName;
  if (field.oneof) {
    const oneofLocalName = field.oneof.localName;
    if (target[oneofLocalName].case === name)
      target[oneofLocalName] = { case: void 0 };
  } else if (field.presence != IMPLICIT$2)
    delete target[name];
  else
    switch (field.fieldKind) {
      case "map":
        target[name] = {};
        break;
      case "list":
        target[name] = [];
        break;
      case "enum":
        target[name] = field.enum.values[0].number;
        break;
      case "scalar":
        target[name] = scalarZeroValue2(field.scalar, field.longAsString);
        break;
    }
}
function restoreJsonNames(message) {
  for (const f of message.field)
    if (!unsafeIsSetExplicit(f, "jsonName"))
      f.jsonName = protoCamelCase(f.name);
  message.nestedType.forEach(restoreJsonNames);
}
function parseTextFormatEnumValue(descEnum, value) {
  const enumValue = descEnum.values.find((v) => v.name === value);
  if (!enumValue)
    throw new Error(`cannot parse ${descEnum} default value: ${value}`);
  return enumValue.number;
}
function parseTextFormatScalarValue(type, value) {
  switch (type) {
    case ScalarType2.STRING:
      return value;
    case ScalarType2.BYTES: {
      const u = unescapeBytesDefaultValue(value);
      if (u === false)
        throw new Error(`cannot parse ${ScalarType2[type]} default value: ${value}`);
      return u;
    }
    case ScalarType2.INT64:
    case ScalarType2.SFIXED64:
    case ScalarType2.SINT64:
      return protoInt642.parse(value);
    case ScalarType2.UINT64:
    case ScalarType2.FIXED64:
      return protoInt642.uParse(value);
    case ScalarType2.DOUBLE:
    case ScalarType2.FLOAT:
      switch (value) {
        case "inf":
          return Number.POSITIVE_INFINITY;
        case "-inf":
          return Number.NEGATIVE_INFINITY;
        case "nan":
          return NaN;
        default:
          return parseFloat(value);
      }
    case ScalarType2.BOOL:
      return value === "true";
    case ScalarType2.INT32:
    case ScalarType2.UINT32:
    case ScalarType2.SINT32:
    case ScalarType2.FIXED32:
    case ScalarType2.SFIXED32:
      return parseInt(value, 10);
  }
}
function unescapeBytesDefaultValue(str) {
  const b = [];
  const input = {
    tail: str,
    c: "",
    next() {
      if (this.tail.length == 0)
        return false;
      this.c = this.tail[0];
      this.tail = this.tail.substring(1);
      return true;
    },
    take(n) {
      if (this.tail.length >= n) {
        const r = this.tail.substring(0, n);
        this.tail = this.tail.substring(n);
        return r;
      }
      return false;
    }
  };
  while (input.next())
    switch (input.c) {
      case "\\":
        if (input.next())
          switch (input.c) {
            case "\\":
              b.push(input.c.charCodeAt(0));
              break;
            case "b":
              b.push(8);
              break;
            case "f":
              b.push(12);
              break;
            case "n":
              b.push(10);
              break;
            case "r":
              b.push(13);
              break;
            case "t":
              b.push(9);
              break;
            case "v":
              b.push(11);
              break;
            case "0":
            case "1":
            case "2":
            case "3":
            case "4":
            case "5":
            case "6":
            case "7": {
              const s = input.c;
              const t = input.take(2);
              if (t === false)
                return false;
              const n = parseInt(s + t, 8);
              if (Number.isNaN(n))
                return false;
              b.push(n);
              break;
            }
            case "x": {
              const s = input.c;
              const t = input.take(2);
              if (t === false)
                return false;
              const n = parseInt(s + t, 16);
              if (Number.isNaN(n))
                return false;
              b.push(n);
              break;
            }
            case "u": {
              const s = input.c;
              const t = input.take(4);
              if (t === false)
                return false;
              const n = parseInt(s + t, 16);
              if (Number.isNaN(n))
                return false;
              const chunk = new Uint8Array(4);
              new DataView(chunk.buffer).setInt32(0, n, true);
              b.push(chunk[0], chunk[1], chunk[2], chunk[3]);
              break;
            }
            case "U": {
              const s = input.c;
              const t = input.take(8);
              if (t === false)
                return false;
              const tc = protoInt642.uEnc(s + t);
              const chunk = new Uint8Array(8);
              const view = new DataView(chunk.buffer);
              view.setInt32(0, tc.lo, true);
              view.setInt32(4, tc.hi, true);
              b.push(chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7]);
              break;
            }
          }
        break;
      default:
        b.push(input.c.charCodeAt(0));
    }
  return new Uint8Array(b);
}
function* nestedTypes(desc) {
  switch (desc.kind) {
    case "file":
      for (const message of desc.messages) {
        yield message;
        yield* nestedTypes(message);
      }
      yield* desc.enums;
      yield* desc.services;
      yield* desc.extensions;
      break;
    case "message":
      for (const message of desc.nestedMessages) {
        yield message;
        yield* nestedTypes(message);
      }
      yield* desc.nestedEnums;
      yield* desc.nestedExtensions;
      break;
  }
}
function createFileRegistry(...args) {
  const registry = createBaseRegistry();
  if (!args.length)
    return registry;
  if ("$typeName" in args[0] && args[0].$typeName == "google.protobuf.FileDescriptorSet") {
    for (const file of args[0].file)
      addFile(file, registry);
    return registry;
  }
  if ("$typeName" in args[0]) {
    let recurseDeps = function(file) {
      const deps = [];
      for (const protoFileName of file.dependency) {
        if (registry.getFile(protoFileName) != void 0)
          continue;
        if (seen.has(protoFileName))
          continue;
        const dep = resolve(protoFileName);
        if (!dep)
          throw new Error(`Unable to resolve ${protoFileName}, imported by ${file.name}`);
        if ("kind" in dep)
          registry.addFile(dep, false, true);
        else {
          seen.add(dep.name);
          deps.push(dep);
        }
      }
      return deps.concat(...deps.map(recurseDeps));
    };
    const input = args[0];
    const resolve = args[1];
    const seen = /* @__PURE__ */ new Set();
    for (const file of [input, ...recurseDeps(input)].reverse())
      addFile(file, registry);
  } else
    for (const fileReg of args)
      for (const file of fileReg.files)
        registry.addFile(file);
  return registry;
}
function createBaseRegistry() {
  const types$1 = /* @__PURE__ */ new Map();
  const extendees = /* @__PURE__ */ new Map();
  const files = /* @__PURE__ */ new Map();
  return {
    kind: "registry",
    types: types$1,
    extendees,
    [Symbol.iterator]() {
      return types$1.values();
    },
    get files() {
      return files.values();
    },
    addFile(file, skipTypes, withDeps) {
      files.set(file.proto.name, file);
      if (!skipTypes)
        for (const type of nestedTypes(file))
          this.add(type);
      if (withDeps)
        for (const f of file.dependencies)
          this.addFile(f, skipTypes, withDeps);
    },
    add(desc) {
      if (desc.kind == "extension") {
        let numberToExt = extendees.get(desc.extendee.typeName);
        if (!numberToExt)
          extendees.set(desc.extendee.typeName, numberToExt = /* @__PURE__ */ new Map());
        numberToExt.set(desc.number, desc);
      }
      types$1.set(desc.typeName, desc);
    },
    get(typeName) {
      return types$1.get(typeName);
    },
    getFile(fileName) {
      return files.get(fileName);
    },
    getMessage(typeName) {
      const t = types$1.get(typeName);
      return (t === null || t === void 0 ? void 0 : t.kind) == "message" ? t : void 0;
    },
    getEnum(typeName) {
      const t = types$1.get(typeName);
      return (t === null || t === void 0 ? void 0 : t.kind) == "enum" ? t : void 0;
    },
    getExtension(typeName) {
      const t = types$1.get(typeName);
      return (t === null || t === void 0 ? void 0 : t.kind) == "extension" ? t : void 0;
    },
    getExtensionFor(extendee, no) {
      var _a;
      return (_a = extendees.get(extendee.typeName)) === null || _a === void 0 ? void 0 : _a.get(no);
    },
    getService(typeName) {
      const t = types$1.get(typeName);
      return (t === null || t === void 0 ? void 0 : t.kind) == "service" ? t : void 0;
    }
  };
}
var EDITION_PROTO2$1 = 998;
var EDITION_PROTO3$1 = 999;
var TYPE_STRING = 9;
var TYPE_GROUP = 10;
var TYPE_MESSAGE = 11;
var TYPE_BYTES = 12;
var TYPE_ENUM = 14;
var LABEL_REPEATED = 3;
var LABEL_REQUIRED = 2;
var JS_STRING = 1;
var IDEMPOTENCY_UNKNOWN = 0;
var EXPLICIT = 1;
var IMPLICIT$1 = 2;
var LEGACY_REQUIRED$1 = 3;
var PACKED = 1;
var DELIMITED = 2;
var OPEN = 1;
var featureDefaults = {
  998: {
    fieldPresence: 1,
    enumType: 2,
    repeatedFieldEncoding: 2,
    utf8Validation: 3,
    messageEncoding: 1,
    jsonFormat: 2,
    enforceNamingStyle: 2,
    defaultSymbolVisibility: 1
  },
  999: {
    fieldPresence: 2,
    enumType: 1,
    repeatedFieldEncoding: 1,
    utf8Validation: 2,
    messageEncoding: 1,
    jsonFormat: 1,
    enforceNamingStyle: 2,
    defaultSymbolVisibility: 1
  },
  1e3: {
    fieldPresence: 1,
    enumType: 1,
    repeatedFieldEncoding: 1,
    utf8Validation: 2,
    messageEncoding: 1,
    jsonFormat: 1,
    enforceNamingStyle: 2,
    defaultSymbolVisibility: 1
  },
  1001: {
    fieldPresence: 1,
    enumType: 1,
    repeatedFieldEncoding: 1,
    utf8Validation: 2,
    messageEncoding: 1,
    jsonFormat: 1,
    enforceNamingStyle: 1,
    defaultSymbolVisibility: 2
  }
};
function addFile(proto, reg) {
  var _a, _b;
  const file = {
    kind: "file",
    proto,
    deprecated: (_b = (_a = proto.options) === null || _a === void 0 ? void 0 : _a.deprecated) !== null && _b !== void 0 ? _b : false,
    edition: getFileEdition(proto),
    name: proto.name.replace(/\.proto$/, ""),
    dependencies: findFileDependencies(proto, reg),
    enums: [],
    messages: [],
    extensions: [],
    services: [],
    toString() {
      return `file ${proto.name}`;
    }
  };
  const mapEntriesStore = /* @__PURE__ */ new Map();
  const mapEntries = {
    get(typeName) {
      return mapEntriesStore.get(typeName);
    },
    add(desc) {
      var _a$1;
      assert(((_a$1 = desc.proto.options) === null || _a$1 === void 0 ? void 0 : _a$1.mapEntry) === true);
      mapEntriesStore.set(desc.typeName, desc);
    }
  };
  for (const enumProto of proto.enumType)
    addEnum(enumProto, file, void 0, reg);
  for (const messageProto of proto.messageType)
    addMessage(messageProto, file, void 0, reg, mapEntries);
  for (const serviceProto of proto.service)
    addService(serviceProto, file, reg);
  addExtensions(file, reg);
  for (const mapEntry of mapEntriesStore.values())
    addFields(mapEntry, reg, mapEntries);
  for (const message of file.messages) {
    addFields(message, reg, mapEntries);
    addExtensions(message, reg);
  }
  reg.addFile(file, true);
}
function addExtensions(desc, reg) {
  switch (desc.kind) {
    case "file":
      for (const proto of desc.proto.extension) {
        const ext = newField(proto, desc, reg);
        desc.extensions.push(ext);
        reg.add(ext);
      }
      break;
    case "message":
      for (const proto of desc.proto.extension) {
        const ext = newField(proto, desc, reg);
        desc.nestedExtensions.push(ext);
        reg.add(ext);
      }
      for (const message of desc.nestedMessages)
        addExtensions(message, reg);
      break;
  }
}
function addFields(message, reg, mapEntries) {
  const allOneofs = message.proto.oneofDecl.map((proto) => newOneof(proto, message));
  const oneofsSeen = /* @__PURE__ */ new Set();
  for (const proto of message.proto.field) {
    const oneof = findOneof(proto, allOneofs);
    const field = newField(proto, message, reg, oneof, mapEntries);
    message.fields.push(field);
    message.field[field.localName] = field;
    if (oneof === void 0)
      message.members.push(field);
    else {
      oneof.fields.push(field);
      if (!oneofsSeen.has(oneof)) {
        oneofsSeen.add(oneof);
        message.members.push(oneof);
      }
    }
  }
  for (const oneof of allOneofs.filter((o) => oneofsSeen.has(o)))
    message.oneofs.push(oneof);
  for (const child of message.nestedMessages)
    addFields(child, reg, mapEntries);
}
function addEnum(proto, file, parent, reg) {
  var _a, _b, _c, _d, _e;
  const sharedPrefix = findEnumSharedPrefix(proto.name, proto.value);
  const desc = {
    kind: "enum",
    proto,
    deprecated: (_b = (_a = proto.options) === null || _a === void 0 ? void 0 : _a.deprecated) !== null && _b !== void 0 ? _b : false,
    file,
    parent,
    open: true,
    name: proto.name,
    typeName: makeTypeName(proto, parent, file),
    value: {},
    values: [],
    sharedPrefix,
    toString() {
      return `enum ${this.typeName}`;
    }
  };
  desc.open = isEnumOpen(desc);
  reg.add(desc);
  for (const p of proto.value) {
    const name = p.name;
    desc.values.push(desc.value[p.number] = {
      kind: "enum_value",
      proto: p,
      deprecated: (_d = (_c = p.options) === null || _c === void 0 ? void 0 : _c.deprecated) !== null && _d !== void 0 ? _d : false,
      parent: desc,
      name,
      localName: safeObjectProperty(sharedPrefix == void 0 ? name : name.substring(sharedPrefix.length)),
      number: p.number,
      toString() {
        return `enum value ${desc.typeName}.${name}`;
      }
    });
  }
  ((_e = parent === null || parent === void 0 ? void 0 : parent.nestedEnums) !== null && _e !== void 0 ? _e : file.enums).push(desc);
}
function addMessage(proto, file, parent, reg, mapEntries) {
  var _a, _b, _c, _d;
  const desc = {
    kind: "message",
    proto,
    deprecated: (_b = (_a = proto.options) === null || _a === void 0 ? void 0 : _a.deprecated) !== null && _b !== void 0 ? _b : false,
    file,
    parent,
    name: proto.name,
    typeName: makeTypeName(proto, parent, file),
    fields: [],
    field: {},
    oneofs: [],
    members: [],
    nestedEnums: [],
    nestedMessages: [],
    nestedExtensions: [],
    toString() {
      return `message ${this.typeName}`;
    }
  };
  if (((_c = proto.options) === null || _c === void 0 ? void 0 : _c.mapEntry) === true)
    mapEntries.add(desc);
  else {
    ((_d = parent === null || parent === void 0 ? void 0 : parent.nestedMessages) !== null && _d !== void 0 ? _d : file.messages).push(desc);
    reg.add(desc);
  }
  for (const enumProto of proto.enumType)
    addEnum(enumProto, file, desc, reg);
  for (const messageProto of proto.nestedType)
    addMessage(messageProto, file, desc, reg, mapEntries);
}
function addService(proto, file, reg) {
  var _a, _b;
  const desc = {
    kind: "service",
    proto,
    deprecated: (_b = (_a = proto.options) === null || _a === void 0 ? void 0 : _a.deprecated) !== null && _b !== void 0 ? _b : false,
    file,
    name: proto.name,
    typeName: makeTypeName(proto, void 0, file),
    methods: [],
    method: {},
    toString() {
      return `service ${this.typeName}`;
    }
  };
  file.services.push(desc);
  reg.add(desc);
  for (const methodProto of proto.method) {
    const method = newMethod(methodProto, desc, reg);
    desc.methods.push(method);
    desc.method[method.localName] = method;
  }
}
function newMethod(proto, parent, reg) {
  var _a, _b, _c, _d;
  let methodKind;
  if (proto.clientStreaming && proto.serverStreaming)
    methodKind = "bidi_streaming";
  else if (proto.clientStreaming)
    methodKind = "client_streaming";
  else if (proto.serverStreaming)
    methodKind = "server_streaming";
  else
    methodKind = "unary";
  const input = reg.getMessage(trimLeadingDot(proto.inputType));
  const output = reg.getMessage(trimLeadingDot(proto.outputType));
  assert(input, `invalid MethodDescriptorProto: input_type ${proto.inputType} not found`);
  assert(output, `invalid MethodDescriptorProto: output_type ${proto.inputType} not found`);
  const name = proto.name;
  return {
    kind: "rpc",
    proto,
    deprecated: (_b = (_a = proto.options) === null || _a === void 0 ? void 0 : _a.deprecated) !== null && _b !== void 0 ? _b : false,
    parent,
    name,
    localName: safeObjectProperty(name.length ? safeObjectProperty(name[0].toLowerCase() + name.substring(1)) : name),
    methodKind,
    input,
    output,
    idempotency: (_d = (_c = proto.options) === null || _c === void 0 ? void 0 : _c.idempotencyLevel) !== null && _d !== void 0 ? _d : IDEMPOTENCY_UNKNOWN,
    toString() {
      return `rpc ${parent.typeName}.${name}`;
    }
  };
}
function newOneof(proto, parent) {
  return {
    kind: "oneof",
    proto,
    deprecated: false,
    parent,
    fields: [],
    name: proto.name,
    localName: safeObjectProperty(protoCamelCase(proto.name)),
    toString() {
      return `oneof ${parent.typeName}.${this.name}`;
    }
  };
}
function newField(proto, parentOrFile, reg, oneof, mapEntries) {
  var _a, _b, _c;
  const isExtension = mapEntries === void 0;
  const field = {
    kind: "field",
    proto,
    deprecated: (_b = (_a = proto.options) === null || _a === void 0 ? void 0 : _a.deprecated) !== null && _b !== void 0 ? _b : false,
    name: proto.name,
    number: proto.number,
    scalar: void 0,
    message: void 0,
    enum: void 0,
    presence: getFieldPresence(proto, oneof, isExtension, parentOrFile),
    listKind: void 0,
    mapKind: void 0,
    mapKey: void 0,
    delimitedEncoding: void 0,
    packed: void 0,
    longAsString: false,
    getDefaultValue: void 0
  };
  if (isExtension) {
    const file = parentOrFile.kind == "file" ? parentOrFile : parentOrFile.file;
    const parent = parentOrFile.kind == "file" ? void 0 : parentOrFile;
    const typeName = makeTypeName(proto, parent, file);
    field.kind = "extension";
    field.file = file;
    field.parent = parent;
    field.oneof = void 0;
    field.typeName = typeName;
    field.jsonName = `[${typeName}]`;
    field.toString = () => `extension ${typeName}`;
    const extendee = reg.getMessage(trimLeadingDot(proto.extendee));
    assert(extendee, `invalid FieldDescriptorProto: extendee ${proto.extendee} not found`);
    field.extendee = extendee;
  } else {
    const parent = parentOrFile;
    assert(parent.kind == "message");
    field.parent = parent;
    field.oneof = oneof;
    field.localName = oneof ? protoCamelCase(proto.name) : safeObjectProperty(protoCamelCase(proto.name));
    field.jsonName = proto.jsonName;
    field.toString = () => `field ${parent.typeName}.${proto.name}`;
  }
  const label = proto.label;
  const type = proto.type;
  const jstype = (_c = proto.options) === null || _c === void 0 ? void 0 : _c.jstype;
  if (label === LABEL_REPEATED) {
    const mapEntry = type == TYPE_MESSAGE ? mapEntries === null || mapEntries === void 0 ? void 0 : mapEntries.get(trimLeadingDot(proto.typeName)) : void 0;
    if (mapEntry) {
      field.fieldKind = "map";
      const { key, value } = findMapEntryFields(mapEntry);
      field.mapKey = key.scalar;
      field.mapKind = value.fieldKind;
      field.message = value.message;
      field.delimitedEncoding = false;
      field.enum = value.enum;
      field.scalar = value.scalar;
      return field;
    }
    field.fieldKind = "list";
    switch (type) {
      case TYPE_MESSAGE:
      case TYPE_GROUP:
        field.listKind = "message";
        field.message = reg.getMessage(trimLeadingDot(proto.typeName));
        assert(field.message);
        field.delimitedEncoding = isDelimitedEncoding(proto, parentOrFile);
        break;
      case TYPE_ENUM:
        field.listKind = "enum";
        field.enum = reg.getEnum(trimLeadingDot(proto.typeName));
        assert(field.enum);
        break;
      default:
        field.listKind = "scalar";
        field.scalar = type;
        field.longAsString = jstype == JS_STRING;
        break;
    }
    field.packed = isPackedField(proto, parentOrFile);
    return field;
  }
  switch (type) {
    case TYPE_MESSAGE:
    case TYPE_GROUP:
      field.fieldKind = "message";
      field.message = reg.getMessage(trimLeadingDot(proto.typeName));
      assert(field.message, `invalid FieldDescriptorProto: type_name ${proto.typeName} not found`);
      field.delimitedEncoding = isDelimitedEncoding(proto, parentOrFile);
      field.getDefaultValue = () => void 0;
      break;
    case TYPE_ENUM: {
      const enumeration = reg.getEnum(trimLeadingDot(proto.typeName));
      assert(enumeration !== void 0, `invalid FieldDescriptorProto: type_name ${proto.typeName} not found`);
      field.fieldKind = "enum";
      field.enum = reg.getEnum(trimLeadingDot(proto.typeName));
      field.getDefaultValue = () => {
        return unsafeIsSetExplicit(proto, "defaultValue") ? parseTextFormatEnumValue(enumeration, proto.defaultValue) : void 0;
      };
      break;
    }
    default:
      field.fieldKind = "scalar";
      field.scalar = type;
      field.longAsString = jstype == JS_STRING;
      field.getDefaultValue = () => {
        return unsafeIsSetExplicit(proto, "defaultValue") ? parseTextFormatScalarValue(type, proto.defaultValue) : void 0;
      };
      break;
  }
  return field;
}
function getFileEdition(proto) {
  switch (proto.syntax) {
    case "":
    case "proto2":
      return EDITION_PROTO2$1;
    case "proto3":
      return EDITION_PROTO3$1;
    case "editions":
      if (proto.edition in featureDefaults)
        return proto.edition;
      throw new Error(`${proto.name}: unsupported edition`);
    default:
      throw new Error(`${proto.name}: unsupported syntax "${proto.syntax}"`);
  }
}
function findFileDependencies(proto, reg) {
  return proto.dependency.map((wantName) => {
    const dep = reg.getFile(wantName);
    if (!dep)
      throw new Error(`Cannot find ${wantName}, imported by ${proto.name}`);
    return dep;
  });
}
function findEnumSharedPrefix(enumName, values) {
  const prefix = camelToSnakeCase(enumName) + "_";
  for (const value of values) {
    if (!value.name.toLowerCase().startsWith(prefix))
      return;
    const shortName = value.name.substring(prefix.length);
    if (shortName.length == 0)
      return;
    if (/^\d/.test(shortName))
      return;
  }
  return prefix;
}
function camelToSnakeCase(camel) {
  return (camel.substring(0, 1) + camel.substring(1).replace(/[A-Z]/g, (c) => "_" + c)).toLowerCase();
}
function makeTypeName(proto, parent, file) {
  let typeName;
  if (parent)
    typeName = `${parent.typeName}.${proto.name}`;
  else if (file.proto.package.length > 0)
    typeName = `${file.proto.package}.${proto.name}`;
  else
    typeName = `${proto.name}`;
  return typeName;
}
function trimLeadingDot(typeName) {
  return typeName.startsWith(".") ? typeName.substring(1) : typeName;
}
function findOneof(proto, allOneofs) {
  if (!unsafeIsSetExplicit(proto, "oneofIndex"))
    return;
  if (proto.proto3Optional)
    return;
  const oneof = allOneofs[proto.oneofIndex];
  assert(oneof, `invalid FieldDescriptorProto: oneof #${proto.oneofIndex} for field #${proto.number} not found`);
  return oneof;
}
function getFieldPresence(proto, oneof, isExtension, parent) {
  if (proto.label == LABEL_REQUIRED)
    return LEGACY_REQUIRED$1;
  if (proto.label == LABEL_REPEATED)
    return IMPLICIT$1;
  if (!!oneof || proto.proto3Optional)
    return EXPLICIT;
  if (isExtension)
    return EXPLICIT;
  const resolved = resolveFeature("fieldPresence", {
    proto,
    parent
  });
  if (resolved == IMPLICIT$1 && (proto.type == TYPE_MESSAGE || proto.type == TYPE_GROUP))
    return EXPLICIT;
  return resolved;
}
function isPackedField(proto, parent) {
  if (proto.label != LABEL_REPEATED)
    return false;
  switch (proto.type) {
    case TYPE_STRING:
    case TYPE_BYTES:
    case TYPE_GROUP:
    case TYPE_MESSAGE:
      return false;
  }
  const o = proto.options;
  if (o && unsafeIsSetExplicit(o, "packed"))
    return o.packed;
  return PACKED == resolveFeature("repeatedFieldEncoding", {
    proto,
    parent
  });
}
function findMapEntryFields(mapEntry) {
  const key = mapEntry.fields.find((f) => f.number === 1);
  const value = mapEntry.fields.find((f) => f.number === 2);
  assert(key && key.fieldKind == "scalar" && key.scalar != ScalarType2.BYTES && key.scalar != ScalarType2.FLOAT && key.scalar != ScalarType2.DOUBLE && value && value.fieldKind != "list" && value.fieldKind != "map");
  return {
    key,
    value
  };
}
function isEnumOpen(desc) {
  var _a;
  return OPEN == resolveFeature("enumType", {
    proto: desc.proto,
    parent: (_a = desc.parent) !== null && _a !== void 0 ? _a : desc.file
  });
}
function isDelimitedEncoding(proto, parent) {
  if (proto.type == TYPE_GROUP)
    return true;
  return DELIMITED == resolveFeature("messageEncoding", {
    proto,
    parent
  });
}
function resolveFeature(name, ref) {
  var _a, _b;
  const featureSet = (_a = ref.proto.options) === null || _a === void 0 ? void 0 : _a.features;
  if (featureSet) {
    const val = featureSet[name];
    if (val != 0)
      return val;
  }
  if ("kind" in ref) {
    if (ref.kind == "message")
      return resolveFeature(name, (_b = ref.parent) !== null && _b !== void 0 ? _b : ref.file);
    const editionDefaults = featureDefaults[ref.edition];
    if (!editionDefaults)
      throw new Error(`feature default for edition ${ref.edition} not found`);
    return editionDefaults[name];
  }
  return resolveFeature(name, ref.parent);
}
function assert(condition, msg) {
  if (!condition)
    throw new Error(msg);
}
function boot(boot$1) {
  const root = bootFileDescriptorProto(boot$1);
  root.messageType.forEach(restoreJsonNames);
  return createFileRegistry(root, () => void 0).getFile(root.name);
}
function bootFileDescriptorProto(init) {
  const proto = /* @__PURE__ */ Object.create({
    syntax: "",
    edition: 0
  });
  return Object.assign(proto, Object.assign(Object.assign({
    $typeName: "google.protobuf.FileDescriptorProto",
    dependency: [],
    publicDependency: [],
    weakDependency: [],
    optionDependency: [],
    service: [],
    extension: []
  }, init), {
    messageType: init.messageType.map(bootDescriptorProto),
    enumType: init.enumType.map(bootEnumDescriptorProto)
  }));
}
function bootDescriptorProto(init) {
  var _a, _b, _c, _d, _e, _f, _g, _h;
  const proto = /* @__PURE__ */ Object.create({ visibility: 0 });
  return Object.assign(proto, {
    $typeName: "google.protobuf.DescriptorProto",
    name: init.name,
    field: (_b = (_a = init.field) === null || _a === void 0 ? void 0 : _a.map(bootFieldDescriptorProto)) !== null && _b !== void 0 ? _b : [],
    extension: [],
    nestedType: (_d = (_c = init.nestedType) === null || _c === void 0 ? void 0 : _c.map(bootDescriptorProto)) !== null && _d !== void 0 ? _d : [],
    enumType: (_f = (_e = init.enumType) === null || _e === void 0 ? void 0 : _e.map(bootEnumDescriptorProto)) !== null && _f !== void 0 ? _f : [],
    extensionRange: (_h = (_g = init.extensionRange) === null || _g === void 0 ? void 0 : _g.map((e) => Object.assign({ $typeName: "google.protobuf.DescriptorProto.ExtensionRange" }, e))) !== null && _h !== void 0 ? _h : [],
    oneofDecl: [],
    reservedRange: [],
    reservedName: []
  });
}
function bootFieldDescriptorProto(init) {
  const proto = /* @__PURE__ */ Object.create({
    label: 1,
    typeName: "",
    extendee: "",
    defaultValue: "",
    oneofIndex: 0,
    jsonName: "",
    proto3Optional: false
  });
  return Object.assign(proto, Object.assign(Object.assign({ $typeName: "google.protobuf.FieldDescriptorProto" }, init), { options: init.options ? bootFieldOptions(init.options) : void 0 }));
}
function bootFieldOptions(init) {
  var _a, _b, _c;
  const proto = /* @__PURE__ */ Object.create({
    ctype: 0,
    packed: false,
    jstype: 0,
    lazy: false,
    unverifiedLazy: false,
    deprecated: false,
    weak: false,
    debugRedact: false,
    retention: 0
  });
  return Object.assign(proto, Object.assign(Object.assign({ $typeName: "google.protobuf.FieldOptions" }, init), {
    targets: (_a = init.targets) !== null && _a !== void 0 ? _a : [],
    editionDefaults: (_c = (_b = init.editionDefaults) === null || _b === void 0 ? void 0 : _b.map((e) => Object.assign({ $typeName: "google.protobuf.FieldOptions.EditionDefault" }, e))) !== null && _c !== void 0 ? _c : [],
    uninterpretedOption: []
  }));
}
function bootEnumDescriptorProto(init) {
  const proto = /* @__PURE__ */ Object.create({ visibility: 0 });
  return Object.assign(proto, {
    $typeName: "google.protobuf.EnumDescriptorProto",
    name: init.name,
    reservedName: [],
    reservedRange: [],
    value: init.value.map((e) => Object.assign({ $typeName: "google.protobuf.EnumValueDescriptorProto" }, e))
  });
}
function base64Decode(base64Str) {
  const table = getDecodeTable();
  let es = base64Str.length * 3 / 4;
  if (base64Str[base64Str.length - 2] == "=")
    es -= 2;
  else if (base64Str[base64Str.length - 1] == "=")
    es -= 1;
  let bytes = new Uint8Array(es), bytePos = 0, groupPos = 0, b, p = 0;
  for (let i = 0; i < base64Str.length; i++) {
    b = table[base64Str.charCodeAt(i)];
    if (b === void 0)
      switch (base64Str[i]) {
        case "=":
          groupPos = 0;
        case "\n":
        case "\r":
        case "	":
        case " ":
          continue;
        default:
          throw Error("invalid base64 string");
      }
    switch (groupPos) {
      case 0:
        p = b;
        groupPos = 1;
        break;
      case 1:
        bytes[bytePos++] = p << 2 | (b & 48) >> 4;
        p = b;
        groupPos = 2;
        break;
      case 2:
        bytes[bytePos++] = (p & 15) << 4 | (b & 60) >> 2;
        p = b;
        groupPos = 3;
        break;
      case 3:
        bytes[bytePos++] = (p & 3) << 6 | b;
        groupPos = 0;
        break;
    }
  }
  if (groupPos == 1)
    throw Error("invalid base64 string");
  return bytes.subarray(0, bytePos);
}
var encodeTableStd;
var encodeTableUrl;
var decodeTable;
function getEncodeTable(encoding) {
  if (!encodeTableStd) {
    encodeTableStd = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".split("");
    encodeTableUrl = encodeTableStd.slice(0, -2).concat("-", "_");
  }
  return encoding == "url" ? encodeTableUrl : encodeTableStd;
}
function getDecodeTable() {
  if (!decodeTable) {
    decodeTable = [];
    const encodeTable = getEncodeTable("std");
    for (let i = 0; i < encodeTable.length; i++)
      decodeTable[encodeTable[i].charCodeAt(0)] = i;
    decodeTable["-".charCodeAt(0)] = encodeTable.indexOf("+");
    decodeTable["_".charCodeAt(0)] = encodeTable.indexOf("/");
  }
  return decodeTable;
}
function isMessage2(arg, schema) {
  if (!(arg !== null && typeof arg == "object" && "$typeName" in arg && typeof arg.$typeName == "string"))
    return false;
  if (schema === void 0)
    return true;
  return schema.typeName === arg.$typeName;
}
var FieldError2 = class extends Error {
  constructor(fieldOrOneof, message, name = "FieldValueInvalidError") {
    super(message);
    this.name = name;
    this.field = () => fieldOrOneof;
  }
};
function isObject2(arg) {
  return arg !== null && typeof arg == "object" && !Array.isArray(arg);
}
function isReflectList2(arg, field) {
  var _a, _b, _c, _d;
  if (isObject2(arg) && unsafeLocal2 in arg && "add" in arg && "field" in arg && typeof arg.field == "function") {
    if (field !== void 0) {
      const a = field;
      const b = arg.field();
      return a.listKind == b.listKind && a.scalar === b.scalar && ((_a = a.message) === null || _a === void 0 ? void 0 : _a.typeName) === ((_b = b.message) === null || _b === void 0 ? void 0 : _b.typeName) && ((_c = a.enum) === null || _c === void 0 ? void 0 : _c.typeName) === ((_d = b.enum) === null || _d === void 0 ? void 0 : _d.typeName);
    }
    return true;
  }
  return false;
}
function isReflectMap2(arg, field) {
  var _a, _b, _c, _d;
  if (isObject2(arg) && unsafeLocal2 in arg && "has" in arg && "field" in arg && typeof arg.field == "function") {
    if (field !== void 0) {
      const a = field, b = arg.field();
      return a.mapKey === b.mapKey && a.mapKind == b.mapKind && a.scalar === b.scalar && ((_a = a.message) === null || _a === void 0 ? void 0 : _a.typeName) === ((_b = b.message) === null || _b === void 0 ? void 0 : _b.typeName) && ((_c = a.enum) === null || _c === void 0 ? void 0 : _c.typeName) === ((_d = b.enum) === null || _d === void 0 ? void 0 : _d.typeName);
    }
    return true;
  }
  return false;
}
function isReflectMessage2(arg, messageDesc$2) {
  return isObject2(arg) && unsafeLocal2 in arg && "desc" in arg && isObject2(arg.desc) && arg.desc.kind === "message" && (messageDesc$2 === void 0 || arg.desc.typeName == messageDesc$2.typeName);
}
var symbol2 = Symbol.for("@bufbuild/protobuf/text-encoding");
function getTextEncoding2() {
  if (globalThis[symbol2] == void 0) {
    const te = new globalThis.TextEncoder();
    const td = new globalThis.TextDecoder();
    globalThis[symbol2] = {
      encodeUtf8(text) {
        return te.encode(text);
      },
      decodeUtf8(bytes) {
        return td.decode(bytes);
      },
      checkUtf8(text) {
        try {
          return true;
        } catch (_) {
          return false;
        }
      }
    };
  }
  return globalThis[symbol2];
}
var WireType2;
(function(WireType$1) {
  WireType$1[WireType$1["Varint"] = 0] = "Varint";
  WireType$1[WireType$1["Bit64"] = 1] = "Bit64";
  WireType$1[WireType$1["LengthDelimited"] = 2] = "LengthDelimited";
  WireType$1[WireType$1["StartGroup"] = 3] = "StartGroup";
  WireType$1[WireType$1["EndGroup"] = 4] = "EndGroup";
  WireType$1[WireType$1["Bit32"] = 5] = "Bit32";
})(WireType2 || (WireType2 = {}));
var FLOAT32_MAX2 = 34028234663852886e22;
var FLOAT32_MIN2 = -34028234663852886e22;
var UINT32_MAX2 = 4294967295;
var INT32_MAX2 = 2147483647;
var INT32_MIN2 = -2147483648;
var BinaryWriter2 = class {
  constructor(encodeUtf8 = getTextEncoding2().encodeUtf8) {
    this.encodeUtf8 = encodeUtf8;
    this.stack = [];
    this.chunks = [];
    this.buf = [];
  }
  /**
  * Return all bytes written and reset this writer.
  */
  finish() {
    if (this.buf.length) {
      this.chunks.push(new Uint8Array(this.buf));
      this.buf = [];
    }
    let len = 0;
    for (let i = 0; i < this.chunks.length; i++)
      len += this.chunks[i].length;
    let bytes = new Uint8Array(len);
    let offset = 0;
    for (let i = 0; i < this.chunks.length; i++) {
      bytes.set(this.chunks[i], offset);
      offset += this.chunks[i].length;
    }
    this.chunks = [];
    return bytes;
  }
  /**
  * Start a new fork for length-delimited data like a message
  * or a packed repeated field.
  *
  * Must be joined later with `join()`.
  */
  fork() {
    this.stack.push({
      chunks: this.chunks,
      buf: this.buf
    });
    this.chunks = [];
    this.buf = [];
    return this;
  }
  /**
  * Join the last fork. Write its length and bytes, then
  * return to the previous state.
  */
  join() {
    let chunk = this.finish();
    let prev = this.stack.pop();
    if (!prev)
      throw new Error("invalid state, fork stack empty");
    this.chunks = prev.chunks;
    this.buf = prev.buf;
    this.uint32(chunk.byteLength);
    return this.raw(chunk);
  }
  /**
  * Writes a tag (field number and wire type).
  *
  * Equivalent to `uint32( (fieldNo << 3 | type) >>> 0 )`.
  *
  * Generated code should compute the tag ahead of time and call `uint32()`.
  */
  tag(fieldNo, type) {
    return this.uint32((fieldNo << 3 | type) >>> 0);
  }
  /**
  * Write a chunk of raw bytes.
  */
  raw(chunk) {
    if (this.buf.length) {
      this.chunks.push(new Uint8Array(this.buf));
      this.buf = [];
    }
    this.chunks.push(chunk);
    return this;
  }
  /**
  * Write a `uint32` value, an unsigned 32 bit varint.
  */
  uint32(value) {
    assertUInt322(value);
    while (value > 127) {
      this.buf.push(value & 127 | 128);
      value = value >>> 7;
    }
    this.buf.push(value);
    return this;
  }
  /**
  * Write a `int32` value, a signed 32 bit varint.
  */
  int32(value) {
    assertInt322(value);
    varint32write2(value, this.buf);
    return this;
  }
  /**
  * Write a `bool` value, a variant.
  */
  bool(value) {
    this.buf.push(value ? 1 : 0);
    return this;
  }
  /**
  * Write a `bytes` value, length-delimited arbitrary data.
  */
  bytes(value) {
    this.uint32(value.byteLength);
    return this.raw(value);
  }
  /**
  * Write a `string` value, length-delimited data converted to UTF-8 text.
  */
  string(value) {
    let chunk = this.encodeUtf8(value);
    this.uint32(chunk.byteLength);
    return this.raw(chunk);
  }
  /**
  * Write a `float` value, 32-bit floating point number.
  */
  float(value) {
    assertFloat322(value);
    let chunk = new Uint8Array(4);
    new DataView(chunk.buffer).setFloat32(0, value, true);
    return this.raw(chunk);
  }
  /**
  * Write a `double` value, a 64-bit floating point number.
  */
  double(value) {
    let chunk = new Uint8Array(8);
    new DataView(chunk.buffer).setFloat64(0, value, true);
    return this.raw(chunk);
  }
  /**
  * Write a `fixed32` value, an unsigned, fixed-length 32-bit integer.
  */
  fixed32(value) {
    assertUInt322(value);
    let chunk = new Uint8Array(4);
    new DataView(chunk.buffer).setUint32(0, value, true);
    return this.raw(chunk);
  }
  /**
  * Write a `sfixed32` value, a signed, fixed-length 32-bit integer.
  */
  sfixed32(value) {
    assertInt322(value);
    let chunk = new Uint8Array(4);
    new DataView(chunk.buffer).setInt32(0, value, true);
    return this.raw(chunk);
  }
  /**
  * Write a `sint32` value, a signed, zigzag-encoded 32-bit varint.
  */
  sint32(value) {
    assertInt322(value);
    value = (value << 1 ^ value >> 31) >>> 0;
    varint32write2(value, this.buf);
    return this;
  }
  /**
  * Write a `fixed64` value, a signed, fixed-length 64-bit integer.
  */
  sfixed64(value) {
    let chunk = new Uint8Array(8), view = new DataView(chunk.buffer), tc = protoInt642.enc(value);
    view.setInt32(0, tc.lo, true);
    view.setInt32(4, tc.hi, true);
    return this.raw(chunk);
  }
  /**
  * Write a `fixed64` value, an unsigned, fixed-length 64 bit integer.
  */
  fixed64(value) {
    let chunk = new Uint8Array(8), view = new DataView(chunk.buffer), tc = protoInt642.uEnc(value);
    view.setInt32(0, tc.lo, true);
    view.setInt32(4, tc.hi, true);
    return this.raw(chunk);
  }
  /**
  * Write a `int64` value, a signed 64-bit varint.
  */
  int64(value) {
    let tc = protoInt642.enc(value);
    varint64write2(tc.lo, tc.hi, this.buf);
    return this;
  }
  /**
  * Write a `sint64` value, a signed, zig-zag-encoded 64-bit varint.
  */
  sint64(value) {
    const tc = protoInt642.enc(value), sign = tc.hi >> 31, lo = tc.lo << 1 ^ sign, hi = (tc.hi << 1 | tc.lo >>> 31) ^ sign;
    varint64write2(lo, hi, this.buf);
    return this;
  }
  /**
  * Write a `uint64` value, an unsigned 64-bit varint.
  */
  uint64(value) {
    const tc = protoInt642.uEnc(value);
    varint64write2(tc.lo, tc.hi, this.buf);
    return this;
  }
};
var BinaryReader2 = class {
  constructor(buf, decodeUtf8 = getTextEncoding2().decodeUtf8) {
    this.decodeUtf8 = decodeUtf8;
    this.varint64 = varint64read2;
    this.uint32 = varint32read2;
    this.buf = buf;
    this.len = buf.length;
    this.pos = 0;
    this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }
  /**
  * Reads a tag - field number and wire type.
  */
  tag() {
    let tag = this.uint32(), fieldNo = tag >>> 3, wireType = tag & 7;
    if (fieldNo <= 0 || wireType < 0 || wireType > 5)
      throw new Error("illegal tag: field no " + fieldNo + " wire type " + wireType);
    return [fieldNo, wireType];
  }
  /**
  * Skip one element and return the skipped data.
  *
  * When skipping StartGroup, provide the tags field number to check for
  * matching field number in the EndGroup tag.
  */
  skip(wireType, fieldNo) {
    let start = this.pos;
    switch (wireType) {
      case WireType2.Varint:
        while (this.buf[this.pos++] & 128)
          ;
        break;
      case WireType2.Bit64:
        this.pos += 4;
      case WireType2.Bit32:
        this.pos += 4;
        break;
      case WireType2.LengthDelimited:
        let len = this.uint32();
        this.pos += len;
        break;
      case WireType2.StartGroup:
        for (; ; ) {
          const [fn, wt] = this.tag();
          if (wt === WireType2.EndGroup) {
            if (fieldNo !== void 0 && fn !== fieldNo)
              throw new Error("invalid end group tag");
            break;
          }
          this.skip(wt, fn);
        }
        break;
      default:
        throw new Error("cant skip wire type " + wireType);
    }
    this.assertBounds();
    return this.buf.subarray(start, this.pos);
  }
  /**
  * Throws error if position in byte array is out of range.
  */
  assertBounds() {
    if (this.pos > this.len)
      throw new RangeError("premature EOF");
  }
  /**
  * Read a `int32` field, a signed 32 bit varint.
  */
  int32() {
    return this.uint32() | 0;
  }
  /**
  * Read a `sint32` field, a signed, zigzag-encoded 32-bit varint.
  */
  sint32() {
    let zze = this.uint32();
    return zze >>> 1 ^ -(zze & 1);
  }
  /**
  * Read a `int64` field, a signed 64-bit varint.
  */
  int64() {
    return protoInt642.dec(...this.varint64());
  }
  /**
  * Read a `uint64` field, an unsigned 64-bit varint.
  */
  uint64() {
    return protoInt642.uDec(...this.varint64());
  }
  /**
  * Read a `sint64` field, a signed, zig-zag-encoded 64-bit varint.
  */
  sint64() {
    let [lo, hi] = this.varint64();
    let s = -(lo & 1);
    lo = (lo >>> 1 | (hi & 1) << 31) ^ s;
    hi = hi >>> 1 ^ s;
    return protoInt642.dec(lo, hi);
  }
  /**
  * Read a `bool` field, a variant.
  */
  bool() {
    let [lo, hi] = this.varint64();
    return lo !== 0 || hi !== 0;
  }
  /**
  * Read a `fixed32` field, an unsigned, fixed-length 32-bit integer.
  */
  fixed32() {
    return this.view.getUint32((this.pos += 4) - 4, true);
  }
  /**
  * Read a `sfixed32` field, a signed, fixed-length 32-bit integer.
  */
  sfixed32() {
    return this.view.getInt32((this.pos += 4) - 4, true);
  }
  /**
  * Read a `fixed64` field, an unsigned, fixed-length 64 bit integer.
  */
  fixed64() {
    return protoInt642.uDec(this.sfixed32(), this.sfixed32());
  }
  /**
  * Read a `fixed64` field, a signed, fixed-length 64-bit integer.
  */
  sfixed64() {
    return protoInt642.dec(this.sfixed32(), this.sfixed32());
  }
  /**
  * Read a `float` field, 32-bit floating point number.
  */
  float() {
    return this.view.getFloat32((this.pos += 4) - 4, true);
  }
  /**
  * Read a `double` field, a 64-bit floating point number.
  */
  double() {
    return this.view.getFloat64((this.pos += 8) - 8, true);
  }
  /**
  * Read a `bytes` field, length-delimited arbitrary data.
  */
  bytes() {
    let len = this.uint32(), start = this.pos;
    this.pos += len;
    this.assertBounds();
    return this.buf.subarray(start, start + len);
  }
  /**
  * Read a `string` field, length-delimited data converted to UTF-8 text.
  */
  string() {
    return this.decodeUtf8(this.bytes());
  }
};
function assertInt322(arg) {
  if (typeof arg == "string")
    arg = Number(arg);
  else if (typeof arg != "number")
    throw new Error("invalid int32: " + typeof arg);
  if (!Number.isInteger(arg) || arg > INT32_MAX2 || arg < INT32_MIN2)
    throw new Error("invalid int32: " + arg);
}
function assertUInt322(arg) {
  if (typeof arg == "string")
    arg = Number(arg);
  else if (typeof arg != "number")
    throw new Error("invalid uint32: " + typeof arg);
  if (!Number.isInteger(arg) || arg > UINT32_MAX2 || arg < 0)
    throw new Error("invalid uint32: " + arg);
}
function assertFloat322(arg) {
  if (typeof arg == "string") {
    const o = arg;
    arg = Number(arg);
    if (Number.isNaN(arg) && o !== "NaN")
      throw new Error("invalid float32: " + o);
  } else if (typeof arg != "number")
    throw new Error("invalid float32: " + typeof arg);
  if (Number.isFinite(arg) && (arg > FLOAT32_MAX2 || arg < FLOAT32_MIN2))
    throw new Error("invalid float32: " + arg);
}
function checkField2(field, value) {
  const check = field.fieldKind == "list" ? isReflectList2(value, field) : field.fieldKind == "map" ? isReflectMap2(value, field) : checkSingular2(field, value);
  if (check === true)
    return;
  let reason;
  switch (field.fieldKind) {
    case "list":
      reason = `expected ${formatReflectList2(field)}, got ${formatVal2(value)}`;
      break;
    case "map":
      reason = `expected ${formatReflectMap2(field)}, got ${formatVal2(value)}`;
      break;
    default:
      reason = reasonSingular2(field, value, check);
  }
  return new FieldError2(field, reason);
}
function checkListItem2(field, index, value) {
  const check = checkSingular2(field, value);
  if (check !== true)
    return new FieldError2(field, `list item #${index + 1}: ${reasonSingular2(field, value, check)}`);
}
function checkMapEntry2(field, key, value) {
  const checkKey = checkScalarValue2(key, field.mapKey);
  if (checkKey !== true)
    return new FieldError2(field, `invalid map key: ${reasonSingular2({ scalar: field.mapKey }, key, checkKey)}`);
  const checkVal = checkSingular2(field, value);
  if (checkVal !== true)
    return new FieldError2(field, `map entry ${formatVal2(key)}: ${reasonSingular2(field, value, checkVal)}`);
}
function checkSingular2(field, value) {
  if (field.scalar !== void 0)
    return checkScalarValue2(value, field.scalar);
  if (field.enum !== void 0) {
    if (field.enum.open)
      return Number.isInteger(value);
    return field.enum.values.some((v) => v.number === value);
  }
  return isReflectMessage2(value, field.message);
}
function checkScalarValue2(value, scalar) {
  switch (scalar) {
    case ScalarType2.DOUBLE:
      return typeof value == "number";
    case ScalarType2.FLOAT:
      if (typeof value != "number")
        return false;
      if (Number.isNaN(value) || !Number.isFinite(value))
        return true;
      if (value > FLOAT32_MAX2 || value < FLOAT32_MIN2)
        return `${value.toFixed()} out of range`;
      return true;
    case ScalarType2.INT32:
    case ScalarType2.SFIXED32:
    case ScalarType2.SINT32:
      if (typeof value !== "number" || !Number.isInteger(value))
        return false;
      if (value > INT32_MAX2 || value < INT32_MIN2)
        return `${value.toFixed()} out of range`;
      return true;
    case ScalarType2.FIXED32:
    case ScalarType2.UINT32:
      if (typeof value !== "number" || !Number.isInteger(value))
        return false;
      if (value > UINT32_MAX2 || value < 0)
        return `${value.toFixed()} out of range`;
      return true;
    case ScalarType2.BOOL:
      return typeof value == "boolean";
    case ScalarType2.STRING:
      if (typeof value != "string")
        return false;
      return getTextEncoding2().checkUtf8(value) || "invalid UTF8";
    case ScalarType2.BYTES:
      return value instanceof Uint8Array;
    case ScalarType2.INT64:
    case ScalarType2.SFIXED64:
    case ScalarType2.SINT64:
      if (typeof value == "bigint" || typeof value == "number" || typeof value == "string" && value.length > 0)
        try {
          protoInt642.parse(value);
          return true;
        } catch (_) {
          return `${value} out of range`;
        }
      return false;
    case ScalarType2.FIXED64:
    case ScalarType2.UINT64:
      if (typeof value == "bigint" || typeof value == "number" || typeof value == "string" && value.length > 0)
        try {
          protoInt642.uParse(value);
          return true;
        } catch (_) {
          return `${value} out of range`;
        }
      return false;
  }
}
function reasonSingular2(field, val, details) {
  details = typeof details == "string" ? `: ${details}` : `, got ${formatVal2(val)}`;
  if (field.scalar !== void 0)
    return `expected ${scalarTypeDescription2(field.scalar)}` + details;
  if (field.enum !== void 0)
    return `expected ${field.enum.toString()}` + details;
  return `expected ${formatReflectMessage2(field.message)}` + details;
}
function formatVal2(val) {
  switch (typeof val) {
    case "object":
      if (val === null)
        return "null";
      if (val instanceof Uint8Array)
        return `Uint8Array(${val.length})`;
      if (Array.isArray(val))
        return `Array(${val.length})`;
      if (isReflectList2(val))
        return formatReflectList2(val.field());
      if (isReflectMap2(val))
        return formatReflectMap2(val.field());
      if (isReflectMessage2(val))
        return formatReflectMessage2(val.desc);
      if (isMessage2(val))
        return `message ${val.$typeName}`;
      return "object";
    case "string":
      return val.length > 30 ? "string" : `"${val.split('"').join('\\"')}"`;
    case "boolean":
      return String(val);
    case "number":
      return String(val);
    case "bigint":
      return String(val) + "n";
    default:
      return typeof val;
  }
}
function formatReflectMessage2(desc) {
  return `ReflectMessage (${desc.typeName})`;
}
function formatReflectList2(field) {
  switch (field.listKind) {
    case "message":
      return `ReflectList (${field.message.toString()})`;
    case "enum":
      return `ReflectList (${field.enum.toString()})`;
    case "scalar":
      return `ReflectList (${ScalarType2[field.scalar]})`;
  }
}
function formatReflectMap2(field) {
  switch (field.mapKind) {
    case "message":
      return `ReflectMap (${ScalarType2[field.mapKey]}, ${field.message.toString()})`;
    case "enum":
      return `ReflectMap (${ScalarType2[field.mapKey]}, ${field.enum.toString()})`;
    case "scalar":
      return `ReflectMap (${ScalarType2[field.mapKey]}, ${ScalarType2[field.scalar]})`;
  }
}
function scalarTypeDescription2(scalar) {
  switch (scalar) {
    case ScalarType2.STRING:
      return "string";
    case ScalarType2.BOOL:
      return "boolean";
    case ScalarType2.INT64:
    case ScalarType2.SINT64:
    case ScalarType2.SFIXED64:
      return "bigint (int64)";
    case ScalarType2.UINT64:
    case ScalarType2.FIXED64:
      return "bigint (uint64)";
    case ScalarType2.BYTES:
      return "Uint8Array";
    case ScalarType2.DOUBLE:
      return "number (float64)";
    case ScalarType2.FLOAT:
      return "number (float32)";
    case ScalarType2.FIXED32:
    case ScalarType2.UINT32:
      return "number (uint32)";
    case ScalarType2.INT32:
    case ScalarType2.SFIXED32:
    case ScalarType2.SINT32:
      return "number (int32)";
  }
}
function isWrapper2(arg) {
  return isWrapperTypeName2(arg.$typeName);
}
function isWrapperDesc2(messageDesc$2) {
  const f = messageDesc$2.fields[0];
  return isWrapperTypeName2(messageDesc$2.typeName) && f !== void 0 && f.fieldKind == "scalar" && f.name == "value" && f.number == 1;
}
function isWrapperTypeName2(name) {
  return name.startsWith("google.protobuf.") && [
    "DoubleValue",
    "FloatValue",
    "Int64Value",
    "UInt64Value",
    "Int32Value",
    "UInt32Value",
    "BoolValue",
    "StringValue",
    "BytesValue"
  ].includes(name.substring(16));
}
var EDITION_PROTO32 = 999;
var EDITION_PROTO22 = 998;
var IMPLICIT3 = 2;
function create2(schema, init) {
  if (isMessage2(init, schema))
    return init;
  const message = createZeroMessage2(schema);
  if (init !== void 0)
    initMessage2(schema, message, init);
  return message;
}
function initMessage2(messageDesc$2, message, init) {
  for (const member of messageDesc$2.members) {
    let value = init[member.localName];
    if (value == null)
      continue;
    let field;
    if (member.kind == "oneof") {
      const oneofField = unsafeOneofCase2(init, member);
      if (!oneofField)
        continue;
      field = oneofField;
      value = unsafeGet2(init, oneofField);
    } else
      field = member;
    switch (field.fieldKind) {
      case "message":
        value = toMessage2(field, value);
        break;
      case "scalar":
        value = initScalar2(field, value);
        break;
      case "list":
        value = initList2(field, value);
        break;
      case "map":
        value = initMap2(field, value);
        break;
    }
    unsafeSet2(message, field, value);
  }
  return message;
}
function initScalar2(field, value) {
  if (field.scalar == ScalarType2.BYTES)
    return toU8Arr2(value);
  return value;
}
function initMap2(field, value) {
  if (isObject2(value)) {
    if (field.scalar == ScalarType2.BYTES)
      return convertObjectValues2(value, toU8Arr2);
    if (field.mapKind == "message")
      return convertObjectValues2(value, (val) => toMessage2(field, val));
  }
  return value;
}
function initList2(field, value) {
  if (Array.isArray(value)) {
    if (field.scalar == ScalarType2.BYTES)
      return value.map(toU8Arr2);
    if (field.listKind == "message")
      return value.map((item) => toMessage2(field, item));
  }
  return value;
}
function toMessage2(field, value) {
  if (field.fieldKind == "message" && !field.oneof && isWrapperDesc2(field.message))
    return initScalar2(field.message.fields[0], value);
  if (isObject2(value)) {
    if (field.message.typeName == "google.protobuf.Struct" && field.parent.typeName !== "google.protobuf.Value")
      return value;
    if (!isMessage2(value, field.message))
      return create2(field.message, value);
  }
  return value;
}
function toU8Arr2(value) {
  return Array.isArray(value) ? new Uint8Array(value) : value;
}
function convertObjectValues2(obj, fn) {
  const ret = {};
  for (const entry of Object.entries(obj))
    ret[entry[0]] = fn(entry[1]);
  return ret;
}
var tokenZeroMessageField2 = Symbol();
var messagePrototypes2 = /* @__PURE__ */ new WeakMap();
function createZeroMessage2(desc) {
  let msg;
  if (!needsPrototypeChain2(desc)) {
    msg = { $typeName: desc.typeName };
    for (const member of desc.members)
      if (member.kind == "oneof" || member.presence == IMPLICIT3)
        msg[member.localName] = createZeroField2(member);
  } else {
    const cached = messagePrototypes2.get(desc);
    let prototype;
    let members;
    if (cached)
      ({ prototype, members } = cached);
    else {
      prototype = {};
      members = /* @__PURE__ */ new Set();
      for (const member of desc.members) {
        if (member.kind == "oneof")
          continue;
        if (member.fieldKind != "scalar" && member.fieldKind != "enum")
          continue;
        if (member.presence == IMPLICIT3)
          continue;
        members.add(member);
        prototype[member.localName] = createZeroField2(member);
      }
      messagePrototypes2.set(desc, {
        prototype,
        members
      });
    }
    msg = Object.create(prototype);
    msg.$typeName = desc.typeName;
    for (const member of desc.members) {
      if (members.has(member))
        continue;
      if (member.kind == "field") {
        if (member.fieldKind == "message")
          continue;
        if (member.fieldKind == "scalar" || member.fieldKind == "enum") {
          if (member.presence != IMPLICIT3)
            continue;
        }
      }
      msg[member.localName] = createZeroField2(member);
    }
  }
  return msg;
}
function needsPrototypeChain2(desc) {
  switch (desc.file.edition) {
    case EDITION_PROTO32:
      return false;
    case EDITION_PROTO22:
      return true;
    default:
      return desc.fields.some((f) => f.presence != IMPLICIT3 && f.fieldKind != "message" && !f.oneof);
  }
}
function createZeroField2(field) {
  if (field.kind == "oneof")
    return { case: void 0 };
  if (field.fieldKind == "list")
    return [];
  if (field.fieldKind == "map")
    return {};
  if (field.fieldKind == "message")
    return tokenZeroMessageField2;
  const defaultValue = field.getDefaultValue();
  if (defaultValue !== void 0)
    return field.fieldKind == "scalar" && field.longAsString ? defaultValue.toString() : defaultValue;
  return field.fieldKind == "scalar" ? scalarZeroValue2(field.scalar, field.longAsString) : field.enum.values[0].number;
}
function reflect2(messageDesc$2, message, check = true) {
  return new ReflectMessageImpl2(messageDesc$2, message, check);
}
var ReflectMessageImpl2 = class {
  get sortedFields() {
    var _a;
    return (_a = this._sortedFields) !== null && _a !== void 0 ? _a : this._sortedFields = this.desc.fields.concat().sort((a, b) => a.number - b.number);
  }
  constructor(messageDesc$2, message, check = true) {
    this.lists = /* @__PURE__ */ new Map();
    this.maps = /* @__PURE__ */ new Map();
    this.check = check;
    this.desc = messageDesc$2;
    this.message = this[unsafeLocal2] = message !== null && message !== void 0 ? message : create2(messageDesc$2);
    this.fields = messageDesc$2.fields;
    this.oneofs = messageDesc$2.oneofs;
    this.members = messageDesc$2.members;
  }
  findNumber(number) {
    if (!this._fieldsByNumber)
      this._fieldsByNumber = new Map(this.desc.fields.map((f) => [f.number, f]));
    return this._fieldsByNumber.get(number);
  }
  oneofCase(oneof) {
    assertOwn2(this.message, oneof);
    return unsafeOneofCase2(this.message, oneof);
  }
  isSet(field) {
    assertOwn2(this.message, field);
    return unsafeIsSet2(this.message, field);
  }
  clear(field) {
    assertOwn2(this.message, field);
    unsafeClear2(this.message, field);
  }
  get(field) {
    assertOwn2(this.message, field);
    const value = unsafeGet2(this.message, field);
    switch (field.fieldKind) {
      case "list":
        let list = this.lists.get(field);
        if (!list || list[unsafeLocal2] !== value)
          this.lists.set(field, list = new ReflectListImpl2(field, value, this.check));
        return list;
      case "map":
        let map = this.maps.get(field);
        if (!map || map[unsafeLocal2] !== value)
          this.maps.set(field, map = new ReflectMapImpl2(field, value, this.check));
        return map;
      case "message":
        return messageToReflect2(field, value, this.check);
      case "scalar":
        return value === void 0 ? scalarZeroValue2(field.scalar, false) : longToReflect2(field, value);
      case "enum":
        return value !== null && value !== void 0 ? value : field.enum.values[0].number;
    }
  }
  set(field, value) {
    assertOwn2(this.message, field);
    if (this.check) {
      const err = checkField2(field, value);
      if (err)
        throw err;
    }
    let local;
    if (field.fieldKind == "message")
      local = messageToLocal2(field, value);
    else if (isReflectMap2(value) || isReflectList2(value))
      local = value[unsafeLocal2];
    else
      local = longToLocal2(field, value);
    unsafeSet2(this.message, field, local);
  }
  getUnknown() {
    return this.message.$unknown;
  }
  setUnknown(value) {
    this.message.$unknown = value;
  }
};
function assertOwn2(owner, member) {
  if (member.parent.typeName !== owner.$typeName)
    throw new FieldError2(member, `cannot use ${member.toString()} with message ${owner.$typeName}`, "ForeignFieldError");
}
var ReflectListImpl2 = class {
  field() {
    return this._field;
  }
  get size() {
    return this._arr.length;
  }
  constructor(field, unsafeInput, check) {
    this._field = field;
    this._arr = this[unsafeLocal2] = unsafeInput;
    this.check = check;
  }
  get(index) {
    const item = this._arr[index];
    return item === void 0 ? void 0 : listItemToReflect2(this._field, item, this.check);
  }
  set(index, item) {
    if (index < 0 || index >= this._arr.length)
      throw new FieldError2(this._field, `list item #${index + 1}: out of range`);
    if (this.check) {
      const err = checkListItem2(this._field, index, item);
      if (err)
        throw err;
    }
    this._arr[index] = listItemToLocal2(this._field, item);
  }
  add(item) {
    if (this.check) {
      const err = checkListItem2(this._field, this._arr.length, item);
      if (err)
        throw err;
    }
    this._arr.push(listItemToLocal2(this._field, item));
  }
  clear() {
    this._arr.splice(0, this._arr.length);
  }
  [Symbol.iterator]() {
    return this.values();
  }
  keys() {
    return this._arr.keys();
  }
  *values() {
    for (const item of this._arr)
      yield listItemToReflect2(this._field, item, this.check);
  }
  *entries() {
    for (let i = 0; i < this._arr.length; i++)
      yield [i, listItemToReflect2(this._field, this._arr[i], this.check)];
  }
};
var ReflectMapImpl2 = class {
  constructor(field, unsafeInput, check = true) {
    this.obj = this[unsafeLocal2] = unsafeInput !== null && unsafeInput !== void 0 ? unsafeInput : {};
    this.check = check;
    this._field = field;
  }
  field() {
    return this._field;
  }
  set(key, value) {
    if (this.check) {
      const err = checkMapEntry2(this._field, key, value);
      if (err)
        throw err;
    }
    this.obj[mapKeyToLocal2(key)] = mapValueToLocal2(this._field, value);
    return this;
  }
  delete(key) {
    const k = mapKeyToLocal2(key);
    const has = Object.prototype.hasOwnProperty.call(this.obj, k);
    if (has)
      delete this.obj[k];
    return has;
  }
  clear() {
    for (const key of Object.keys(this.obj))
      delete this.obj[key];
  }
  get(key) {
    let val = this.obj[mapKeyToLocal2(key)];
    if (val !== void 0)
      val = mapValueToReflect2(this._field, val, this.check);
    return val;
  }
  has(key) {
    return Object.prototype.hasOwnProperty.call(this.obj, mapKeyToLocal2(key));
  }
  *keys() {
    for (const objKey of Object.keys(this.obj))
      yield mapKeyToReflect2(objKey, this._field.mapKey);
  }
  *entries() {
    for (const objEntry of Object.entries(this.obj))
      yield [mapKeyToReflect2(objEntry[0], this._field.mapKey), mapValueToReflect2(this._field, objEntry[1], this.check)];
  }
  [Symbol.iterator]() {
    return this.entries();
  }
  get size() {
    return Object.keys(this.obj).length;
  }
  *values() {
    for (const val of Object.values(this.obj))
      yield mapValueToReflect2(this._field, val, this.check);
  }
  forEach(callbackfn, thisArg) {
    for (const mapEntry of this.entries())
      callbackfn.call(thisArg, mapEntry[1], mapEntry[0], this);
  }
};
function messageToLocal2(field, value) {
  if (!isReflectMessage2(value))
    return value;
  if (isWrapper2(value.message) && !field.oneof && field.fieldKind == "message")
    return value.message.value;
  if (value.desc.typeName == "google.protobuf.Struct" && field.parent.typeName != "google.protobuf.Value")
    return wktStructToLocal2(value.message);
  return value.message;
}
function messageToReflect2(field, value, check) {
  if (value !== void 0) {
    if (isWrapperDesc2(field.message) && !field.oneof && field.fieldKind == "message")
      value = {
        $typeName: field.message.typeName,
        value: longToReflect2(field.message.fields[0], value)
      };
    else if (field.message.typeName == "google.protobuf.Struct" && field.parent.typeName != "google.protobuf.Value" && isObject2(value))
      value = wktStructToReflect2(value);
  }
  return new ReflectMessageImpl2(field.message, value, check);
}
function listItemToLocal2(field, value) {
  if (field.listKind == "message")
    return messageToLocal2(field, value);
  return longToLocal2(field, value);
}
function listItemToReflect2(field, value, check) {
  if (field.listKind == "message")
    return messageToReflect2(field, value, check);
  return longToReflect2(field, value);
}
function mapValueToLocal2(field, value) {
  if (field.mapKind == "message")
    return messageToLocal2(field, value);
  return longToLocal2(field, value);
}
function mapValueToReflect2(field, value, check) {
  if (field.mapKind == "message")
    return messageToReflect2(field, value, check);
  return value;
}
function mapKeyToLocal2(key) {
  return typeof key == "string" || typeof key == "number" ? key : String(key);
}
function mapKeyToReflect2(key, type) {
  switch (type) {
    case ScalarType2.STRING:
      return key;
    case ScalarType2.INT32:
    case ScalarType2.FIXED32:
    case ScalarType2.UINT32:
    case ScalarType2.SFIXED32:
    case ScalarType2.SINT32: {
      const n = Number.parseInt(key);
      if (Number.isFinite(n))
        return n;
      break;
    }
    case ScalarType2.BOOL:
      switch (key) {
        case "true":
          return true;
        case "false":
          return false;
      }
      break;
    case ScalarType2.UINT64:
    case ScalarType2.FIXED64:
      try {
        return protoInt642.uParse(key);
      } catch (_a) {
      }
      break;
    default:
      try {
        return protoInt642.parse(key);
      } catch (_b) {
      }
      break;
  }
  return key;
}
function longToReflect2(field, value) {
  switch (field.scalar) {
    case ScalarType2.INT64:
    case ScalarType2.SFIXED64:
    case ScalarType2.SINT64:
      if ("longAsString" in field && field.longAsString && typeof value == "string")
        value = protoInt642.parse(value);
      break;
    case ScalarType2.FIXED64:
    case ScalarType2.UINT64:
      if ("longAsString" in field && field.longAsString && typeof value == "string")
        value = protoInt642.uParse(value);
      break;
  }
  return value;
}
function longToLocal2(field, value) {
  switch (field.scalar) {
    case ScalarType2.INT64:
    case ScalarType2.SFIXED64:
    case ScalarType2.SINT64:
      if ("longAsString" in field && field.longAsString)
        value = String(value);
      else if (typeof value == "string" || typeof value == "number")
        value = protoInt642.parse(value);
      break;
    case ScalarType2.FIXED64:
    case ScalarType2.UINT64:
      if ("longAsString" in field && field.longAsString)
        value = String(value);
      else if (typeof value == "string" || typeof value == "number")
        value = protoInt642.uParse(value);
      break;
  }
  return value;
}
function wktStructToReflect2(json) {
  const struct = {
    $typeName: "google.protobuf.Struct",
    fields: {}
  };
  if (isObject2(json))
    for (const [k, v] of Object.entries(json))
      struct.fields[k] = wktValueToReflect2(v);
  return struct;
}
function wktStructToLocal2(val) {
  const json = {};
  for (const [k, v] of Object.entries(val.fields))
    json[k] = wktValueToLocal2(v);
  return json;
}
function wktValueToLocal2(val) {
  switch (val.kind.case) {
    case "structValue":
      return wktStructToLocal2(val.kind.value);
    case "listValue":
      return val.kind.value.values.map(wktValueToLocal2);
    case "nullValue":
    case void 0:
      return null;
    default:
      return val.kind.value;
  }
}
function wktValueToReflect2(json) {
  const value = {
    $typeName: "google.protobuf.Value",
    kind: { case: void 0 }
  };
  switch (typeof json) {
    case "number":
      value.kind = {
        case: "numberValue",
        value: json
      };
      break;
    case "string":
      value.kind = {
        case: "stringValue",
        value: json
      };
      break;
    case "boolean":
      value.kind = {
        case: "boolValue",
        value: json
      };
      break;
    case "object":
      if (json === null)
        value.kind = {
          case: "nullValue",
          value: 0
        };
      else if (Array.isArray(json)) {
        const listValue = {
          $typeName: "google.protobuf.ListValue",
          values: []
        };
        if (Array.isArray(json))
          for (const e of json)
            listValue.values.push(wktValueToReflect2(e));
        value.kind = {
          case: "listValue",
          value: listValue
        };
      } else
        value.kind = {
          case: "structValue",
          value: wktStructToReflect2(json)
        };
      break;
  }
  return value;
}
var LEGACY_REQUIRED2 = 3;
var writeDefaults2 = { writeUnknownFields: true };
function makeWriteOptions2(options) {
  return options ? Object.assign(Object.assign({}, writeDefaults2), options) : writeDefaults2;
}
function toBinary2(schema, message, options) {
  return writeFields2(new BinaryWriter2(), makeWriteOptions2(options), reflect2(schema, message)).finish();
}
function writeFields2(writer, opts, msg) {
  var _a;
  for (const f of msg.sortedFields) {
    if (!msg.isSet(f)) {
      if (f.presence == LEGACY_REQUIRED2)
        throw new Error(`cannot encode ${f} to binary: required field not set`);
      continue;
    }
    writeField2(writer, opts, msg, f);
  }
  if (opts.writeUnknownFields)
    for (const { no, wireType, data } of (_a = msg.getUnknown()) !== null && _a !== void 0 ? _a : [])
      writer.tag(no, wireType).raw(data);
  return writer;
}
function writeField2(writer, opts, msg, field) {
  var _a;
  switch (field.fieldKind) {
    case "scalar":
    case "enum":
      writeScalar2(writer, msg.desc.typeName, field.name, (_a = field.scalar) !== null && _a !== void 0 ? _a : ScalarType2.INT32, field.number, msg.get(field));
      break;
    case "list":
      writeListField2(writer, opts, field, msg.get(field));
      break;
    case "message":
      writeMessageField2(writer, opts, field, msg.get(field));
      break;
    case "map":
      for (const [key, val] of msg.get(field))
        writeMapEntry2(writer, opts, field, key, val);
      break;
  }
}
function writeScalar2(writer, msgName, fieldName, scalarType, fieldNo, value) {
  writeScalarValue2(writer.tag(fieldNo, writeTypeOfScalar2(scalarType)), msgName, fieldName, scalarType, value);
}
function writeMessageField2(writer, opts, field, message) {
  if (field.delimitedEncoding)
    writeFields2(writer.tag(field.number, WireType2.StartGroup), opts, message).tag(field.number, WireType2.EndGroup);
  else
    writeFields2(writer.tag(field.number, WireType2.LengthDelimited).fork(), opts, message).join();
}
function writeListField2(writer, opts, field, list) {
  var _a;
  if (field.listKind == "message") {
    for (const item of list)
      writeMessageField2(writer, opts, field, item);
    return;
  }
  const scalarType = (_a = field.scalar) !== null && _a !== void 0 ? _a : ScalarType2.INT32;
  if (field.packed) {
    if (!list.size)
      return;
    writer.tag(field.number, WireType2.LengthDelimited).fork();
    for (const item of list)
      writeScalarValue2(writer, field.parent.typeName, field.name, scalarType, item);
    writer.join();
    return;
  }
  for (const item of list)
    writeScalar2(writer, field.parent.typeName, field.name, scalarType, field.number, item);
}
function writeMapEntry2(writer, opts, field, key, value) {
  var _a;
  writer.tag(field.number, WireType2.LengthDelimited).fork();
  writeScalar2(writer, field.parent.typeName, field.name, field.mapKey, 1, key);
  switch (field.mapKind) {
    case "scalar":
    case "enum":
      writeScalar2(writer, field.parent.typeName, field.name, (_a = field.scalar) !== null && _a !== void 0 ? _a : ScalarType2.INT32, 2, value);
      break;
    case "message":
      writeFields2(writer.tag(2, WireType2.LengthDelimited).fork(), opts, value).join();
      break;
  }
  writer.join();
}
function writeScalarValue2(writer, msgName, fieldName, type, value) {
  try {
    switch (type) {
      case ScalarType2.STRING:
        writer.string(value);
        break;
      case ScalarType2.BOOL:
        writer.bool(value);
        break;
      case ScalarType2.DOUBLE:
        writer.double(value);
        break;
      case ScalarType2.FLOAT:
        writer.float(value);
        break;
      case ScalarType2.INT32:
        writer.int32(value);
        break;
      case ScalarType2.INT64:
        writer.int64(value);
        break;
      case ScalarType2.UINT64:
        writer.uint64(value);
        break;
      case ScalarType2.FIXED64:
        writer.fixed64(value);
        break;
      case ScalarType2.BYTES:
        writer.bytes(value);
        break;
      case ScalarType2.FIXED32:
        writer.fixed32(value);
        break;
      case ScalarType2.SFIXED32:
        writer.sfixed32(value);
        break;
      case ScalarType2.SFIXED64:
        writer.sfixed64(value);
        break;
      case ScalarType2.SINT64:
        writer.sint64(value);
        break;
      case ScalarType2.UINT32:
        writer.uint32(value);
        break;
      case ScalarType2.SINT32:
        writer.sint32(value);
        break;
    }
  } catch (e) {
    if (e instanceof Error)
      throw new Error(`cannot encode field ${msgName}.${fieldName} to binary: ${e.message}`);
    throw e;
  }
}
function writeTypeOfScalar2(type) {
  switch (type) {
    case ScalarType2.BYTES:
    case ScalarType2.STRING:
      return WireType2.LengthDelimited;
    case ScalarType2.DOUBLE:
    case ScalarType2.FIXED64:
    case ScalarType2.SFIXED64:
      return WireType2.Bit64;
    case ScalarType2.FIXED32:
    case ScalarType2.SFIXED32:
    case ScalarType2.FLOAT:
      return WireType2.Bit32;
    default:
      return WireType2.Varint;
  }
}
function messageDesc$1(file, path, ...paths) {
  return paths.reduce((acc, cur) => acc.nestedMessages[cur], file.messages[path]);
}
var file_google_protobuf_descriptor = /* @__PURE__ */ boot({
  "name": "google/protobuf/descriptor.proto",
  "package": "google.protobuf",
  "messageType": [
    {
      "name": "FileDescriptorSet",
      "field": [{
        "name": "file",
        "number": 1,
        "type": 11,
        "label": 3,
        "typeName": ".google.protobuf.FileDescriptorProto"
      }],
      "extensionRange": [{
        "start": 536e6,
        "end": 536000001
      }]
    },
    {
      "name": "FileDescriptorProto",
      "field": [
        {
          "name": "name",
          "number": 1,
          "type": 9,
          "label": 1
        },
        {
          "name": "package",
          "number": 2,
          "type": 9,
          "label": 1
        },
        {
          "name": "dependency",
          "number": 3,
          "type": 9,
          "label": 3
        },
        {
          "name": "public_dependency",
          "number": 10,
          "type": 5,
          "label": 3
        },
        {
          "name": "weak_dependency",
          "number": 11,
          "type": 5,
          "label": 3
        },
        {
          "name": "option_dependency",
          "number": 15,
          "type": 9,
          "label": 3
        },
        {
          "name": "message_type",
          "number": 4,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.DescriptorProto"
        },
        {
          "name": "enum_type",
          "number": 5,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.EnumDescriptorProto"
        },
        {
          "name": "service",
          "number": 6,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.ServiceDescriptorProto"
        },
        {
          "name": "extension",
          "number": 7,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.FieldDescriptorProto"
        },
        {
          "name": "options",
          "number": 8,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.FileOptions"
        },
        {
          "name": "source_code_info",
          "number": 9,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.SourceCodeInfo"
        },
        {
          "name": "syntax",
          "number": 12,
          "type": 9,
          "label": 1
        },
        {
          "name": "edition",
          "number": 14,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.Edition"
        }
      ]
    },
    {
      "name": "DescriptorProto",
      "field": [
        {
          "name": "name",
          "number": 1,
          "type": 9,
          "label": 1
        },
        {
          "name": "field",
          "number": 2,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.FieldDescriptorProto"
        },
        {
          "name": "extension",
          "number": 6,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.FieldDescriptorProto"
        },
        {
          "name": "nested_type",
          "number": 3,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.DescriptorProto"
        },
        {
          "name": "enum_type",
          "number": 4,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.EnumDescriptorProto"
        },
        {
          "name": "extension_range",
          "number": 5,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.DescriptorProto.ExtensionRange"
        },
        {
          "name": "oneof_decl",
          "number": 8,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.OneofDescriptorProto"
        },
        {
          "name": "options",
          "number": 7,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.MessageOptions"
        },
        {
          "name": "reserved_range",
          "number": 9,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.DescriptorProto.ReservedRange"
        },
        {
          "name": "reserved_name",
          "number": 10,
          "type": 9,
          "label": 3
        },
        {
          "name": "visibility",
          "number": 11,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.SymbolVisibility"
        }
      ],
      "nestedType": [{
        "name": "ExtensionRange",
        "field": [
          {
            "name": "start",
            "number": 1,
            "type": 5,
            "label": 1
          },
          {
            "name": "end",
            "number": 2,
            "type": 5,
            "label": 1
          },
          {
            "name": "options",
            "number": 3,
            "type": 11,
            "label": 1,
            "typeName": ".google.protobuf.ExtensionRangeOptions"
          }
        ]
      }, {
        "name": "ReservedRange",
        "field": [{
          "name": "start",
          "number": 1,
          "type": 5,
          "label": 1
        }, {
          "name": "end",
          "number": 2,
          "type": 5,
          "label": 1
        }]
      }]
    },
    {
      "name": "ExtensionRangeOptions",
      "field": [
        {
          "name": "uninterpreted_option",
          "number": 999,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.UninterpretedOption"
        },
        {
          "name": "declaration",
          "number": 2,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.ExtensionRangeOptions.Declaration",
          "options": { "retention": 2 }
        },
        {
          "name": "features",
          "number": 50,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet"
        },
        {
          "name": "verification",
          "number": 3,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.ExtensionRangeOptions.VerificationState",
          "defaultValue": "UNVERIFIED",
          "options": { "retention": 2 }
        }
      ],
      "nestedType": [{
        "name": "Declaration",
        "field": [
          {
            "name": "number",
            "number": 1,
            "type": 5,
            "label": 1
          },
          {
            "name": "full_name",
            "number": 2,
            "type": 9,
            "label": 1
          },
          {
            "name": "type",
            "number": 3,
            "type": 9,
            "label": 1
          },
          {
            "name": "reserved",
            "number": 5,
            "type": 8,
            "label": 1
          },
          {
            "name": "repeated",
            "number": 6,
            "type": 8,
            "label": 1
          }
        ]
      }],
      "enumType": [{
        "name": "VerificationState",
        "value": [{
          "name": "DECLARATION",
          "number": 0
        }, {
          "name": "UNVERIFIED",
          "number": 1
        }]
      }],
      "extensionRange": [{
        "start": 1e3,
        "end": 536870912
      }]
    },
    {
      "name": "FieldDescriptorProto",
      "field": [
        {
          "name": "name",
          "number": 1,
          "type": 9,
          "label": 1
        },
        {
          "name": "number",
          "number": 3,
          "type": 5,
          "label": 1
        },
        {
          "name": "label",
          "number": 4,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FieldDescriptorProto.Label"
        },
        {
          "name": "type",
          "number": 5,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FieldDescriptorProto.Type"
        },
        {
          "name": "type_name",
          "number": 6,
          "type": 9,
          "label": 1
        },
        {
          "name": "extendee",
          "number": 2,
          "type": 9,
          "label": 1
        },
        {
          "name": "default_value",
          "number": 7,
          "type": 9,
          "label": 1
        },
        {
          "name": "oneof_index",
          "number": 9,
          "type": 5,
          "label": 1
        },
        {
          "name": "json_name",
          "number": 10,
          "type": 9,
          "label": 1
        },
        {
          "name": "options",
          "number": 8,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.FieldOptions"
        },
        {
          "name": "proto3_optional",
          "number": 17,
          "type": 8,
          "label": 1
        }
      ],
      "enumType": [{
        "name": "Type",
        "value": [
          {
            "name": "TYPE_DOUBLE",
            "number": 1
          },
          {
            "name": "TYPE_FLOAT",
            "number": 2
          },
          {
            "name": "TYPE_INT64",
            "number": 3
          },
          {
            "name": "TYPE_UINT64",
            "number": 4
          },
          {
            "name": "TYPE_INT32",
            "number": 5
          },
          {
            "name": "TYPE_FIXED64",
            "number": 6
          },
          {
            "name": "TYPE_FIXED32",
            "number": 7
          },
          {
            "name": "TYPE_BOOL",
            "number": 8
          },
          {
            "name": "TYPE_STRING",
            "number": 9
          },
          {
            "name": "TYPE_GROUP",
            "number": 10
          },
          {
            "name": "TYPE_MESSAGE",
            "number": 11
          },
          {
            "name": "TYPE_BYTES",
            "number": 12
          },
          {
            "name": "TYPE_UINT32",
            "number": 13
          },
          {
            "name": "TYPE_ENUM",
            "number": 14
          },
          {
            "name": "TYPE_SFIXED32",
            "number": 15
          },
          {
            "name": "TYPE_SFIXED64",
            "number": 16
          },
          {
            "name": "TYPE_SINT32",
            "number": 17
          },
          {
            "name": "TYPE_SINT64",
            "number": 18
          }
        ]
      }, {
        "name": "Label",
        "value": [
          {
            "name": "LABEL_OPTIONAL",
            "number": 1
          },
          {
            "name": "LABEL_REPEATED",
            "number": 3
          },
          {
            "name": "LABEL_REQUIRED",
            "number": 2
          }
        ]
      }]
    },
    {
      "name": "OneofDescriptorProto",
      "field": [{
        "name": "name",
        "number": 1,
        "type": 9,
        "label": 1
      }, {
        "name": "options",
        "number": 2,
        "type": 11,
        "label": 1,
        "typeName": ".google.protobuf.OneofOptions"
      }]
    },
    {
      "name": "EnumDescriptorProto",
      "field": [
        {
          "name": "name",
          "number": 1,
          "type": 9,
          "label": 1
        },
        {
          "name": "value",
          "number": 2,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.EnumValueDescriptorProto"
        },
        {
          "name": "options",
          "number": 3,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.EnumOptions"
        },
        {
          "name": "reserved_range",
          "number": 4,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.EnumDescriptorProto.EnumReservedRange"
        },
        {
          "name": "reserved_name",
          "number": 5,
          "type": 9,
          "label": 3
        },
        {
          "name": "visibility",
          "number": 6,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.SymbolVisibility"
        }
      ],
      "nestedType": [{
        "name": "EnumReservedRange",
        "field": [{
          "name": "start",
          "number": 1,
          "type": 5,
          "label": 1
        }, {
          "name": "end",
          "number": 2,
          "type": 5,
          "label": 1
        }]
      }]
    },
    {
      "name": "EnumValueDescriptorProto",
      "field": [
        {
          "name": "name",
          "number": 1,
          "type": 9,
          "label": 1
        },
        {
          "name": "number",
          "number": 2,
          "type": 5,
          "label": 1
        },
        {
          "name": "options",
          "number": 3,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.EnumValueOptions"
        }
      ]
    },
    {
      "name": "ServiceDescriptorProto",
      "field": [
        {
          "name": "name",
          "number": 1,
          "type": 9,
          "label": 1
        },
        {
          "name": "method",
          "number": 2,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.MethodDescriptorProto"
        },
        {
          "name": "options",
          "number": 3,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.ServiceOptions"
        }
      ]
    },
    {
      "name": "MethodDescriptorProto",
      "field": [
        {
          "name": "name",
          "number": 1,
          "type": 9,
          "label": 1
        },
        {
          "name": "input_type",
          "number": 2,
          "type": 9,
          "label": 1
        },
        {
          "name": "output_type",
          "number": 3,
          "type": 9,
          "label": 1
        },
        {
          "name": "options",
          "number": 4,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.MethodOptions"
        },
        {
          "name": "client_streaming",
          "number": 5,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "server_streaming",
          "number": 6,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        }
      ]
    },
    {
      "name": "FileOptions",
      "field": [
        {
          "name": "java_package",
          "number": 1,
          "type": 9,
          "label": 1
        },
        {
          "name": "java_outer_classname",
          "number": 8,
          "type": 9,
          "label": 1
        },
        {
          "name": "java_multiple_files",
          "number": 10,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "java_generate_equals_and_hash",
          "number": 20,
          "type": 8,
          "label": 1,
          "options": { "deprecated": true }
        },
        {
          "name": "java_string_check_utf8",
          "number": 27,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "optimize_for",
          "number": 9,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FileOptions.OptimizeMode",
          "defaultValue": "SPEED"
        },
        {
          "name": "go_package",
          "number": 11,
          "type": 9,
          "label": 1
        },
        {
          "name": "cc_generic_services",
          "number": 16,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "java_generic_services",
          "number": 17,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "py_generic_services",
          "number": 18,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "deprecated",
          "number": 23,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "cc_enable_arenas",
          "number": 31,
          "type": 8,
          "label": 1,
          "defaultValue": "true"
        },
        {
          "name": "objc_class_prefix",
          "number": 36,
          "type": 9,
          "label": 1
        },
        {
          "name": "csharp_namespace",
          "number": 37,
          "type": 9,
          "label": 1
        },
        {
          "name": "swift_prefix",
          "number": 39,
          "type": 9,
          "label": 1
        },
        {
          "name": "php_class_prefix",
          "number": 40,
          "type": 9,
          "label": 1
        },
        {
          "name": "php_namespace",
          "number": 41,
          "type": 9,
          "label": 1
        },
        {
          "name": "php_metadata_namespace",
          "number": 44,
          "type": 9,
          "label": 1
        },
        {
          "name": "ruby_package",
          "number": 45,
          "type": 9,
          "label": 1
        },
        {
          "name": "features",
          "number": 50,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet"
        },
        {
          "name": "uninterpreted_option",
          "number": 999,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.UninterpretedOption"
        }
      ],
      "enumType": [{
        "name": "OptimizeMode",
        "value": [
          {
            "name": "SPEED",
            "number": 1
          },
          {
            "name": "CODE_SIZE",
            "number": 2
          },
          {
            "name": "LITE_RUNTIME",
            "number": 3
          }
        ]
      }],
      "extensionRange": [{
        "start": 1e3,
        "end": 536870912
      }]
    },
    {
      "name": "MessageOptions",
      "field": [
        {
          "name": "message_set_wire_format",
          "number": 1,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "no_standard_descriptor_accessor",
          "number": 2,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "deprecated",
          "number": 3,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "map_entry",
          "number": 7,
          "type": 8,
          "label": 1
        },
        {
          "name": "deprecated_legacy_json_field_conflicts",
          "number": 11,
          "type": 8,
          "label": 1,
          "options": { "deprecated": true }
        },
        {
          "name": "features",
          "number": 12,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet"
        },
        {
          "name": "uninterpreted_option",
          "number": 999,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.UninterpretedOption"
        }
      ],
      "extensionRange": [{
        "start": 1e3,
        "end": 536870912
      }]
    },
    {
      "name": "FieldOptions",
      "field": [
        {
          "name": "ctype",
          "number": 1,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FieldOptions.CType",
          "defaultValue": "STRING"
        },
        {
          "name": "packed",
          "number": 2,
          "type": 8,
          "label": 1
        },
        {
          "name": "jstype",
          "number": 6,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FieldOptions.JSType",
          "defaultValue": "JS_NORMAL"
        },
        {
          "name": "lazy",
          "number": 5,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "unverified_lazy",
          "number": 15,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "deprecated",
          "number": 3,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "weak",
          "number": 10,
          "type": 8,
          "label": 1,
          "defaultValue": "false",
          "options": { "deprecated": true }
        },
        {
          "name": "debug_redact",
          "number": 16,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "retention",
          "number": 17,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FieldOptions.OptionRetention"
        },
        {
          "name": "targets",
          "number": 19,
          "type": 14,
          "label": 3,
          "typeName": ".google.protobuf.FieldOptions.OptionTargetType"
        },
        {
          "name": "edition_defaults",
          "number": 20,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.FieldOptions.EditionDefault"
        },
        {
          "name": "features",
          "number": 21,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet"
        },
        {
          "name": "feature_support",
          "number": 22,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.FieldOptions.FeatureSupport"
        },
        {
          "name": "uninterpreted_option",
          "number": 999,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.UninterpretedOption"
        }
      ],
      "nestedType": [{
        "name": "EditionDefault",
        "field": [{
          "name": "edition",
          "number": 3,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.Edition"
        }, {
          "name": "value",
          "number": 2,
          "type": 9,
          "label": 1
        }]
      }, {
        "name": "FeatureSupport",
        "field": [
          {
            "name": "edition_introduced",
            "number": 1,
            "type": 14,
            "label": 1,
            "typeName": ".google.protobuf.Edition"
          },
          {
            "name": "edition_deprecated",
            "number": 2,
            "type": 14,
            "label": 1,
            "typeName": ".google.protobuf.Edition"
          },
          {
            "name": "deprecation_warning",
            "number": 3,
            "type": 9,
            "label": 1
          },
          {
            "name": "edition_removed",
            "number": 4,
            "type": 14,
            "label": 1,
            "typeName": ".google.protobuf.Edition"
          }
        ]
      }],
      "enumType": [
        {
          "name": "CType",
          "value": [
            {
              "name": "STRING",
              "number": 0
            },
            {
              "name": "CORD",
              "number": 1
            },
            {
              "name": "STRING_PIECE",
              "number": 2
            }
          ]
        },
        {
          "name": "JSType",
          "value": [
            {
              "name": "JS_NORMAL",
              "number": 0
            },
            {
              "name": "JS_STRING",
              "number": 1
            },
            {
              "name": "JS_NUMBER",
              "number": 2
            }
          ]
        },
        {
          "name": "OptionRetention",
          "value": [
            {
              "name": "RETENTION_UNKNOWN",
              "number": 0
            },
            {
              "name": "RETENTION_RUNTIME",
              "number": 1
            },
            {
              "name": "RETENTION_SOURCE",
              "number": 2
            }
          ]
        },
        {
          "name": "OptionTargetType",
          "value": [
            {
              "name": "TARGET_TYPE_UNKNOWN",
              "number": 0
            },
            {
              "name": "TARGET_TYPE_FILE",
              "number": 1
            },
            {
              "name": "TARGET_TYPE_EXTENSION_RANGE",
              "number": 2
            },
            {
              "name": "TARGET_TYPE_MESSAGE",
              "number": 3
            },
            {
              "name": "TARGET_TYPE_FIELD",
              "number": 4
            },
            {
              "name": "TARGET_TYPE_ONEOF",
              "number": 5
            },
            {
              "name": "TARGET_TYPE_ENUM",
              "number": 6
            },
            {
              "name": "TARGET_TYPE_ENUM_ENTRY",
              "number": 7
            },
            {
              "name": "TARGET_TYPE_SERVICE",
              "number": 8
            },
            {
              "name": "TARGET_TYPE_METHOD",
              "number": 9
            }
          ]
        }
      ],
      "extensionRange": [{
        "start": 1e3,
        "end": 536870912
      }]
    },
    {
      "name": "OneofOptions",
      "field": [{
        "name": "features",
        "number": 1,
        "type": 11,
        "label": 1,
        "typeName": ".google.protobuf.FeatureSet"
      }, {
        "name": "uninterpreted_option",
        "number": 999,
        "type": 11,
        "label": 3,
        "typeName": ".google.protobuf.UninterpretedOption"
      }],
      "extensionRange": [{
        "start": 1e3,
        "end": 536870912
      }]
    },
    {
      "name": "EnumOptions",
      "field": [
        {
          "name": "allow_alias",
          "number": 2,
          "type": 8,
          "label": 1
        },
        {
          "name": "deprecated",
          "number": 3,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "deprecated_legacy_json_field_conflicts",
          "number": 6,
          "type": 8,
          "label": 1,
          "options": { "deprecated": true }
        },
        {
          "name": "features",
          "number": 7,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet"
        },
        {
          "name": "uninterpreted_option",
          "number": 999,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.UninterpretedOption"
        }
      ],
      "extensionRange": [{
        "start": 1e3,
        "end": 536870912
      }]
    },
    {
      "name": "EnumValueOptions",
      "field": [
        {
          "name": "deprecated",
          "number": 1,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "features",
          "number": 2,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet"
        },
        {
          "name": "debug_redact",
          "number": 3,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "feature_support",
          "number": 4,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.FieldOptions.FeatureSupport"
        },
        {
          "name": "uninterpreted_option",
          "number": 999,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.UninterpretedOption"
        }
      ],
      "extensionRange": [{
        "start": 1e3,
        "end": 536870912
      }]
    },
    {
      "name": "ServiceOptions",
      "field": [
        {
          "name": "features",
          "number": 34,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet"
        },
        {
          "name": "deprecated",
          "number": 33,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "uninterpreted_option",
          "number": 999,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.UninterpretedOption"
        }
      ],
      "extensionRange": [{
        "start": 1e3,
        "end": 536870912
      }]
    },
    {
      "name": "MethodOptions",
      "field": [
        {
          "name": "deprecated",
          "number": 33,
          "type": 8,
          "label": 1,
          "defaultValue": "false"
        },
        {
          "name": "idempotency_level",
          "number": 34,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.MethodOptions.IdempotencyLevel",
          "defaultValue": "IDEMPOTENCY_UNKNOWN"
        },
        {
          "name": "features",
          "number": 35,
          "type": 11,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet"
        },
        {
          "name": "uninterpreted_option",
          "number": 999,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.UninterpretedOption"
        }
      ],
      "enumType": [{
        "name": "IdempotencyLevel",
        "value": [
          {
            "name": "IDEMPOTENCY_UNKNOWN",
            "number": 0
          },
          {
            "name": "NO_SIDE_EFFECTS",
            "number": 1
          },
          {
            "name": "IDEMPOTENT",
            "number": 2
          }
        ]
      }],
      "extensionRange": [{
        "start": 1e3,
        "end": 536870912
      }]
    },
    {
      "name": "UninterpretedOption",
      "field": [
        {
          "name": "name",
          "number": 2,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.UninterpretedOption.NamePart"
        },
        {
          "name": "identifier_value",
          "number": 3,
          "type": 9,
          "label": 1
        },
        {
          "name": "positive_int_value",
          "number": 4,
          "type": 4,
          "label": 1
        },
        {
          "name": "negative_int_value",
          "number": 5,
          "type": 3,
          "label": 1
        },
        {
          "name": "double_value",
          "number": 6,
          "type": 1,
          "label": 1
        },
        {
          "name": "string_value",
          "number": 7,
          "type": 12,
          "label": 1
        },
        {
          "name": "aggregate_value",
          "number": 8,
          "type": 9,
          "label": 1
        }
      ],
      "nestedType": [{
        "name": "NamePart",
        "field": [{
          "name": "name_part",
          "number": 1,
          "type": 9,
          "label": 2
        }, {
          "name": "is_extension",
          "number": 2,
          "type": 8,
          "label": 2
        }]
      }]
    },
    {
      "name": "FeatureSet",
      "field": [
        {
          "name": "field_presence",
          "number": 1,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet.FieldPresence",
          "options": {
            "retention": 1,
            "targets": [4, 1],
            "editionDefaults": [
              {
                "value": "EXPLICIT",
                "edition": 900
              },
              {
                "value": "IMPLICIT",
                "edition": 999
              },
              {
                "value": "EXPLICIT",
                "edition": 1e3
              }
            ]
          }
        },
        {
          "name": "enum_type",
          "number": 2,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet.EnumType",
          "options": {
            "retention": 1,
            "targets": [6, 1],
            "editionDefaults": [{
              "value": "CLOSED",
              "edition": 900
            }, {
              "value": "OPEN",
              "edition": 999
            }]
          }
        },
        {
          "name": "repeated_field_encoding",
          "number": 3,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet.RepeatedFieldEncoding",
          "options": {
            "retention": 1,
            "targets": [4, 1],
            "editionDefaults": [{
              "value": "EXPANDED",
              "edition": 900
            }, {
              "value": "PACKED",
              "edition": 999
            }]
          }
        },
        {
          "name": "utf8_validation",
          "number": 4,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet.Utf8Validation",
          "options": {
            "retention": 1,
            "targets": [4, 1],
            "editionDefaults": [{
              "value": "NONE",
              "edition": 900
            }, {
              "value": "VERIFY",
              "edition": 999
            }]
          }
        },
        {
          "name": "message_encoding",
          "number": 5,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet.MessageEncoding",
          "options": {
            "retention": 1,
            "targets": [4, 1],
            "editionDefaults": [{
              "value": "LENGTH_PREFIXED",
              "edition": 900
            }]
          }
        },
        {
          "name": "json_format",
          "number": 6,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet.JsonFormat",
          "options": {
            "retention": 1,
            "targets": [
              3,
              6,
              1
            ],
            "editionDefaults": [{
              "value": "LEGACY_BEST_EFFORT",
              "edition": 900
            }, {
              "value": "ALLOW",
              "edition": 999
            }]
          }
        },
        {
          "name": "enforce_naming_style",
          "number": 7,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet.EnforceNamingStyle",
          "options": {
            "retention": 2,
            "targets": [
              1,
              2,
              3,
              4,
              5,
              6,
              7,
              8,
              9
            ],
            "editionDefaults": [{
              "value": "STYLE_LEGACY",
              "edition": 900
            }, {
              "value": "STYLE2024",
              "edition": 1001
            }]
          }
        },
        {
          "name": "default_symbol_visibility",
          "number": 8,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.FeatureSet.VisibilityFeature.DefaultSymbolVisibility",
          "options": {
            "retention": 2,
            "targets": [1],
            "editionDefaults": [{
              "value": "EXPORT_ALL",
              "edition": 900
            }, {
              "value": "EXPORT_TOP_LEVEL",
              "edition": 1001
            }]
          }
        }
      ],
      "nestedType": [{
        "name": "VisibilityFeature",
        "enumType": [{
          "name": "DefaultSymbolVisibility",
          "value": [
            {
              "name": "DEFAULT_SYMBOL_VISIBILITY_UNKNOWN",
              "number": 0
            },
            {
              "name": "EXPORT_ALL",
              "number": 1
            },
            {
              "name": "EXPORT_TOP_LEVEL",
              "number": 2
            },
            {
              "name": "LOCAL_ALL",
              "number": 3
            },
            {
              "name": "STRICT",
              "number": 4
            }
          ]
        }]
      }],
      "enumType": [
        {
          "name": "FieldPresence",
          "value": [
            {
              "name": "FIELD_PRESENCE_UNKNOWN",
              "number": 0
            },
            {
              "name": "EXPLICIT",
              "number": 1
            },
            {
              "name": "IMPLICIT",
              "number": 2
            },
            {
              "name": "LEGACY_REQUIRED",
              "number": 3
            }
          ]
        },
        {
          "name": "EnumType",
          "value": [
            {
              "name": "ENUM_TYPE_UNKNOWN",
              "number": 0
            },
            {
              "name": "OPEN",
              "number": 1
            },
            {
              "name": "CLOSED",
              "number": 2
            }
          ]
        },
        {
          "name": "RepeatedFieldEncoding",
          "value": [
            {
              "name": "REPEATED_FIELD_ENCODING_UNKNOWN",
              "number": 0
            },
            {
              "name": "PACKED",
              "number": 1
            },
            {
              "name": "EXPANDED",
              "number": 2
            }
          ]
        },
        {
          "name": "Utf8Validation",
          "value": [
            {
              "name": "UTF8_VALIDATION_UNKNOWN",
              "number": 0
            },
            {
              "name": "VERIFY",
              "number": 2
            },
            {
              "name": "NONE",
              "number": 3
            }
          ]
        },
        {
          "name": "MessageEncoding",
          "value": [
            {
              "name": "MESSAGE_ENCODING_UNKNOWN",
              "number": 0
            },
            {
              "name": "LENGTH_PREFIXED",
              "number": 1
            },
            {
              "name": "DELIMITED",
              "number": 2
            }
          ]
        },
        {
          "name": "JsonFormat",
          "value": [
            {
              "name": "JSON_FORMAT_UNKNOWN",
              "number": 0
            },
            {
              "name": "ALLOW",
              "number": 1
            },
            {
              "name": "LEGACY_BEST_EFFORT",
              "number": 2
            }
          ]
        },
        {
          "name": "EnforceNamingStyle",
          "value": [
            {
              "name": "ENFORCE_NAMING_STYLE_UNKNOWN",
              "number": 0
            },
            {
              "name": "STYLE2024",
              "number": 1
            },
            {
              "name": "STYLE_LEGACY",
              "number": 2
            }
          ]
        }
      ],
      "extensionRange": [
        {
          "start": 1e3,
          "end": 9995
        },
        {
          "start": 9995,
          "end": 1e4
        },
        {
          "start": 1e4,
          "end": 10001
        }
      ]
    },
    {
      "name": "FeatureSetDefaults",
      "field": [
        {
          "name": "defaults",
          "number": 1,
          "type": 11,
          "label": 3,
          "typeName": ".google.protobuf.FeatureSetDefaults.FeatureSetEditionDefault"
        },
        {
          "name": "minimum_edition",
          "number": 4,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.Edition"
        },
        {
          "name": "maximum_edition",
          "number": 5,
          "type": 14,
          "label": 1,
          "typeName": ".google.protobuf.Edition"
        }
      ],
      "nestedType": [{
        "name": "FeatureSetEditionDefault",
        "field": [
          {
            "name": "edition",
            "number": 3,
            "type": 14,
            "label": 1,
            "typeName": ".google.protobuf.Edition"
          },
          {
            "name": "overridable_features",
            "number": 4,
            "type": 11,
            "label": 1,
            "typeName": ".google.protobuf.FeatureSet"
          },
          {
            "name": "fixed_features",
            "number": 5,
            "type": 11,
            "label": 1,
            "typeName": ".google.protobuf.FeatureSet"
          }
        ]
      }]
    },
    {
      "name": "SourceCodeInfo",
      "field": [{
        "name": "location",
        "number": 1,
        "type": 11,
        "label": 3,
        "typeName": ".google.protobuf.SourceCodeInfo.Location"
      }],
      "nestedType": [{
        "name": "Location",
        "field": [
          {
            "name": "path",
            "number": 1,
            "type": 5,
            "label": 3,
            "options": { "packed": true }
          },
          {
            "name": "span",
            "number": 2,
            "type": 5,
            "label": 3,
            "options": { "packed": true }
          },
          {
            "name": "leading_comments",
            "number": 3,
            "type": 9,
            "label": 1
          },
          {
            "name": "trailing_comments",
            "number": 4,
            "type": 9,
            "label": 1
          },
          {
            "name": "leading_detached_comments",
            "number": 6,
            "type": 9,
            "label": 3
          }
        ]
      }],
      "extensionRange": [{
        "start": 536e6,
        "end": 536000001
      }]
    },
    {
      "name": "GeneratedCodeInfo",
      "field": [{
        "name": "annotation",
        "number": 1,
        "type": 11,
        "label": 3,
        "typeName": ".google.protobuf.GeneratedCodeInfo.Annotation"
      }],
      "nestedType": [{
        "name": "Annotation",
        "field": [
          {
            "name": "path",
            "number": 1,
            "type": 5,
            "label": 3,
            "options": { "packed": true }
          },
          {
            "name": "source_file",
            "number": 2,
            "type": 9,
            "label": 1
          },
          {
            "name": "begin",
            "number": 3,
            "type": 5,
            "label": 1
          },
          {
            "name": "end",
            "number": 4,
            "type": 5,
            "label": 1
          },
          {
            "name": "semantic",
            "number": 5,
            "type": 14,
            "label": 1,
            "typeName": ".google.protobuf.GeneratedCodeInfo.Annotation.Semantic"
          }
        ],
        "enumType": [{
          "name": "Semantic",
          "value": [
            {
              "name": "NONE",
              "number": 0
            },
            {
              "name": "SET",
              "number": 1
            },
            {
              "name": "ALIAS",
              "number": 2
            }
          ]
        }]
      }]
    }
  ],
  "enumType": [{
    "name": "Edition",
    "value": [
      {
        "name": "EDITION_UNKNOWN",
        "number": 0
      },
      {
        "name": "EDITION_LEGACY",
        "number": 900
      },
      {
        "name": "EDITION_PROTO2",
        "number": 998
      },
      {
        "name": "EDITION_PROTO3",
        "number": 999
      },
      {
        "name": "EDITION_2023",
        "number": 1e3
      },
      {
        "name": "EDITION_2024",
        "number": 1001
      },
      {
        "name": "EDITION_1_TEST_ONLY",
        "number": 1
      },
      {
        "name": "EDITION_2_TEST_ONLY",
        "number": 2
      },
      {
        "name": "EDITION_99997_TEST_ONLY",
        "number": 99997
      },
      {
        "name": "EDITION_99998_TEST_ONLY",
        "number": 99998
      },
      {
        "name": "EDITION_99999_TEST_ONLY",
        "number": 99999
      },
      {
        "name": "EDITION_MAX",
        "number": 2147483647
      }
    ]
  }, {
    "name": "SymbolVisibility",
    "value": [
      {
        "name": "VISIBILITY_UNSET",
        "number": 0
      },
      {
        "name": "VISIBILITY_LOCAL",
        "number": 1
      },
      {
        "name": "VISIBILITY_EXPORT",
        "number": 2
      }
    ]
  }]
});
var FileDescriptorProtoSchema = /* @__PURE__ */ messageDesc$1(file_google_protobuf_descriptor, 1);
var ExtensionRangeOptions_VerificationState;
(function(ExtensionRangeOptions_VerificationState$1) {
  ExtensionRangeOptions_VerificationState$1[ExtensionRangeOptions_VerificationState$1["DECLARATION"] = 0] = "DECLARATION";
  ExtensionRangeOptions_VerificationState$1[ExtensionRangeOptions_VerificationState$1["UNVERIFIED"] = 1] = "UNVERIFIED";
})(ExtensionRangeOptions_VerificationState || (ExtensionRangeOptions_VerificationState = {}));
var FieldDescriptorProto_Type;
(function(FieldDescriptorProto_Type$1) {
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["DOUBLE"] = 1] = "DOUBLE";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["FLOAT"] = 2] = "FLOAT";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["INT64"] = 3] = "INT64";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["UINT64"] = 4] = "UINT64";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["INT32"] = 5] = "INT32";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["FIXED64"] = 6] = "FIXED64";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["FIXED32"] = 7] = "FIXED32";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["BOOL"] = 8] = "BOOL";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["STRING"] = 9] = "STRING";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["GROUP"] = 10] = "GROUP";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["MESSAGE"] = 11] = "MESSAGE";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["BYTES"] = 12] = "BYTES";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["UINT32"] = 13] = "UINT32";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["ENUM"] = 14] = "ENUM";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["SFIXED32"] = 15] = "SFIXED32";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["SFIXED64"] = 16] = "SFIXED64";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["SINT32"] = 17] = "SINT32";
  FieldDescriptorProto_Type$1[FieldDescriptorProto_Type$1["SINT64"] = 18] = "SINT64";
})(FieldDescriptorProto_Type || (FieldDescriptorProto_Type = {}));
var FieldDescriptorProto_Label;
(function(FieldDescriptorProto_Label$1) {
  FieldDescriptorProto_Label$1[FieldDescriptorProto_Label$1["OPTIONAL"] = 1] = "OPTIONAL";
  FieldDescriptorProto_Label$1[FieldDescriptorProto_Label$1["REPEATED"] = 3] = "REPEATED";
  FieldDescriptorProto_Label$1[FieldDescriptorProto_Label$1["REQUIRED"] = 2] = "REQUIRED";
})(FieldDescriptorProto_Label || (FieldDescriptorProto_Label = {}));
var FileOptions_OptimizeMode;
(function(FileOptions_OptimizeMode$1) {
  FileOptions_OptimizeMode$1[FileOptions_OptimizeMode$1["SPEED"] = 1] = "SPEED";
  FileOptions_OptimizeMode$1[FileOptions_OptimizeMode$1["CODE_SIZE"] = 2] = "CODE_SIZE";
  FileOptions_OptimizeMode$1[FileOptions_OptimizeMode$1["LITE_RUNTIME"] = 3] = "LITE_RUNTIME";
})(FileOptions_OptimizeMode || (FileOptions_OptimizeMode = {}));
var FieldOptions_CType;
(function(FieldOptions_CType$1) {
  FieldOptions_CType$1[FieldOptions_CType$1["STRING"] = 0] = "STRING";
  FieldOptions_CType$1[FieldOptions_CType$1["CORD"] = 1] = "CORD";
  FieldOptions_CType$1[FieldOptions_CType$1["STRING_PIECE"] = 2] = "STRING_PIECE";
})(FieldOptions_CType || (FieldOptions_CType = {}));
var FieldOptions_JSType;
(function(FieldOptions_JSType$1) {
  FieldOptions_JSType$1[FieldOptions_JSType$1["JS_NORMAL"] = 0] = "JS_NORMAL";
  FieldOptions_JSType$1[FieldOptions_JSType$1["JS_STRING"] = 1] = "JS_STRING";
  FieldOptions_JSType$1[FieldOptions_JSType$1["JS_NUMBER"] = 2] = "JS_NUMBER";
})(FieldOptions_JSType || (FieldOptions_JSType = {}));
var FieldOptions_OptionRetention;
(function(FieldOptions_OptionRetention$1) {
  FieldOptions_OptionRetention$1[FieldOptions_OptionRetention$1["RETENTION_UNKNOWN"] = 0] = "RETENTION_UNKNOWN";
  FieldOptions_OptionRetention$1[FieldOptions_OptionRetention$1["RETENTION_RUNTIME"] = 1] = "RETENTION_RUNTIME";
  FieldOptions_OptionRetention$1[FieldOptions_OptionRetention$1["RETENTION_SOURCE"] = 2] = "RETENTION_SOURCE";
})(FieldOptions_OptionRetention || (FieldOptions_OptionRetention = {}));
var FieldOptions_OptionTargetType;
(function(FieldOptions_OptionTargetType$1) {
  FieldOptions_OptionTargetType$1[FieldOptions_OptionTargetType$1["TARGET_TYPE_UNKNOWN"] = 0] = "TARGET_TYPE_UNKNOWN";
  FieldOptions_OptionTargetType$1[FieldOptions_OptionTargetType$1["TARGET_TYPE_FILE"] = 1] = "TARGET_TYPE_FILE";
  FieldOptions_OptionTargetType$1[FieldOptions_OptionTargetType$1["TARGET_TYPE_EXTENSION_RANGE"] = 2] = "TARGET_TYPE_EXTENSION_RANGE";
  FieldOptions_OptionTargetType$1[FieldOptions_OptionTargetType$1["TARGET_TYPE_MESSAGE"] = 3] = "TARGET_TYPE_MESSAGE";
  FieldOptions_OptionTargetType$1[FieldOptions_OptionTargetType$1["TARGET_TYPE_FIELD"] = 4] = "TARGET_TYPE_FIELD";
  FieldOptions_OptionTargetType$1[FieldOptions_OptionTargetType$1["TARGET_TYPE_ONEOF"] = 5] = "TARGET_TYPE_ONEOF";
  FieldOptions_OptionTargetType$1[FieldOptions_OptionTargetType$1["TARGET_TYPE_ENUM"] = 6] = "TARGET_TYPE_ENUM";
  FieldOptions_OptionTargetType$1[FieldOptions_OptionTargetType$1["TARGET_TYPE_ENUM_ENTRY"] = 7] = "TARGET_TYPE_ENUM_ENTRY";
  FieldOptions_OptionTargetType$1[FieldOptions_OptionTargetType$1["TARGET_TYPE_SERVICE"] = 8] = "TARGET_TYPE_SERVICE";
  FieldOptions_OptionTargetType$1[FieldOptions_OptionTargetType$1["TARGET_TYPE_METHOD"] = 9] = "TARGET_TYPE_METHOD";
})(FieldOptions_OptionTargetType || (FieldOptions_OptionTargetType = {}));
var MethodOptions_IdempotencyLevel;
(function(MethodOptions_IdempotencyLevel$1) {
  MethodOptions_IdempotencyLevel$1[MethodOptions_IdempotencyLevel$1["IDEMPOTENCY_UNKNOWN"] = 0] = "IDEMPOTENCY_UNKNOWN";
  MethodOptions_IdempotencyLevel$1[MethodOptions_IdempotencyLevel$1["NO_SIDE_EFFECTS"] = 1] = "NO_SIDE_EFFECTS";
  MethodOptions_IdempotencyLevel$1[MethodOptions_IdempotencyLevel$1["IDEMPOTENT"] = 2] = "IDEMPOTENT";
})(MethodOptions_IdempotencyLevel || (MethodOptions_IdempotencyLevel = {}));
var FeatureSet_VisibilityFeature_DefaultSymbolVisibility;
(function(FeatureSet_VisibilityFeature_DefaultSymbolVisibility$1) {
  FeatureSet_VisibilityFeature_DefaultSymbolVisibility$1[FeatureSet_VisibilityFeature_DefaultSymbolVisibility$1["DEFAULT_SYMBOL_VISIBILITY_UNKNOWN"] = 0] = "DEFAULT_SYMBOL_VISIBILITY_UNKNOWN";
  FeatureSet_VisibilityFeature_DefaultSymbolVisibility$1[FeatureSet_VisibilityFeature_DefaultSymbolVisibility$1["EXPORT_ALL"] = 1] = "EXPORT_ALL";
  FeatureSet_VisibilityFeature_DefaultSymbolVisibility$1[FeatureSet_VisibilityFeature_DefaultSymbolVisibility$1["EXPORT_TOP_LEVEL"] = 2] = "EXPORT_TOP_LEVEL";
  FeatureSet_VisibilityFeature_DefaultSymbolVisibility$1[FeatureSet_VisibilityFeature_DefaultSymbolVisibility$1["LOCAL_ALL"] = 3] = "LOCAL_ALL";
  FeatureSet_VisibilityFeature_DefaultSymbolVisibility$1[FeatureSet_VisibilityFeature_DefaultSymbolVisibility$1["STRICT"] = 4] = "STRICT";
})(FeatureSet_VisibilityFeature_DefaultSymbolVisibility || (FeatureSet_VisibilityFeature_DefaultSymbolVisibility = {}));
var FeatureSet_FieldPresence;
(function(FeatureSet_FieldPresence$1) {
  FeatureSet_FieldPresence$1[FeatureSet_FieldPresence$1["FIELD_PRESENCE_UNKNOWN"] = 0] = "FIELD_PRESENCE_UNKNOWN";
  FeatureSet_FieldPresence$1[FeatureSet_FieldPresence$1["EXPLICIT"] = 1] = "EXPLICIT";
  FeatureSet_FieldPresence$1[FeatureSet_FieldPresence$1["IMPLICIT"] = 2] = "IMPLICIT";
  FeatureSet_FieldPresence$1[FeatureSet_FieldPresence$1["LEGACY_REQUIRED"] = 3] = "LEGACY_REQUIRED";
})(FeatureSet_FieldPresence || (FeatureSet_FieldPresence = {}));
var FeatureSet_EnumType;
(function(FeatureSet_EnumType$1) {
  FeatureSet_EnumType$1[FeatureSet_EnumType$1["ENUM_TYPE_UNKNOWN"] = 0] = "ENUM_TYPE_UNKNOWN";
  FeatureSet_EnumType$1[FeatureSet_EnumType$1["OPEN"] = 1] = "OPEN";
  FeatureSet_EnumType$1[FeatureSet_EnumType$1["CLOSED"] = 2] = "CLOSED";
})(FeatureSet_EnumType || (FeatureSet_EnumType = {}));
var FeatureSet_RepeatedFieldEncoding;
(function(FeatureSet_RepeatedFieldEncoding$1) {
  FeatureSet_RepeatedFieldEncoding$1[FeatureSet_RepeatedFieldEncoding$1["REPEATED_FIELD_ENCODING_UNKNOWN"] = 0] = "REPEATED_FIELD_ENCODING_UNKNOWN";
  FeatureSet_RepeatedFieldEncoding$1[FeatureSet_RepeatedFieldEncoding$1["PACKED"] = 1] = "PACKED";
  FeatureSet_RepeatedFieldEncoding$1[FeatureSet_RepeatedFieldEncoding$1["EXPANDED"] = 2] = "EXPANDED";
})(FeatureSet_RepeatedFieldEncoding || (FeatureSet_RepeatedFieldEncoding = {}));
var FeatureSet_Utf8Validation;
(function(FeatureSet_Utf8Validation$1) {
  FeatureSet_Utf8Validation$1[FeatureSet_Utf8Validation$1["UTF8_VALIDATION_UNKNOWN"] = 0] = "UTF8_VALIDATION_UNKNOWN";
  FeatureSet_Utf8Validation$1[FeatureSet_Utf8Validation$1["VERIFY"] = 2] = "VERIFY";
  FeatureSet_Utf8Validation$1[FeatureSet_Utf8Validation$1["NONE"] = 3] = "NONE";
})(FeatureSet_Utf8Validation || (FeatureSet_Utf8Validation = {}));
var FeatureSet_MessageEncoding;
(function(FeatureSet_MessageEncoding$1) {
  FeatureSet_MessageEncoding$1[FeatureSet_MessageEncoding$1["MESSAGE_ENCODING_UNKNOWN"] = 0] = "MESSAGE_ENCODING_UNKNOWN";
  FeatureSet_MessageEncoding$1[FeatureSet_MessageEncoding$1["LENGTH_PREFIXED"] = 1] = "LENGTH_PREFIXED";
  FeatureSet_MessageEncoding$1[FeatureSet_MessageEncoding$1["DELIMITED"] = 2] = "DELIMITED";
})(FeatureSet_MessageEncoding || (FeatureSet_MessageEncoding = {}));
var FeatureSet_JsonFormat;
(function(FeatureSet_JsonFormat$1) {
  FeatureSet_JsonFormat$1[FeatureSet_JsonFormat$1["JSON_FORMAT_UNKNOWN"] = 0] = "JSON_FORMAT_UNKNOWN";
  FeatureSet_JsonFormat$1[FeatureSet_JsonFormat$1["ALLOW"] = 1] = "ALLOW";
  FeatureSet_JsonFormat$1[FeatureSet_JsonFormat$1["LEGACY_BEST_EFFORT"] = 2] = "LEGACY_BEST_EFFORT";
})(FeatureSet_JsonFormat || (FeatureSet_JsonFormat = {}));
var FeatureSet_EnforceNamingStyle;
(function(FeatureSet_EnforceNamingStyle$1) {
  FeatureSet_EnforceNamingStyle$1[FeatureSet_EnforceNamingStyle$1["ENFORCE_NAMING_STYLE_UNKNOWN"] = 0] = "ENFORCE_NAMING_STYLE_UNKNOWN";
  FeatureSet_EnforceNamingStyle$1[FeatureSet_EnforceNamingStyle$1["STYLE2024"] = 1] = "STYLE2024";
  FeatureSet_EnforceNamingStyle$1[FeatureSet_EnforceNamingStyle$1["STYLE_LEGACY"] = 2] = "STYLE_LEGACY";
})(FeatureSet_EnforceNamingStyle || (FeatureSet_EnforceNamingStyle = {}));
var GeneratedCodeInfo_Annotation_Semantic;
(function(GeneratedCodeInfo_Annotation_Semantic$1) {
  GeneratedCodeInfo_Annotation_Semantic$1[GeneratedCodeInfo_Annotation_Semantic$1["NONE"] = 0] = "NONE";
  GeneratedCodeInfo_Annotation_Semantic$1[GeneratedCodeInfo_Annotation_Semantic$1["SET"] = 1] = "SET";
  GeneratedCodeInfo_Annotation_Semantic$1[GeneratedCodeInfo_Annotation_Semantic$1["ALIAS"] = 2] = "ALIAS";
})(GeneratedCodeInfo_Annotation_Semantic || (GeneratedCodeInfo_Annotation_Semantic = {}));
var Edition;
(function(Edition$1) {
  Edition$1[Edition$1["EDITION_UNKNOWN"] = 0] = "EDITION_UNKNOWN";
  Edition$1[Edition$1["EDITION_LEGACY"] = 900] = "EDITION_LEGACY";
  Edition$1[Edition$1["EDITION_PROTO2"] = 998] = "EDITION_PROTO2";
  Edition$1[Edition$1["EDITION_PROTO3"] = 999] = "EDITION_PROTO3";
  Edition$1[Edition$1["EDITION_2023"] = 1e3] = "EDITION_2023";
  Edition$1[Edition$1["EDITION_2024"] = 1001] = "EDITION_2024";
  Edition$1[Edition$1["EDITION_1_TEST_ONLY"] = 1] = "EDITION_1_TEST_ONLY";
  Edition$1[Edition$1["EDITION_2_TEST_ONLY"] = 2] = "EDITION_2_TEST_ONLY";
  Edition$1[Edition$1["EDITION_99997_TEST_ONLY"] = 99997] = "EDITION_99997_TEST_ONLY";
  Edition$1[Edition$1["EDITION_99998_TEST_ONLY"] = 99998] = "EDITION_99998_TEST_ONLY";
  Edition$1[Edition$1["EDITION_99999_TEST_ONLY"] = 99999] = "EDITION_99999_TEST_ONLY";
  Edition$1[Edition$1["EDITION_MAX"] = 2147483647] = "EDITION_MAX";
})(Edition || (Edition = {}));
var SymbolVisibility;
(function(SymbolVisibility$1) {
  SymbolVisibility$1[SymbolVisibility$1["VISIBILITY_UNSET"] = 0] = "VISIBILITY_UNSET";
  SymbolVisibility$1[SymbolVisibility$1["VISIBILITY_LOCAL"] = 1] = "VISIBILITY_LOCAL";
  SymbolVisibility$1[SymbolVisibility$1["VISIBILITY_EXPORT"] = 2] = "VISIBILITY_EXPORT";
})(SymbolVisibility || (SymbolVisibility = {}));
function enumDesc(file, path, ...paths) {
  if (paths.length == 0)
    return file.enums[path];
  const e = paths.pop();
  return paths.reduce((acc, cur) => acc.nestedMessages[cur], file.messages[path]).nestedEnums[e];
}
var readDefaults2 = { readUnknownFields: true };
function makeReadOptions2(options) {
  return options ? Object.assign(Object.assign({}, readDefaults2), options) : readDefaults2;
}
function fromBinary2(schema, bytes, options) {
  const msg = reflect2(schema, void 0, false);
  readMessage2(msg, new BinaryReader2(bytes), makeReadOptions2(options), false, bytes.byteLength);
  return msg.message;
}
function readMessage2(message, reader, options, delimited, lengthOrDelimitedFieldNo) {
  var _a;
  const end = delimited ? reader.len : reader.pos + lengthOrDelimitedFieldNo;
  let fieldNo;
  let wireType;
  const unknownFields = (_a = message.getUnknown()) !== null && _a !== void 0 ? _a : [];
  while (reader.pos < end) {
    [fieldNo, wireType] = reader.tag();
    if (delimited && wireType == WireType2.EndGroup)
      break;
    const field = message.findNumber(fieldNo);
    if (!field) {
      const data = reader.skip(wireType, fieldNo);
      if (options.readUnknownFields)
        unknownFields.push({
          no: fieldNo,
          wireType,
          data
        });
      continue;
    }
    readField2(message, reader, field, wireType, options);
  }
  if (delimited) {
    if (wireType != WireType2.EndGroup || fieldNo !== lengthOrDelimitedFieldNo)
      throw new Error("invalid end group tag");
  }
  if (unknownFields.length > 0)
    message.setUnknown(unknownFields);
}
function readField2(message, reader, field, wireType, options) {
  var _a;
  switch (field.fieldKind) {
    case "scalar":
      message.set(field, readScalar2(reader, field.scalar));
      break;
    case "enum":
      const val = readScalar2(reader, ScalarType2.INT32);
      if (field.enum.open)
        message.set(field, val);
      else if (field.enum.values.some((v) => v.number === val))
        message.set(field, val);
      else if (options.readUnknownFields) {
        const bytes = [];
        varint32write2(val, bytes);
        const unknownFields = (_a = message.getUnknown()) !== null && _a !== void 0 ? _a : [];
        unknownFields.push({
          no: field.number,
          wireType,
          data: new Uint8Array(bytes)
        });
        message.setUnknown(unknownFields);
      }
      break;
    case "message":
      message.set(field, readMessageField2(reader, options, field, message.get(field)));
      break;
    case "list":
      readListField2(reader, wireType, message.get(field), options);
      break;
    case "map":
      readMapEntry2(reader, message.get(field), options);
      break;
  }
}
function readMapEntry2(reader, map, options) {
  const field = map.field();
  let key;
  let val;
  const len = reader.uint32();
  const end = reader.pos + len;
  while (reader.pos < end) {
    const [fieldNo] = reader.tag();
    switch (fieldNo) {
      case 1:
        key = readScalar2(reader, field.mapKey);
        break;
      case 2:
        switch (field.mapKind) {
          case "scalar":
            val = readScalar2(reader, field.scalar);
            break;
          case "enum":
            val = reader.int32();
            break;
          case "message":
            val = readMessageField2(reader, options, field);
            break;
        }
        break;
    }
  }
  if (key === void 0)
    key = scalarZeroValue2(field.mapKey, false);
  if (val === void 0)
    switch (field.mapKind) {
      case "scalar":
        val = scalarZeroValue2(field.scalar, false);
        break;
      case "enum":
        val = field.enum.values[0].number;
        break;
      case "message":
        val = reflect2(field.message, void 0, false);
        break;
    }
  map.set(key, val);
}
function readListField2(reader, wireType, list, options) {
  var _a;
  const field = list.field();
  if (field.listKind === "message") {
    list.add(readMessageField2(reader, options, field));
    return;
  }
  const scalarType = (_a = field.scalar) !== null && _a !== void 0 ? _a : ScalarType2.INT32;
  if (!(wireType == WireType2.LengthDelimited && scalarType != ScalarType2.STRING && scalarType != ScalarType2.BYTES)) {
    list.add(readScalar2(reader, scalarType));
    return;
  }
  const e = reader.uint32() + reader.pos;
  while (reader.pos < e)
    list.add(readScalar2(reader, scalarType));
}
function readMessageField2(reader, options, field, mergeMessage) {
  const delimited = field.delimitedEncoding;
  const message = mergeMessage !== null && mergeMessage !== void 0 ? mergeMessage : reflect2(field.message, void 0, false);
  readMessage2(message, reader, options, delimited, delimited ? field.number : reader.uint32());
  return message;
}
function readScalar2(reader, type) {
  switch (type) {
    case ScalarType2.STRING:
      return reader.string();
    case ScalarType2.BOOL:
      return reader.bool();
    case ScalarType2.DOUBLE:
      return reader.double();
    case ScalarType2.FLOAT:
      return reader.float();
    case ScalarType2.INT32:
      return reader.int32();
    case ScalarType2.INT64:
      return reader.int64();
    case ScalarType2.UINT64:
      return reader.uint64();
    case ScalarType2.FIXED64:
      return reader.fixed64();
    case ScalarType2.BYTES:
      return reader.bytes();
    case ScalarType2.FIXED32:
      return reader.fixed32();
    case ScalarType2.SFIXED32:
      return reader.sfixed32();
    case ScalarType2.SFIXED64:
      return reader.sfixed64();
    case ScalarType2.SINT64:
      return reader.sint64();
    case ScalarType2.UINT32:
      return reader.uint32();
    case ScalarType2.SINT32:
      return reader.sint32();
  }
}
function fileDesc(b64, imports) {
  var _a;
  const root = fromBinary2(FileDescriptorProtoSchema, base64Decode(b64));
  root.messageType.forEach(restoreJsonNames);
  root.dependency = (_a = imports === null || imports === void 0 ? void 0 : imports.map((f) => f.proto.name)) !== null && _a !== void 0 ? _a : [];
  return createFileRegistry(root, (protoFileName) => imports === null || imports === void 0 ? void 0 : imports.find((f) => f.proto.name === protoFileName)).getFile(root.name);
}
function messageDesc(file, path, ...paths) {
  return paths.reduce((acc, cur) => acc.nestedMessages[cur], file.messages[path]);
}
var dist_exports = __export({
  ATAK: () => atak_pb_exports,
  Admin: () => admin_pb_exports,
  AppOnly: () => apponly_pb_exports,
  CannedMessages: () => cannedmessages_pb_exports,
  Channel: () => channel_pb_exports,
  ClientOnly: () => clientonly_pb_exports,
  Config: () => config_pb_exports,
  ConnectionStatus: () => connection_status_pb_exports,
  LocalOnly: () => localonly_pb_exports,
  Mesh: () => mesh_pb_exports,
  ModuleConfig: () => module_config_pb_exports,
  Mqtt: () => mqtt_pb_exports,
  PaxCount: () => paxcount_pb_exports,
  Portnums: () => portnums_pb_exports,
  PowerMon: () => powermon_pb_exports,
  RemoteHardware: () => remote_hardware_pb_exports,
  Rtttl: () => rtttl_pb_exports,
  StoreForward: () => storeforward_pb_exports,
  Telemetry: () => telemetry_pb_exports,
  Xmodem: () => xmodem_pb_exports
});
var channel_pb_exports = __export2({
  ChannelSchema: () => ChannelSchema,
  ChannelSettingsSchema: () => ChannelSettingsSchema,
  Channel_Role: () => Channel_Role,
  Channel_RoleSchema: () => Channel_RoleSchema,
  ModuleSettingsSchema: () => ModuleSettingsSchema,
  file_channel: () => file_channel
});
var file_channel = /* @__PURE__ */ fileDesc("Cg1jaGFubmVsLnByb3RvEgptZXNodGFzdGljIrgBCg9DaGFubmVsU2V0dGluZ3MSFwoLY2hhbm5lbF9udW0YASABKA1CAhgBEgsKA3BzaxgCIAEoDBIMCgRuYW1lGAMgASgJEgoKAmlkGAQgASgHEhYKDnVwbGlua19lbmFibGVkGAUgASgIEhgKEGRvd25saW5rX2VuYWJsZWQYBiABKAgSMwoPbW9kdWxlX3NldHRpbmdzGAcgASgLMhoubWVzaHRhc3RpYy5Nb2R1bGVTZXR0aW5ncyJFCg5Nb2R1bGVTZXR0aW5ncxIaChJwb3NpdGlvbl9wcmVjaXNpb24YASABKA0SFwoPaXNfY2xpZW50X211dGVkGAIgASgIIqEBCgdDaGFubmVsEg0KBWluZGV4GAEgASgFEi0KCHNldHRpbmdzGAIgASgLMhsubWVzaHRhc3RpYy5DaGFubmVsU2V0dGluZ3MSJgoEcm9sZRgDIAEoDjIYLm1lc2h0YXN0aWMuQ2hhbm5lbC5Sb2xlIjAKBFJvbGUSDAoIRElTQUJMRUQQABILCgdQUklNQVJZEAESDQoJU0VDT05EQVJZEAJCYgoTY29tLmdlZWtzdmlsbGUubWVzaEINQ2hhbm5lbFByb3Rvc1oiZ2l0aHViLmNvbS9tZXNodGFzdGljL2dvL2dlbmVyYXRlZKoCFE1lc2h0YXN0aWMuUHJvdG9idWZzugIAYgZwcm90bzM");
var ChannelSettingsSchema = /* @__PURE__ */ messageDesc(file_channel, 0);
var ModuleSettingsSchema = /* @__PURE__ */ messageDesc(file_channel, 1);
var ChannelSchema = /* @__PURE__ */ messageDesc(file_channel, 2);
var Channel_Role = /* @__PURE__ */ function(Channel_Role$1) {
  Channel_Role$1[Channel_Role$1["DISABLED"] = 0] = "DISABLED";
  Channel_Role$1[Channel_Role$1["PRIMARY"] = 1] = "PRIMARY";
  Channel_Role$1[Channel_Role$1["SECONDARY"] = 2] = "SECONDARY";
  return Channel_Role$1;
}({});
var Channel_RoleSchema = /* @__PURE__ */ enumDesc(file_channel, 2, 0);
var file_device_ui = /* @__PURE__ */ fileDesc("Cg9kZXZpY2VfdWkucHJvdG8SCm1lc2h0YXN0aWMivgMKDkRldmljZVVJQ29uZmlnEg8KB3ZlcnNpb24YASABKA0SGQoRc2NyZWVuX2JyaWdodG5lc3MYAiABKA0SFgoOc2NyZWVuX3RpbWVvdXQYAyABKA0SEwoLc2NyZWVuX2xvY2sYBCABKAgSFQoNc2V0dGluZ3NfbG9jaxgFIAEoCBIQCghwaW5fY29kZRgGIAEoDRIgCgV0aGVtZRgHIAEoDjIRLm1lc2h0YXN0aWMuVGhlbWUSFQoNYWxlcnRfZW5hYmxlZBgIIAEoCBIWCg5iYW5uZXJfZW5hYmxlZBgJIAEoCBIUCgxyaW5nX3RvbmVfaWQYCiABKA0SJgoIbGFuZ3VhZ2UYCyABKA4yFC5tZXNodGFzdGljLkxhbmd1YWdlEisKC25vZGVfZmlsdGVyGAwgASgLMhYubWVzaHRhc3RpYy5Ob2RlRmlsdGVyEjEKDm5vZGVfaGlnaGxpZ2h0GA0gASgLMhkubWVzaHRhc3RpYy5Ob2RlSGlnaGxpZ2h0EhgKEGNhbGlicmF0aW9uX2RhdGEYDiABKAwSIQoIbWFwX2RhdGEYDyABKAsyDy5tZXNodGFzdGljLk1hcCKnAQoKTm9kZUZpbHRlchIWCg51bmtub3duX3N3aXRjaBgBIAEoCBIWCg5vZmZsaW5lX3N3aXRjaBgCIAEoCBIZChFwdWJsaWNfa2V5X3N3aXRjaBgDIAEoCBIRCglob3BzX2F3YXkYBCABKAUSFwoPcG9zaXRpb25fc3dpdGNoGAUgASgIEhEKCW5vZGVfbmFtZRgGIAEoCRIPCgdjaGFubmVsGAcgASgFIn4KDU5vZGVIaWdobGlnaHQSEwoLY2hhdF9zd2l0Y2gYASABKAgSFwoPcG9zaXRpb25fc3dpdGNoGAIgASgIEhgKEHRlbGVtZXRyeV9zd2l0Y2gYAyABKAgSEgoKaWFxX3N3aXRjaBgEIAEoCBIRCglub2RlX25hbWUYBSABKAkiPQoIR2VvUG9pbnQSDAoEem9vbRgBIAEoBRIQCghsYXRpdHVkZRgCIAEoBRIRCglsb25naXR1ZGUYAyABKAUiTAoDTWFwEiIKBGhvbWUYASABKAsyFC5tZXNodGFzdGljLkdlb1BvaW50Eg0KBXN0eWxlGAIgASgJEhIKCmZvbGxvd19ncHMYAyABKAgqJQoFVGhlbWUSCAoEREFSSxAAEgkKBUxJR0hUEAESBwoDUkVEEAIqqQIKCExhbmd1YWdlEgsKB0VOR0xJU0gQABIKCgZGUkVOQ0gQARIKCgZHRVJNQU4QAhILCgdJVEFMSUFOEAMSDgoKUE9SVFVHVUVTRRAEEgsKB1NQQU5JU0gQBRILCgdTV0VESVNIEAYSCwoHRklOTklTSBAHEgoKBlBPTElTSBAIEgsKB1RVUktJU0gQCRILCgdTRVJCSUFOEAoSCwoHUlVTU0lBThALEgkKBURVVENIEAwSCQoFR1JFRUsQDRINCglOT1JXRUdJQU4QDhINCglTTE9WRU5JQU4QDxINCglVS1JBSU5JQU4QEBINCglCVUxHQVJJQU4QERIWChJTSU1QTElGSUVEX0NISU5FU0UQHhIXChNUUkFESVRJT05BTF9DSElORVNFEB9CYwoTY29tLmdlZWtzdmlsbGUubWVzaEIORGV2aWNlVUlQcm90b3NaImdpdGh1Yi5jb20vbWVzaHRhc3RpYy9nby9nZW5lcmF0ZWSqAhRNZXNodGFzdGljLlByb3RvYnVmc7oCAGIGcHJvdG8z");
var config_pb_exports = __export2({
  ConfigSchema: () => ConfigSchema,
  Config_BluetoothConfigSchema: () => Config_BluetoothConfigSchema,
  Config_BluetoothConfig_PairingMode: () => Config_BluetoothConfig_PairingMode,
  Config_BluetoothConfig_PairingModeSchema: () => Config_BluetoothConfig_PairingModeSchema,
  Config_DeviceConfigSchema: () => Config_DeviceConfigSchema,
  Config_DeviceConfig_BuzzerMode: () => Config_DeviceConfig_BuzzerMode,
  Config_DeviceConfig_BuzzerModeSchema: () => Config_DeviceConfig_BuzzerModeSchema,
  Config_DeviceConfig_RebroadcastMode: () => Config_DeviceConfig_RebroadcastMode,
  Config_DeviceConfig_RebroadcastModeSchema: () => Config_DeviceConfig_RebroadcastModeSchema,
  Config_DeviceConfig_Role: () => Config_DeviceConfig_Role,
  Config_DeviceConfig_RoleSchema: () => Config_DeviceConfig_RoleSchema,
  Config_DisplayConfigSchema: () => Config_DisplayConfigSchema,
  Config_DisplayConfig_CompassOrientation: () => Config_DisplayConfig_CompassOrientation,
  Config_DisplayConfig_CompassOrientationSchema: () => Config_DisplayConfig_CompassOrientationSchema,
  Config_DisplayConfig_DisplayMode: () => Config_DisplayConfig_DisplayMode,
  Config_DisplayConfig_DisplayModeSchema: () => Config_DisplayConfig_DisplayModeSchema,
  Config_DisplayConfig_DisplayUnits: () => Config_DisplayConfig_DisplayUnits,
  Config_DisplayConfig_DisplayUnitsSchema: () => Config_DisplayConfig_DisplayUnitsSchema,
  Config_DisplayConfig_GpsCoordinateFormat: () => Config_DisplayConfig_GpsCoordinateFormat,
  Config_DisplayConfig_GpsCoordinateFormatSchema: () => Config_DisplayConfig_GpsCoordinateFormatSchema,
  Config_DisplayConfig_OledType: () => Config_DisplayConfig_OledType,
  Config_DisplayConfig_OledTypeSchema: () => Config_DisplayConfig_OledTypeSchema,
  Config_LoRaConfigSchema: () => Config_LoRaConfigSchema,
  Config_LoRaConfig_ModemPreset: () => Config_LoRaConfig_ModemPreset,
  Config_LoRaConfig_ModemPresetSchema: () => Config_LoRaConfig_ModemPresetSchema,
  Config_LoRaConfig_RegionCode: () => Config_LoRaConfig_RegionCode,
  Config_LoRaConfig_RegionCodeSchema: () => Config_LoRaConfig_RegionCodeSchema,
  Config_NetworkConfigSchema: () => Config_NetworkConfigSchema,
  Config_NetworkConfig_AddressMode: () => Config_NetworkConfig_AddressMode,
  Config_NetworkConfig_AddressModeSchema: () => Config_NetworkConfig_AddressModeSchema,
  Config_NetworkConfig_IpV4ConfigSchema: () => Config_NetworkConfig_IpV4ConfigSchema,
  Config_NetworkConfig_ProtocolFlags: () => Config_NetworkConfig_ProtocolFlags,
  Config_NetworkConfig_ProtocolFlagsSchema: () => Config_NetworkConfig_ProtocolFlagsSchema,
  Config_PositionConfigSchema: () => Config_PositionConfigSchema,
  Config_PositionConfig_GpsMode: () => Config_PositionConfig_GpsMode,
  Config_PositionConfig_GpsModeSchema: () => Config_PositionConfig_GpsModeSchema,
  Config_PositionConfig_PositionFlags: () => Config_PositionConfig_PositionFlags,
  Config_PositionConfig_PositionFlagsSchema: () => Config_PositionConfig_PositionFlagsSchema,
  Config_PowerConfigSchema: () => Config_PowerConfigSchema,
  Config_SecurityConfigSchema: () => Config_SecurityConfigSchema,
  Config_SessionkeyConfigSchema: () => Config_SessionkeyConfigSchema,
  file_config: () => file_config
});
var file_config = /* @__PURE__ */ fileDesc("Cgxjb25maWcucHJvdG8SCm1lc2h0YXN0aWMipigKBkNvbmZpZxIxCgZkZXZpY2UYASABKAsyHy5tZXNodGFzdGljLkNvbmZpZy5EZXZpY2VDb25maWdIABI1Cghwb3NpdGlvbhgCIAEoCzIhLm1lc2h0YXN0aWMuQ29uZmlnLlBvc2l0aW9uQ29uZmlnSAASLwoFcG93ZXIYAyABKAsyHi5tZXNodGFzdGljLkNvbmZpZy5Qb3dlckNvbmZpZ0gAEjMKB25ldHdvcmsYBCABKAsyIC5tZXNodGFzdGljLkNvbmZpZy5OZXR3b3JrQ29uZmlnSAASMwoHZGlzcGxheRgFIAEoCzIgLm1lc2h0YXN0aWMuQ29uZmlnLkRpc3BsYXlDb25maWdIABItCgRsb3JhGAYgASgLMh0ubWVzaHRhc3RpYy5Db25maWcuTG9SYUNvbmZpZ0gAEjcKCWJsdWV0b290aBgHIAEoCzIiLm1lc2h0YXN0aWMuQ29uZmlnLkJsdWV0b290aENvbmZpZ0gAEjUKCHNlY3VyaXR5GAggASgLMiEubWVzaHRhc3RpYy5Db25maWcuU2VjdXJpdHlDb25maWdIABI5CgpzZXNzaW9ua2V5GAkgASgLMiMubWVzaHRhc3RpYy5Db25maWcuU2Vzc2lvbmtleUNvbmZpZ0gAEi8KCWRldmljZV91aRgKIAEoCzIaLm1lc2h0YXN0aWMuRGV2aWNlVUlDb25maWdIABrMBgoMRGV2aWNlQ29uZmlnEjIKBHJvbGUYASABKA4yJC5tZXNodGFzdGljLkNvbmZpZy5EZXZpY2VDb25maWcuUm9sZRIaCg5zZXJpYWxfZW5hYmxlZBgCIAEoCEICGAESEwoLYnV0dG9uX2dwaW8YBCABKA0SEwoLYnV6emVyX2dwaW8YBSABKA0SSQoQcmVicm9hZGNhc3RfbW9kZRgGIAEoDjIvLm1lc2h0YXN0aWMuQ29uZmlnLkRldmljZUNvbmZpZy5SZWJyb2FkY2FzdE1vZGUSIAoYbm9kZV9pbmZvX2Jyb2FkY2FzdF9zZWNzGAcgASgNEiIKGmRvdWJsZV90YXBfYXNfYnV0dG9uX3ByZXNzGAggASgIEhYKCmlzX21hbmFnZWQYCSABKAhCAhgBEhwKFGRpc2FibGVfdHJpcGxlX2NsaWNrGAogASgIEg0KBXR6ZGVmGAsgASgJEh4KFmxlZF9oZWFydGJlYXRfZGlzYWJsZWQYDCABKAgSPwoLYnV6emVyX21vZGUYDSABKA4yKi5tZXNodGFzdGljLkNvbmZpZy5EZXZpY2VDb25maWcuQnV6emVyTW9kZSK/AQoEUm9sZRIKCgZDTElFTlQQABIPCgtDTElFTlRfTVVURRABEgoKBlJPVVRFUhACEhUKDVJPVVRFUl9DTElFTlQQAxoCCAESDAoIUkVQRUFURVIQBBILCgdUUkFDS0VSEAUSCgoGU0VOU09SEAYSBwoDVEFLEAcSEQoNQ0xJRU5UX0hJRERFThAIEhIKDkxPU1RfQU5EX0ZPVU5EEAkSDwoLVEFLX1RSQUNLRVIQChIPCgtST1VURVJfTEFURRALInMKD1JlYnJvYWRjYXN0TW9kZRIHCgNBTEwQABIVChFBTExfU0tJUF9ERUNPRElORxABEg4KCkxPQ0FMX09OTFkQAhIOCgpLTk9XTl9PTkxZEAMSCAoETk9ORRAEEhYKEkNPUkVfUE9SVE5VTVNfT05MWRAFIlQKCkJ1enplck1vZGUSDwoLQUxMX0VOQUJMRUQQABIMCghESVNBQkxFRBABEhYKEk5PVElGSUNBVElPTlNfT05MWRACEg8KC1NZU1RFTV9PTkxZEAMakQUKDlBvc2l0aW9uQ29uZmlnEh8KF3Bvc2l0aW9uX2Jyb2FkY2FzdF9zZWNzGAEgASgNEigKIHBvc2l0aW9uX2Jyb2FkY2FzdF9zbWFydF9lbmFibGVkGAIgASgIEhYKDmZpeGVkX3Bvc2l0aW9uGAMgASgIEhcKC2dwc19lbmFibGVkGAQgASgIQgIYARIbChNncHNfdXBkYXRlX2ludGVydmFsGAUgASgNEhwKEGdwc19hdHRlbXB0X3RpbWUYBiABKA1CAhgBEhYKDnBvc2l0aW9uX2ZsYWdzGAcgASgNEg8KB3J4X2dwaW8YCCABKA0SDwoHdHhfZ3BpbxgJIAEoDRIoCiBicm9hZGNhc3Rfc21hcnRfbWluaW11bV9kaXN0YW5jZRgKIAEoDRItCiVicm9hZGNhc3Rfc21hcnRfbWluaW11bV9pbnRlcnZhbF9zZWNzGAsgASgNEhMKC2dwc19lbl9ncGlvGAwgASgNEjsKCGdwc19tb2RlGA0gASgOMikubWVzaHRhc3RpYy5Db25maWcuUG9zaXRpb25Db25maWcuR3BzTW9kZSKrAQoNUG9zaXRpb25GbGFncxIJCgVVTlNFVBAAEgwKCEFMVElUVURFEAESEAoMQUxUSVRVREVfTVNMEAISFgoSR0VPSURBTF9TRVBBUkFUSU9OEAQSBwoDRE9QEAgSCQoFSFZET1AQEBINCglTQVRJTlZJRVcQIBIKCgZTRVFfTk8QQBIOCglUSU1FU1RBTVAQgAESDAoHSEVBRElORxCAAhIKCgVTUEVFRBCABCI1CgdHcHNNb2RlEgwKCERJU0FCTEVEEAASCwoHRU5BQkxFRBABEg8KC05PVF9QUkVTRU5UEAIahAIKC1Bvd2VyQ29uZmlnEhcKD2lzX3Bvd2VyX3NhdmluZxgBIAEoCBImCh5vbl9iYXR0ZXJ5X3NodXRkb3duX2FmdGVyX3NlY3MYAiABKA0SHwoXYWRjX211bHRpcGxpZXJfb3ZlcnJpZGUYAyABKAISGwoTd2FpdF9ibHVldG9vdGhfc2VjcxgEIAEoDRIQCghzZHNfc2VjcxgGIAEoDRIPCgdsc19zZWNzGAcgASgNEhUKDW1pbl93YWtlX3NlY3MYCCABKA0SIgoaZGV2aWNlX2JhdHRlcnlfaW5hX2FkZHJlc3MYCSABKA0SGAoQcG93ZXJtb25fZW5hYmxlcxggIAEoBBrlAwoNTmV0d29ya0NvbmZpZxIUCgx3aWZpX2VuYWJsZWQYASABKAgSEQoJd2lmaV9zc2lkGAMgASgJEhAKCHdpZmlfcHNrGAQgASgJEhIKCm50cF9zZXJ2ZXIYBSABKAkSEwoLZXRoX2VuYWJsZWQYBiABKAgSQgoMYWRkcmVzc19tb2RlGAcgASgOMiwubWVzaHRhc3RpYy5Db25maWcuTmV0d29ya0NvbmZpZy5BZGRyZXNzTW9kZRJACgtpcHY0X2NvbmZpZxgIIAEoCzIrLm1lc2h0YXN0aWMuQ29uZmlnLk5ldHdvcmtDb25maWcuSXBWNENvbmZpZxIWCg5yc3lzbG9nX3NlcnZlchgJIAEoCRIZChFlbmFibGVkX3Byb3RvY29scxgKIAEoDRIUCgxpcHY2X2VuYWJsZWQYCyABKAgaRgoKSXBWNENvbmZpZxIKCgJpcBgBIAEoBxIPCgdnYXRld2F5GAIgASgHEg4KBnN1Ym5ldBgDIAEoBxILCgNkbnMYBCABKAciIwoLQWRkcmVzc01vZGUSCAoEREhDUBAAEgoKBlNUQVRJQxABIjQKDVByb3RvY29sRmxhZ3MSEAoMTk9fQlJPQURDQVNUEAASEQoNVURQX0JST0FEQ0FTVBABGvwHCg1EaXNwbGF5Q29uZmlnEhYKDnNjcmVlbl9vbl9zZWNzGAEgASgNEkgKCmdwc19mb3JtYXQYAiABKA4yNC5tZXNodGFzdGljLkNvbmZpZy5EaXNwbGF5Q29uZmlnLkdwc0Nvb3JkaW5hdGVGb3JtYXQSIQoZYXV0b19zY3JlZW5fY2Fyb3VzZWxfc2VjcxgDIAEoDRIZChFjb21wYXNzX25vcnRoX3RvcBgEIAEoCBITCgtmbGlwX3NjcmVlbhgFIAEoCBI8CgV1bml0cxgGIAEoDjItLm1lc2h0YXN0aWMuQ29uZmlnLkRpc3BsYXlDb25maWcuRGlzcGxheVVuaXRzEjcKBG9sZWQYByABKA4yKS5tZXNodGFzdGljLkNvbmZpZy5EaXNwbGF5Q29uZmlnLk9sZWRUeXBlEkEKC2Rpc3BsYXltb2RlGAggASgOMiwubWVzaHRhc3RpYy5Db25maWcuRGlzcGxheUNvbmZpZy5EaXNwbGF5TW9kZRIUCgxoZWFkaW5nX2JvbGQYCSABKAgSHQoVd2FrZV9vbl90YXBfb3JfbW90aW9uGAogASgIElAKE2NvbXBhc3Nfb3JpZW50YXRpb24YCyABKA4yMy5tZXNodGFzdGljLkNvbmZpZy5EaXNwbGF5Q29uZmlnLkNvbXBhc3NPcmllbnRhdGlvbhIVCg11c2VfMTJoX2Nsb2NrGAwgASgIIk0KE0dwc0Nvb3JkaW5hdGVGb3JtYXQSBwoDREVDEAASBwoDRE1TEAESBwoDVVRNEAISCAoETUdSUxADEgcKA09MQxAEEggKBE9TR1IQBSIoCgxEaXNwbGF5VW5pdHMSCgoGTUVUUklDEAASDAoISU1QRVJJQUwQASJlCghPbGVkVHlwZRINCglPTEVEX0FVVE8QABIQCgxPTEVEX1NTRDEzMDYQARIPCgtPTEVEX1NIMTEwNhACEg8KC09MRURfU0gxMTA3EAMSFgoST0xFRF9TSDExMDdfMTI4XzY0EAQiQQoLRGlzcGxheU1vZGUSCwoHREVGQVVMVBAAEgwKCFRXT0NPTE9SEAESDAoISU5WRVJURUQQAhIJCgVDT0xPUhADIroBChJDb21wYXNzT3JpZW50YXRpb24SDQoJREVHUkVFU18wEAASDgoKREVHUkVFU185MBABEg8KC0RFR1JFRVNfMTgwEAISDwoLREVHUkVFU18yNzAQAxIWChJERUdSRUVTXzBfSU5WRVJURUQQBBIXChNERUdSRUVTXzkwX0lOVkVSVEVEEAUSGAoUREVHUkVFU18xODBfSU5WRVJURUQQBhIYChRERUdSRUVTXzI3MF9JTlZFUlRFRBAHGqoHCgpMb1JhQ29uZmlnEhIKCnVzZV9wcmVzZXQYASABKAgSPwoMbW9kZW1fcHJlc2V0GAIgASgOMikubWVzaHRhc3RpYy5Db25maWcuTG9SYUNvbmZpZy5Nb2RlbVByZXNldBIRCgliYW5kd2lkdGgYAyABKA0SFQoNc3ByZWFkX2ZhY3RvchgEIAEoDRITCgtjb2RpbmdfcmF0ZRgFIAEoDRIYChBmcmVxdWVuY3lfb2Zmc2V0GAYgASgCEjgKBnJlZ2lvbhgHIAEoDjIoLm1lc2h0YXN0aWMuQ29uZmlnLkxvUmFDb25maWcuUmVnaW9uQ29kZRIRCglob3BfbGltaXQYCCABKA0SEgoKdHhfZW5hYmxlZBgJIAEoCBIQCgh0eF9wb3dlchgKIAEoBRITCgtjaGFubmVsX251bRgLIAEoDRIbChNvdmVycmlkZV9kdXR5X2N5Y2xlGAwgASgIEh4KFnN4MTI2eF9yeF9ib29zdGVkX2dhaW4YDSABKAgSGgoSb3ZlcnJpZGVfZnJlcXVlbmN5GA4gASgCEhcKD3BhX2Zhbl9kaXNhYmxlZBgPIAEoCBIXCg9pZ25vcmVfaW5jb21pbmcYZyADKA0SEwoLaWdub3JlX21xdHQYaCABKAgSGQoRY29uZmlnX29rX3RvX21xdHQYaSABKAgi/gEKClJlZ2lvbkNvZGUSCQoFVU5TRVQQABIGCgJVUxABEgoKBkVVXzQzMxACEgoKBkVVXzg2OBADEgYKAkNOEAQSBgoCSlAQBRIHCgNBTloQBhIGCgJLUhAHEgYKAlRXEAgSBgoCUlUQCRIGCgJJThAKEgoKBk5aXzg2NRALEgYKAlRIEAwSCwoHTE9SQV8yNBANEgoKBlVBXzQzMxAOEgoKBlVBXzg2OBAPEgoKBk1ZXzQzMxAQEgoKBk1ZXzkxORAREgoKBlNHXzkyMxASEgoKBlBIXzQzMxATEgoKBlBIXzg2OBAUEgoKBlBIXzkxNRAVEgsKB0FOWl80MzMQFiKpAQoLTW9kZW1QcmVzZXQSDQoJTE9OR19GQVNUEAASDQoJTE9OR19TTE9XEAESFgoOVkVSWV9MT05HX1NMT1cQAhoCCAESDwoLTUVESVVNX1NMT1cQAxIPCgtNRURJVU1fRkFTVBAEEg4KClNIT1JUX1NMT1cQBRIOCgpTSE9SVF9GQVNUEAYSEQoNTE9OR19NT0RFUkFURRAHEg8KC1NIT1JUX1RVUkJPEAgarQEKD0JsdWV0b290aENvbmZpZxIPCgdlbmFibGVkGAEgASgIEjwKBG1vZGUYAiABKA4yLi5tZXNodGFzdGljLkNvbmZpZy5CbHVldG9vdGhDb25maWcuUGFpcmluZ01vZGUSEQoJZml4ZWRfcGluGAMgASgNIjgKC1BhaXJpbmdNb2RlEg4KClJBTkRPTV9QSU4QABINCglGSVhFRF9QSU4QARIKCgZOT19QSU4QAhq2AQoOU2VjdXJpdHlDb25maWcSEgoKcHVibGljX2tleRgBIAEoDBITCgtwcml2YXRlX2tleRgCIAEoDBIRCglhZG1pbl9rZXkYAyADKAwSEgoKaXNfbWFuYWdlZBgEIAEoCBIWCg5zZXJpYWxfZW5hYmxlZBgFIAEoCBIdChVkZWJ1Z19sb2dfYXBpX2VuYWJsZWQYBiABKAgSHQoVYWRtaW5fY2hhbm5lbF9lbmFibGVkGAggASgIGhIKEFNlc3Npb25rZXlDb25maWdCEQoPcGF5bG9hZF92YXJpYW50QmEKE2NvbS5nZWVrc3ZpbGxlLm1lc2hCDENvbmZpZ1Byb3Rvc1oiZ2l0aHViLmNvbS9tZXNodGFzdGljL2dvL2dlbmVyYXRlZKoCFE1lc2h0YXN0aWMuUHJvdG9idWZzugIAYgZwcm90bzM", [file_device_ui]);
var ConfigSchema = /* @__PURE__ */ messageDesc(file_config, 0);
var Config_DeviceConfigSchema = /* @__PURE__ */ messageDesc(file_config, 0, 0);
var Config_DeviceConfig_Role = /* @__PURE__ */ function(Config_DeviceConfig_Role$1) {
  Config_DeviceConfig_Role$1[Config_DeviceConfig_Role$1["CLIENT"] = 0] = "CLIENT";
  Config_DeviceConfig_Role$1[Config_DeviceConfig_Role$1["CLIENT_MUTE"] = 1] = "CLIENT_MUTE";
  Config_DeviceConfig_Role$1[Config_DeviceConfig_Role$1["ROUTER"] = 2] = "ROUTER";
  Config_DeviceConfig_Role$1[Config_DeviceConfig_Role$1["ROUTER_CLIENT"] = 3] = "ROUTER_CLIENT";
  Config_DeviceConfig_Role$1[Config_DeviceConfig_Role$1["REPEATER"] = 4] = "REPEATER";
  Config_DeviceConfig_Role$1[Config_DeviceConfig_Role$1["TRACKER"] = 5] = "TRACKER";
  Config_DeviceConfig_Role$1[Config_DeviceConfig_Role$1["SENSOR"] = 6] = "SENSOR";
  Config_DeviceConfig_Role$1[Config_DeviceConfig_Role$1["TAK"] = 7] = "TAK";
  Config_DeviceConfig_Role$1[Config_DeviceConfig_Role$1["CLIENT_HIDDEN"] = 8] = "CLIENT_HIDDEN";
  Config_DeviceConfig_Role$1[Config_DeviceConfig_Role$1["LOST_AND_FOUND"] = 9] = "LOST_AND_FOUND";
  Config_DeviceConfig_Role$1[Config_DeviceConfig_Role$1["TAK_TRACKER"] = 10] = "TAK_TRACKER";
  Config_DeviceConfig_Role$1[Config_DeviceConfig_Role$1["ROUTER_LATE"] = 11] = "ROUTER_LATE";
  return Config_DeviceConfig_Role$1;
}({});
var Config_DeviceConfig_RoleSchema = /* @__PURE__ */ enumDesc(file_config, 0, 0, 0);
var Config_DeviceConfig_RebroadcastMode = /* @__PURE__ */ function(Config_DeviceConfig_RebroadcastMode$1) {
  Config_DeviceConfig_RebroadcastMode$1[Config_DeviceConfig_RebroadcastMode$1["ALL"] = 0] = "ALL";
  Config_DeviceConfig_RebroadcastMode$1[Config_DeviceConfig_RebroadcastMode$1["ALL_SKIP_DECODING"] = 1] = "ALL_SKIP_DECODING";
  Config_DeviceConfig_RebroadcastMode$1[Config_DeviceConfig_RebroadcastMode$1["LOCAL_ONLY"] = 2] = "LOCAL_ONLY";
  Config_DeviceConfig_RebroadcastMode$1[Config_DeviceConfig_RebroadcastMode$1["KNOWN_ONLY"] = 3] = "KNOWN_ONLY";
  Config_DeviceConfig_RebroadcastMode$1[Config_DeviceConfig_RebroadcastMode$1["NONE"] = 4] = "NONE";
  Config_DeviceConfig_RebroadcastMode$1[Config_DeviceConfig_RebroadcastMode$1["CORE_PORTNUMS_ONLY"] = 5] = "CORE_PORTNUMS_ONLY";
  return Config_DeviceConfig_RebroadcastMode$1;
}({});
var Config_DeviceConfig_RebroadcastModeSchema = /* @__PURE__ */ enumDesc(file_config, 0, 0, 1);
var Config_DeviceConfig_BuzzerMode = /* @__PURE__ */ function(Config_DeviceConfig_BuzzerMode$1) {
  Config_DeviceConfig_BuzzerMode$1[Config_DeviceConfig_BuzzerMode$1["ALL_ENABLED"] = 0] = "ALL_ENABLED";
  Config_DeviceConfig_BuzzerMode$1[Config_DeviceConfig_BuzzerMode$1["DISABLED"] = 1] = "DISABLED";
  Config_DeviceConfig_BuzzerMode$1[Config_DeviceConfig_BuzzerMode$1["NOTIFICATIONS_ONLY"] = 2] = "NOTIFICATIONS_ONLY";
  Config_DeviceConfig_BuzzerMode$1[Config_DeviceConfig_BuzzerMode$1["SYSTEM_ONLY"] = 3] = "SYSTEM_ONLY";
  return Config_DeviceConfig_BuzzerMode$1;
}({});
var Config_DeviceConfig_BuzzerModeSchema = /* @__PURE__ */ enumDesc(file_config, 0, 0, 2);
var Config_PositionConfigSchema = /* @__PURE__ */ messageDesc(file_config, 0, 1);
var Config_PositionConfig_PositionFlags = /* @__PURE__ */ function(Config_PositionConfig_PositionFlags$1) {
  Config_PositionConfig_PositionFlags$1[Config_PositionConfig_PositionFlags$1["UNSET"] = 0] = "UNSET";
  Config_PositionConfig_PositionFlags$1[Config_PositionConfig_PositionFlags$1["ALTITUDE"] = 1] = "ALTITUDE";
  Config_PositionConfig_PositionFlags$1[Config_PositionConfig_PositionFlags$1["ALTITUDE_MSL"] = 2] = "ALTITUDE_MSL";
  Config_PositionConfig_PositionFlags$1[Config_PositionConfig_PositionFlags$1["GEOIDAL_SEPARATION"] = 4] = "GEOIDAL_SEPARATION";
  Config_PositionConfig_PositionFlags$1[Config_PositionConfig_PositionFlags$1["DOP"] = 8] = "DOP";
  Config_PositionConfig_PositionFlags$1[Config_PositionConfig_PositionFlags$1["HVDOP"] = 16] = "HVDOP";
  Config_PositionConfig_PositionFlags$1[Config_PositionConfig_PositionFlags$1["SATINVIEW"] = 32] = "SATINVIEW";
  Config_PositionConfig_PositionFlags$1[Config_PositionConfig_PositionFlags$1["SEQ_NO"] = 64] = "SEQ_NO";
  Config_PositionConfig_PositionFlags$1[Config_PositionConfig_PositionFlags$1["TIMESTAMP"] = 128] = "TIMESTAMP";
  Config_PositionConfig_PositionFlags$1[Config_PositionConfig_PositionFlags$1["HEADING"] = 256] = "HEADING";
  Config_PositionConfig_PositionFlags$1[Config_PositionConfig_PositionFlags$1["SPEED"] = 512] = "SPEED";
  return Config_PositionConfig_PositionFlags$1;
}({});
var Config_PositionConfig_PositionFlagsSchema = /* @__PURE__ */ enumDesc(file_config, 0, 1, 0);
var Config_PositionConfig_GpsMode = /* @__PURE__ */ function(Config_PositionConfig_GpsMode$1) {
  Config_PositionConfig_GpsMode$1[Config_PositionConfig_GpsMode$1["DISABLED"] = 0] = "DISABLED";
  Config_PositionConfig_GpsMode$1[Config_PositionConfig_GpsMode$1["ENABLED"] = 1] = "ENABLED";
  Config_PositionConfig_GpsMode$1[Config_PositionConfig_GpsMode$1["NOT_PRESENT"] = 2] = "NOT_PRESENT";
  return Config_PositionConfig_GpsMode$1;
}({});
var Config_PositionConfig_GpsModeSchema = /* @__PURE__ */ enumDesc(file_config, 0, 1, 1);
var Config_PowerConfigSchema = /* @__PURE__ */ messageDesc(file_config, 0, 2);
var Config_NetworkConfigSchema = /* @__PURE__ */ messageDesc(file_config, 0, 3);
var Config_NetworkConfig_IpV4ConfigSchema = /* @__PURE__ */ messageDesc(file_config, 0, 3, 0);
var Config_NetworkConfig_AddressMode = /* @__PURE__ */ function(Config_NetworkConfig_AddressMode$1) {
  Config_NetworkConfig_AddressMode$1[Config_NetworkConfig_AddressMode$1["DHCP"] = 0] = "DHCP";
  Config_NetworkConfig_AddressMode$1[Config_NetworkConfig_AddressMode$1["STATIC"] = 1] = "STATIC";
  return Config_NetworkConfig_AddressMode$1;
}({});
var Config_NetworkConfig_AddressModeSchema = /* @__PURE__ */ enumDesc(file_config, 0, 3, 0);
var Config_NetworkConfig_ProtocolFlags = /* @__PURE__ */ function(Config_NetworkConfig_ProtocolFlags$1) {
  Config_NetworkConfig_ProtocolFlags$1[Config_NetworkConfig_ProtocolFlags$1["NO_BROADCAST"] = 0] = "NO_BROADCAST";
  Config_NetworkConfig_ProtocolFlags$1[Config_NetworkConfig_ProtocolFlags$1["UDP_BROADCAST"] = 1] = "UDP_BROADCAST";
  return Config_NetworkConfig_ProtocolFlags$1;
}({});
var Config_NetworkConfig_ProtocolFlagsSchema = /* @__PURE__ */ enumDesc(file_config, 0, 3, 1);
var Config_DisplayConfigSchema = /* @__PURE__ */ messageDesc(file_config, 0, 4);
var Config_DisplayConfig_GpsCoordinateFormat = /* @__PURE__ */ function(Config_DisplayConfig_GpsCoordinateFormat$1) {
  Config_DisplayConfig_GpsCoordinateFormat$1[Config_DisplayConfig_GpsCoordinateFormat$1["DEC"] = 0] = "DEC";
  Config_DisplayConfig_GpsCoordinateFormat$1[Config_DisplayConfig_GpsCoordinateFormat$1["DMS"] = 1] = "DMS";
  Config_DisplayConfig_GpsCoordinateFormat$1[Config_DisplayConfig_GpsCoordinateFormat$1["UTM"] = 2] = "UTM";
  Config_DisplayConfig_GpsCoordinateFormat$1[Config_DisplayConfig_GpsCoordinateFormat$1["MGRS"] = 3] = "MGRS";
  Config_DisplayConfig_GpsCoordinateFormat$1[Config_DisplayConfig_GpsCoordinateFormat$1["OLC"] = 4] = "OLC";
  Config_DisplayConfig_GpsCoordinateFormat$1[Config_DisplayConfig_GpsCoordinateFormat$1["OSGR"] = 5] = "OSGR";
  return Config_DisplayConfig_GpsCoordinateFormat$1;
}({});
var Config_DisplayConfig_GpsCoordinateFormatSchema = /* @__PURE__ */ enumDesc(file_config, 0, 4, 0);
var Config_DisplayConfig_DisplayUnits = /* @__PURE__ */ function(Config_DisplayConfig_DisplayUnits$1) {
  Config_DisplayConfig_DisplayUnits$1[Config_DisplayConfig_DisplayUnits$1["METRIC"] = 0] = "METRIC";
  Config_DisplayConfig_DisplayUnits$1[Config_DisplayConfig_DisplayUnits$1["IMPERIAL"] = 1] = "IMPERIAL";
  return Config_DisplayConfig_DisplayUnits$1;
}({});
var Config_DisplayConfig_DisplayUnitsSchema = /* @__PURE__ */ enumDesc(file_config, 0, 4, 1);
var Config_DisplayConfig_OledType = /* @__PURE__ */ function(Config_DisplayConfig_OledType$1) {
  Config_DisplayConfig_OledType$1[Config_DisplayConfig_OledType$1["OLED_AUTO"] = 0] = "OLED_AUTO";
  Config_DisplayConfig_OledType$1[Config_DisplayConfig_OledType$1["OLED_SSD1306"] = 1] = "OLED_SSD1306";
  Config_DisplayConfig_OledType$1[Config_DisplayConfig_OledType$1["OLED_SH1106"] = 2] = "OLED_SH1106";
  Config_DisplayConfig_OledType$1[Config_DisplayConfig_OledType$1["OLED_SH1107"] = 3] = "OLED_SH1107";
  Config_DisplayConfig_OledType$1[Config_DisplayConfig_OledType$1["OLED_SH1107_128_64"] = 4] = "OLED_SH1107_128_64";
  return Config_DisplayConfig_OledType$1;
}({});
var Config_DisplayConfig_OledTypeSchema = /* @__PURE__ */ enumDesc(file_config, 0, 4, 2);
var Config_DisplayConfig_DisplayMode = /* @__PURE__ */ function(Config_DisplayConfig_DisplayMode$1) {
  Config_DisplayConfig_DisplayMode$1[Config_DisplayConfig_DisplayMode$1["DEFAULT"] = 0] = "DEFAULT";
  Config_DisplayConfig_DisplayMode$1[Config_DisplayConfig_DisplayMode$1["TWOCOLOR"] = 1] = "TWOCOLOR";
  Config_DisplayConfig_DisplayMode$1[Config_DisplayConfig_DisplayMode$1["INVERTED"] = 2] = "INVERTED";
  Config_DisplayConfig_DisplayMode$1[Config_DisplayConfig_DisplayMode$1["COLOR"] = 3] = "COLOR";
  return Config_DisplayConfig_DisplayMode$1;
}({});
var Config_DisplayConfig_DisplayModeSchema = /* @__PURE__ */ enumDesc(file_config, 0, 4, 3);
var Config_DisplayConfig_CompassOrientation = /* @__PURE__ */ function(Config_DisplayConfig_CompassOrientation$1) {
  Config_DisplayConfig_CompassOrientation$1[Config_DisplayConfig_CompassOrientation$1["DEGREES_0"] = 0] = "DEGREES_0";
  Config_DisplayConfig_CompassOrientation$1[Config_DisplayConfig_CompassOrientation$1["DEGREES_90"] = 1] = "DEGREES_90";
  Config_DisplayConfig_CompassOrientation$1[Config_DisplayConfig_CompassOrientation$1["DEGREES_180"] = 2] = "DEGREES_180";
  Config_DisplayConfig_CompassOrientation$1[Config_DisplayConfig_CompassOrientation$1["DEGREES_270"] = 3] = "DEGREES_270";
  Config_DisplayConfig_CompassOrientation$1[Config_DisplayConfig_CompassOrientation$1["DEGREES_0_INVERTED"] = 4] = "DEGREES_0_INVERTED";
  Config_DisplayConfig_CompassOrientation$1[Config_DisplayConfig_CompassOrientation$1["DEGREES_90_INVERTED"] = 5] = "DEGREES_90_INVERTED";
  Config_DisplayConfig_CompassOrientation$1[Config_DisplayConfig_CompassOrientation$1["DEGREES_180_INVERTED"] = 6] = "DEGREES_180_INVERTED";
  Config_DisplayConfig_CompassOrientation$1[Config_DisplayConfig_CompassOrientation$1["DEGREES_270_INVERTED"] = 7] = "DEGREES_270_INVERTED";
  return Config_DisplayConfig_CompassOrientation$1;
}({});
var Config_DisplayConfig_CompassOrientationSchema = /* @__PURE__ */ enumDesc(file_config, 0, 4, 4);
var Config_LoRaConfigSchema = /* @__PURE__ */ messageDesc(file_config, 0, 5);
var Config_LoRaConfig_RegionCode = /* @__PURE__ */ function(Config_LoRaConfig_RegionCode$1) {
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["UNSET"] = 0] = "UNSET";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["US"] = 1] = "US";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["EU_433"] = 2] = "EU_433";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["EU_868"] = 3] = "EU_868";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["CN"] = 4] = "CN";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["JP"] = 5] = "JP";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["ANZ"] = 6] = "ANZ";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["KR"] = 7] = "KR";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["TW"] = 8] = "TW";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["RU"] = 9] = "RU";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["IN"] = 10] = "IN";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["NZ_865"] = 11] = "NZ_865";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["TH"] = 12] = "TH";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["LORA_24"] = 13] = "LORA_24";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["UA_433"] = 14] = "UA_433";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["UA_868"] = 15] = "UA_868";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["MY_433"] = 16] = "MY_433";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["MY_919"] = 17] = "MY_919";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["SG_923"] = 18] = "SG_923";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["PH_433"] = 19] = "PH_433";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["PH_868"] = 20] = "PH_868";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["PH_915"] = 21] = "PH_915";
  Config_LoRaConfig_RegionCode$1[Config_LoRaConfig_RegionCode$1["ANZ_433"] = 22] = "ANZ_433";
  return Config_LoRaConfig_RegionCode$1;
}({});
var Config_LoRaConfig_RegionCodeSchema = /* @__PURE__ */ enumDesc(file_config, 0, 5, 0);
var Config_LoRaConfig_ModemPreset = /* @__PURE__ */ function(Config_LoRaConfig_ModemPreset$1) {
  Config_LoRaConfig_ModemPreset$1[Config_LoRaConfig_ModemPreset$1["LONG_FAST"] = 0] = "LONG_FAST";
  Config_LoRaConfig_ModemPreset$1[Config_LoRaConfig_ModemPreset$1["LONG_SLOW"] = 1] = "LONG_SLOW";
  Config_LoRaConfig_ModemPreset$1[Config_LoRaConfig_ModemPreset$1["VERY_LONG_SLOW"] = 2] = "VERY_LONG_SLOW";
  Config_LoRaConfig_ModemPreset$1[Config_LoRaConfig_ModemPreset$1["MEDIUM_SLOW"] = 3] = "MEDIUM_SLOW";
  Config_LoRaConfig_ModemPreset$1[Config_LoRaConfig_ModemPreset$1["MEDIUM_FAST"] = 4] = "MEDIUM_FAST";
  Config_LoRaConfig_ModemPreset$1[Config_LoRaConfig_ModemPreset$1["SHORT_SLOW"] = 5] = "SHORT_SLOW";
  Config_LoRaConfig_ModemPreset$1[Config_LoRaConfig_ModemPreset$1["SHORT_FAST"] = 6] = "SHORT_FAST";
  Config_LoRaConfig_ModemPreset$1[Config_LoRaConfig_ModemPreset$1["LONG_MODERATE"] = 7] = "LONG_MODERATE";
  Config_LoRaConfig_ModemPreset$1[Config_LoRaConfig_ModemPreset$1["SHORT_TURBO"] = 8] = "SHORT_TURBO";
  return Config_LoRaConfig_ModemPreset$1;
}({});
var Config_LoRaConfig_ModemPresetSchema = /* @__PURE__ */ enumDesc(file_config, 0, 5, 1);
var Config_BluetoothConfigSchema = /* @__PURE__ */ messageDesc(file_config, 0, 6);
var Config_BluetoothConfig_PairingMode = /* @__PURE__ */ function(Config_BluetoothConfig_PairingMode$1) {
  Config_BluetoothConfig_PairingMode$1[Config_BluetoothConfig_PairingMode$1["RANDOM_PIN"] = 0] = "RANDOM_PIN";
  Config_BluetoothConfig_PairingMode$1[Config_BluetoothConfig_PairingMode$1["FIXED_PIN"] = 1] = "FIXED_PIN";
  Config_BluetoothConfig_PairingMode$1[Config_BluetoothConfig_PairingMode$1["NO_PIN"] = 2] = "NO_PIN";
  return Config_BluetoothConfig_PairingMode$1;
}({});
var Config_BluetoothConfig_PairingModeSchema = /* @__PURE__ */ enumDesc(file_config, 0, 6, 0);
var Config_SecurityConfigSchema = /* @__PURE__ */ messageDesc(file_config, 0, 7);
var Config_SessionkeyConfigSchema = /* @__PURE__ */ messageDesc(file_config, 0, 8);
var connection_status_pb_exports = __export2({
  BluetoothConnectionStatusSchema: () => BluetoothConnectionStatusSchema,
  DeviceConnectionStatusSchema: () => DeviceConnectionStatusSchema,
  EthernetConnectionStatusSchema: () => EthernetConnectionStatusSchema,
  NetworkConnectionStatusSchema: () => NetworkConnectionStatusSchema,
  SerialConnectionStatusSchema: () => SerialConnectionStatusSchema,
  WifiConnectionStatusSchema: () => WifiConnectionStatusSchema,
  file_connection_status: () => file_connection_status
});
var file_connection_status = /* @__PURE__ */ fileDesc("Chdjb25uZWN0aW9uX3N0YXR1cy5wcm90bxIKbWVzaHRhc3RpYyKxAgoWRGV2aWNlQ29ubmVjdGlvblN0YXR1cxIzCgR3aWZpGAEgASgLMiAubWVzaHRhc3RpYy5XaWZpQ29ubmVjdGlvblN0YXR1c0gAiAEBEjsKCGV0aGVybmV0GAIgASgLMiQubWVzaHRhc3RpYy5FdGhlcm5ldENvbm5lY3Rpb25TdGF0dXNIAYgBARI9CglibHVldG9vdGgYAyABKAsyJS5tZXNodGFzdGljLkJsdWV0b290aENvbm5lY3Rpb25TdGF0dXNIAogBARI3CgZzZXJpYWwYBCABKAsyIi5tZXNodGFzdGljLlNlcmlhbENvbm5lY3Rpb25TdGF0dXNIA4gBAUIHCgVfd2lmaUILCglfZXRoZXJuZXRCDAoKX2JsdWV0b290aEIJCgdfc2VyaWFsImcKFFdpZmlDb25uZWN0aW9uU3RhdHVzEjMKBnN0YXR1cxgBIAEoCzIjLm1lc2h0YXN0aWMuTmV0d29ya0Nvbm5lY3Rpb25TdGF0dXMSDAoEc3NpZBgCIAEoCRIMCgRyc3NpGAMgASgFIk8KGEV0aGVybmV0Q29ubmVjdGlvblN0YXR1cxIzCgZzdGF0dXMYASABKAsyIy5tZXNodGFzdGljLk5ldHdvcmtDb25uZWN0aW9uU3RhdHVzInsKF05ldHdvcmtDb25uZWN0aW9uU3RhdHVzEhIKCmlwX2FkZHJlc3MYASABKAcSFAoMaXNfY29ubmVjdGVkGAIgASgIEhkKEWlzX21xdHRfY29ubmVjdGVkGAMgASgIEhsKE2lzX3N5c2xvZ19jb25uZWN0ZWQYBCABKAgiTAoZQmx1ZXRvb3RoQ29ubmVjdGlvblN0YXR1cxILCgNwaW4YASABKA0SDAoEcnNzaRgCIAEoBRIUCgxpc19jb25uZWN0ZWQYAyABKAgiPAoWU2VyaWFsQ29ubmVjdGlvblN0YXR1cxIMCgRiYXVkGAEgASgNEhQKDGlzX2Nvbm5lY3RlZBgCIAEoCEJlChNjb20uZ2Vla3N2aWxsZS5tZXNoQhBDb25uU3RhdHVzUHJvdG9zWiJnaXRodWIuY29tL21lc2h0YXN0aWMvZ28vZ2VuZXJhdGVkqgIUTWVzaHRhc3RpYy5Qcm90b2J1ZnO6AgBiBnByb3RvMw");
var DeviceConnectionStatusSchema = /* @__PURE__ */ messageDesc(file_connection_status, 0);
var WifiConnectionStatusSchema = /* @__PURE__ */ messageDesc(file_connection_status, 1);
var EthernetConnectionStatusSchema = /* @__PURE__ */ messageDesc(file_connection_status, 2);
var NetworkConnectionStatusSchema = /* @__PURE__ */ messageDesc(file_connection_status, 3);
var BluetoothConnectionStatusSchema = /* @__PURE__ */ messageDesc(file_connection_status, 4);
var SerialConnectionStatusSchema = /* @__PURE__ */ messageDesc(file_connection_status, 5);
var module_config_pb_exports = __export2({
  ModuleConfigSchema: () => ModuleConfigSchema,
  ModuleConfig_AmbientLightingConfigSchema: () => ModuleConfig_AmbientLightingConfigSchema,
  ModuleConfig_AudioConfigSchema: () => ModuleConfig_AudioConfigSchema,
  ModuleConfig_AudioConfig_Audio_Baud: () => ModuleConfig_AudioConfig_Audio_Baud,
  ModuleConfig_AudioConfig_Audio_BaudSchema: () => ModuleConfig_AudioConfig_Audio_BaudSchema,
  ModuleConfig_CannedMessageConfigSchema: () => ModuleConfig_CannedMessageConfigSchema,
  ModuleConfig_CannedMessageConfig_InputEventChar: () => ModuleConfig_CannedMessageConfig_InputEventChar,
  ModuleConfig_CannedMessageConfig_InputEventCharSchema: () => ModuleConfig_CannedMessageConfig_InputEventCharSchema,
  ModuleConfig_DetectionSensorConfigSchema: () => ModuleConfig_DetectionSensorConfigSchema,
  ModuleConfig_DetectionSensorConfig_TriggerType: () => ModuleConfig_DetectionSensorConfig_TriggerType,
  ModuleConfig_DetectionSensorConfig_TriggerTypeSchema: () => ModuleConfig_DetectionSensorConfig_TriggerTypeSchema,
  ModuleConfig_ExternalNotificationConfigSchema: () => ModuleConfig_ExternalNotificationConfigSchema,
  ModuleConfig_MQTTConfigSchema: () => ModuleConfig_MQTTConfigSchema,
  ModuleConfig_MapReportSettingsSchema: () => ModuleConfig_MapReportSettingsSchema,
  ModuleConfig_NeighborInfoConfigSchema: () => ModuleConfig_NeighborInfoConfigSchema,
  ModuleConfig_PaxcounterConfigSchema: () => ModuleConfig_PaxcounterConfigSchema,
  ModuleConfig_RangeTestConfigSchema: () => ModuleConfig_RangeTestConfigSchema,
  ModuleConfig_RemoteHardwareConfigSchema: () => ModuleConfig_RemoteHardwareConfigSchema,
  ModuleConfig_SerialConfigSchema: () => ModuleConfig_SerialConfigSchema,
  ModuleConfig_SerialConfig_Serial_Baud: () => ModuleConfig_SerialConfig_Serial_Baud,
  ModuleConfig_SerialConfig_Serial_BaudSchema: () => ModuleConfig_SerialConfig_Serial_BaudSchema,
  ModuleConfig_SerialConfig_Serial_Mode: () => ModuleConfig_SerialConfig_Serial_Mode,
  ModuleConfig_SerialConfig_Serial_ModeSchema: () => ModuleConfig_SerialConfig_Serial_ModeSchema,
  ModuleConfig_StoreForwardConfigSchema: () => ModuleConfig_StoreForwardConfigSchema,
  ModuleConfig_TelemetryConfigSchema: () => ModuleConfig_TelemetryConfigSchema,
  RemoteHardwarePinSchema: () => RemoteHardwarePinSchema,
  RemoteHardwarePinType: () => RemoteHardwarePinType,
  RemoteHardwarePinTypeSchema: () => RemoteHardwarePinTypeSchema,
  file_module_config: () => file_module_config
});
var file_module_config = /* @__PURE__ */ fileDesc("ChNtb2R1bGVfY29uZmlnLnByb3RvEgptZXNodGFzdGljIuMlCgxNb2R1bGVDb25maWcSMwoEbXF0dBgBIAEoCzIjLm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnLk1RVFRDb25maWdIABI3CgZzZXJpYWwYAiABKAsyJS5tZXNodGFzdGljLk1vZHVsZUNvbmZpZy5TZXJpYWxDb25maWdIABJUChVleHRlcm5hbF9ub3RpZmljYXRpb24YAyABKAsyMy5tZXNodGFzdGljLk1vZHVsZUNvbmZpZy5FeHRlcm5hbE5vdGlmaWNhdGlvbkNvbmZpZ0gAEkQKDXN0b3JlX2ZvcndhcmQYBCABKAsyKy5tZXNodGFzdGljLk1vZHVsZUNvbmZpZy5TdG9yZUZvcndhcmRDb25maWdIABI+CgpyYW5nZV90ZXN0GAUgASgLMigubWVzaHRhc3RpYy5Nb2R1bGVDb25maWcuUmFuZ2VUZXN0Q29uZmlnSAASPQoJdGVsZW1ldHJ5GAYgASgLMigubWVzaHRhc3RpYy5Nb2R1bGVDb25maWcuVGVsZW1ldHJ5Q29uZmlnSAASRgoOY2FubmVkX21lc3NhZ2UYByABKAsyLC5tZXNodGFzdGljLk1vZHVsZUNvbmZpZy5DYW5uZWRNZXNzYWdlQ29uZmlnSAASNQoFYXVkaW8YCCABKAsyJC5tZXNodGFzdGljLk1vZHVsZUNvbmZpZy5BdWRpb0NvbmZpZ0gAEkgKD3JlbW90ZV9oYXJkd2FyZRgJIAEoCzItLm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnLlJlbW90ZUhhcmR3YXJlQ29uZmlnSAASRAoNbmVpZ2hib3JfaW5mbxgKIAEoCzIrLm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnLk5laWdoYm9ySW5mb0NvbmZpZ0gAEkoKEGFtYmllbnRfbGlnaHRpbmcYCyABKAsyLi5tZXNodGFzdGljLk1vZHVsZUNvbmZpZy5BbWJpZW50TGlnaHRpbmdDb25maWdIABJKChBkZXRlY3Rpb25fc2Vuc29yGAwgASgLMi4ubWVzaHRhc3RpYy5Nb2R1bGVDb25maWcuRGV0ZWN0aW9uU2Vuc29yQ29uZmlnSAASPwoKcGF4Y291bnRlchgNIAEoCzIpLm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnLlBheGNvdW50ZXJDb25maWdIABqwAgoKTVFUVENvbmZpZxIPCgdlbmFibGVkGAEgASgIEg8KB2FkZHJlc3MYAiABKAkSEAoIdXNlcm5hbWUYAyABKAkSEAoIcGFzc3dvcmQYBCABKAkSGgoSZW5jcnlwdGlvbl9lbmFibGVkGAUgASgIEhQKDGpzb25fZW5hYmxlZBgGIAEoCBITCgt0bHNfZW5hYmxlZBgHIAEoCBIMCgRyb290GAggASgJEh8KF3Byb3h5X3RvX2NsaWVudF9lbmFibGVkGAkgASgIEh0KFW1hcF9yZXBvcnRpbmdfZW5hYmxlZBgKIAEoCBJHChNtYXBfcmVwb3J0X3NldHRpbmdzGAsgASgLMioubWVzaHRhc3RpYy5Nb2R1bGVDb25maWcuTWFwUmVwb3J0U2V0dGluZ3MabgoRTWFwUmVwb3J0U2V0dGluZ3MSHQoVcHVibGlzaF9pbnRlcnZhbF9zZWNzGAEgASgNEhoKEnBvc2l0aW9uX3ByZWNpc2lvbhgCIAEoDRIeChZzaG91bGRfcmVwb3J0X2xvY2F0aW9uGAMgASgIGoIBChRSZW1vdGVIYXJkd2FyZUNvbmZpZxIPCgdlbmFibGVkGAEgASgIEiIKGmFsbG93X3VuZGVmaW5lZF9waW5fYWNjZXNzGAIgASgIEjUKDmF2YWlsYWJsZV9waW5zGAMgAygLMh0ubWVzaHRhc3RpYy5SZW1vdGVIYXJkd2FyZVBpbhpaChJOZWlnaGJvckluZm9Db25maWcSDwoHZW5hYmxlZBgBIAEoCBIXCg91cGRhdGVfaW50ZXJ2YWwYAiABKA0SGgoSdHJhbnNtaXRfb3Zlcl9sb3JhGAMgASgIGpcDChVEZXRlY3Rpb25TZW5zb3JDb25maWcSDwoHZW5hYmxlZBgBIAEoCBIeChZtaW5pbXVtX2Jyb2FkY2FzdF9zZWNzGAIgASgNEhwKFHN0YXRlX2Jyb2FkY2FzdF9zZWNzGAMgASgNEhEKCXNlbmRfYmVsbBgEIAEoCBIMCgRuYW1lGAUgASgJEhMKC21vbml0b3JfcGluGAYgASgNEloKFmRldGVjdGlvbl90cmlnZ2VyX3R5cGUYByABKA4yOi5tZXNodGFzdGljLk1vZHVsZUNvbmZpZy5EZXRlY3Rpb25TZW5zb3JDb25maWcuVHJpZ2dlclR5cGUSEgoKdXNlX3B1bGx1cBgIIAEoCCKIAQoLVHJpZ2dlclR5cGUSDQoJTE9HSUNfTE9XEAASDgoKTE9HSUNfSElHSBABEhAKDEZBTExJTkdfRURHRRACEg8KC1JJU0lOR19FREdFEAMSGgoWRUlUSEVSX0VER0VfQUNUSVZFX0xPVxAEEhsKF0VJVEhFUl9FREdFX0FDVElWRV9ISUdIEAUa5AIKC0F1ZGlvQ29uZmlnEhYKDmNvZGVjMl9lbmFibGVkGAEgASgIEg8KB3B0dF9waW4YAiABKA0SQAoHYml0cmF0ZRgDIAEoDjIvLm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnLkF1ZGlvQ29uZmlnLkF1ZGlvX0JhdWQSDgoGaTJzX3dzGAQgASgNEg4KBmkyc19zZBgFIAEoDRIPCgdpMnNfZGluGAYgASgNEg8KB2kyc19zY2sYByABKA0ipwEKCkF1ZGlvX0JhdWQSEgoOQ09ERUMyX0RFRkFVTFQQABIPCgtDT0RFQzJfMzIwMBABEg8KC0NPREVDMl8yNDAwEAISDwoLQ09ERUMyXzE2MDAQAxIPCgtDT0RFQzJfMTQwMBAEEg8KC0NPREVDMl8xMzAwEAUSDwoLQ09ERUMyXzEyMDAQBhIOCgpDT0RFQzJfNzAwEAcSDwoLQ09ERUMyXzcwMEIQCBp2ChBQYXhjb3VudGVyQ29uZmlnEg8KB2VuYWJsZWQYASABKAgSIgoacGF4Y291bnRlcl91cGRhdGVfaW50ZXJ2YWwYAiABKA0SFgoOd2lmaV90aHJlc2hvbGQYAyABKAUSFQoNYmxlX3RocmVzaG9sZBgEIAEoBRr9BAoMU2VyaWFsQ29uZmlnEg8KB2VuYWJsZWQYASABKAgSDAoEZWNobxgCIAEoCBILCgNyeGQYAyABKA0SCwoDdHhkGAQgASgNEj8KBGJhdWQYBSABKA4yMS5tZXNodGFzdGljLk1vZHVsZUNvbmZpZy5TZXJpYWxDb25maWcuU2VyaWFsX0JhdWQSDwoHdGltZW91dBgGIAEoDRI/CgRtb2RlGAcgASgOMjEubWVzaHRhc3RpYy5Nb2R1bGVDb25maWcuU2VyaWFsQ29uZmlnLlNlcmlhbF9Nb2RlEiQKHG92ZXJyaWRlX2NvbnNvbGVfc2VyaWFsX3BvcnQYCCABKAgiigIKC1NlcmlhbF9CYXVkEhAKDEJBVURfREVGQVVMVBAAEgwKCEJBVURfMTEwEAESDAoIQkFVRF8zMDAQAhIMCghCQVVEXzYwMBADEg0KCUJBVURfMTIwMBAEEg0KCUJBVURfMjQwMBAFEg0KCUJBVURfNDgwMBAGEg0KCUJBVURfOTYwMBAHEg4KCkJBVURfMTkyMDAQCBIOCgpCQVVEXzM4NDAwEAkSDgoKQkFVRF81NzYwMBAKEg8KC0JBVURfMTE1MjAwEAsSDwoLQkFVRF8yMzA0MDAQDBIPCgtCQVVEXzQ2MDgwMBANEg8KC0JBVURfNTc2MDAwEA4SDwoLQkFVRF85MjE2MDAQDyJuCgtTZXJpYWxfTW9kZRILCgdERUZBVUxUEAASCgoGU0lNUExFEAESCQoFUFJPVE8QAhILCgdURVhUTVNHEAMSCAoETk1FQRAEEgsKB0NBTFRPUE8QBRIICgRXUzg1EAYSDQoJVkVfRElSRUNUEAca6QIKGkV4dGVybmFsTm90aWZpY2F0aW9uQ29uZmlnEg8KB2VuYWJsZWQYASABKAgSEQoJb3V0cHV0X21zGAIgASgNEg4KBm91dHB1dBgDIAEoDRIUCgxvdXRwdXRfdmlicmEYCCABKA0SFQoNb3V0cHV0X2J1enplchgJIAEoDRIOCgZhY3RpdmUYBCABKAgSFQoNYWxlcnRfbWVzc2FnZRgFIAEoCBIbChNhbGVydF9tZXNzYWdlX3ZpYnJhGAogASgIEhwKFGFsZXJ0X21lc3NhZ2VfYnV6emVyGAsgASgIEhIKCmFsZXJ0X2JlbGwYBiABKAgSGAoQYWxlcnRfYmVsbF92aWJyYRgMIAEoCBIZChFhbGVydF9iZWxsX2J1enplchgNIAEoCBIPCgd1c2VfcHdtGAcgASgIEhMKC25hZ190aW1lb3V0GA4gASgNEhkKEXVzZV9pMnNfYXNfYnV6emVyGA8gASgIGpcBChJTdG9yZUZvcndhcmRDb25maWcSDwoHZW5hYmxlZBgBIAEoCBIRCgloZWFydGJlYXQYAiABKAgSDwoHcmVjb3JkcxgDIAEoDRIaChJoaXN0b3J5X3JldHVybl9tYXgYBCABKA0SHQoVaGlzdG9yeV9yZXR1cm5fd2luZG93GAUgASgNEhEKCWlzX3NlcnZlchgGIAEoCBpACg9SYW5nZVRlc3RDb25maWcSDwoHZW5hYmxlZBgBIAEoCBIOCgZzZW5kZXIYAiABKA0SDAoEc2F2ZRgDIAEoCBrJAwoPVGVsZW1ldHJ5Q29uZmlnEh4KFmRldmljZV91cGRhdGVfaW50ZXJ2YWwYASABKA0SIwobZW52aXJvbm1lbnRfdXBkYXRlX2ludGVydmFsGAIgASgNEicKH2Vudmlyb25tZW50X21lYXN1cmVtZW50X2VuYWJsZWQYAyABKAgSIgoaZW52aXJvbm1lbnRfc2NyZWVuX2VuYWJsZWQYBCABKAgSJgoeZW52aXJvbm1lbnRfZGlzcGxheV9mYWhyZW5oZWl0GAUgASgIEhsKE2Fpcl9xdWFsaXR5X2VuYWJsZWQYBiABKAgSHAoUYWlyX3F1YWxpdHlfaW50ZXJ2YWwYByABKA0SIQoZcG93ZXJfbWVhc3VyZW1lbnRfZW5hYmxlZBgIIAEoCBIdChVwb3dlcl91cGRhdGVfaW50ZXJ2YWwYCSABKA0SHAoUcG93ZXJfc2NyZWVuX2VuYWJsZWQYCiABKAgSIgoaaGVhbHRoX21lYXN1cmVtZW50X2VuYWJsZWQYCyABKAgSHgoWaGVhbHRoX3VwZGF0ZV9pbnRlcnZhbBgMIAEoDRIdChVoZWFsdGhfc2NyZWVuX2VuYWJsZWQYDSABKAga1gQKE0Nhbm5lZE1lc3NhZ2VDb25maWcSFwoPcm90YXJ5MV9lbmFibGVkGAEgASgIEhkKEWlucHV0YnJva2VyX3Bpbl9hGAIgASgNEhkKEWlucHV0YnJva2VyX3Bpbl9iGAMgASgNEh0KFWlucHV0YnJva2VyX3Bpbl9wcmVzcxgEIAEoDRJZChRpbnB1dGJyb2tlcl9ldmVudF9jdxgFIAEoDjI7Lm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnLkNhbm5lZE1lc3NhZ2VDb25maWcuSW5wdXRFdmVudENoYXISWgoVaW5wdXRicm9rZXJfZXZlbnRfY2N3GAYgASgOMjsubWVzaHRhc3RpYy5Nb2R1bGVDb25maWcuQ2FubmVkTWVzc2FnZUNvbmZpZy5JbnB1dEV2ZW50Q2hhchJcChdpbnB1dGJyb2tlcl9ldmVudF9wcmVzcxgHIAEoDjI7Lm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnLkNhbm5lZE1lc3NhZ2VDb25maWcuSW5wdXRFdmVudENoYXISFwoPdXBkb3duMV9lbmFibGVkGAggASgIEg8KB2VuYWJsZWQYCSABKAgSGgoSYWxsb3dfaW5wdXRfc291cmNlGAogASgJEhEKCXNlbmRfYmVsbBgLIAEoCCJjCg5JbnB1dEV2ZW50Q2hhchIICgROT05FEAASBgoCVVAQERIICgRET1dOEBISCAoETEVGVBATEgkKBVJJR0hUEBQSCgoGU0VMRUNUEAoSCAoEQkFDSxAbEgoKBkNBTkNFTBAYGmUKFUFtYmllbnRMaWdodGluZ0NvbmZpZxIRCglsZWRfc3RhdGUYASABKAgSDwoHY3VycmVudBgCIAEoDRILCgNyZWQYAyABKA0SDQoFZ3JlZW4YBCABKA0SDAoEYmx1ZRgFIAEoDUIRCg9wYXlsb2FkX3ZhcmlhbnQiZAoRUmVtb3RlSGFyZHdhcmVQaW4SEAoIZ3Bpb19waW4YASABKA0SDAoEbmFtZRgCIAEoCRIvCgR0eXBlGAMgASgOMiEubWVzaHRhc3RpYy5SZW1vdGVIYXJkd2FyZVBpblR5cGUqSQoVUmVtb3RlSGFyZHdhcmVQaW5UeXBlEgsKB1VOS05PV04QABIQCgxESUdJVEFMX1JFQUQQARIRCg1ESUdJVEFMX1dSSVRFEAJCZwoTY29tLmdlZWtzdmlsbGUubWVzaEISTW9kdWxlQ29uZmlnUHJvdG9zWiJnaXRodWIuY29tL21lc2h0YXN0aWMvZ28vZ2VuZXJhdGVkqgIUTWVzaHRhc3RpYy5Qcm90b2J1ZnO6AgBiBnByb3RvMw");
var ModuleConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0);
var ModuleConfig_MQTTConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 0);
var ModuleConfig_MapReportSettingsSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 1);
var ModuleConfig_RemoteHardwareConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 2);
var ModuleConfig_NeighborInfoConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 3);
var ModuleConfig_DetectionSensorConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 4);
var ModuleConfig_DetectionSensorConfig_TriggerType = /* @__PURE__ */ function(ModuleConfig_DetectionSensorConfig_TriggerType$1) {
  ModuleConfig_DetectionSensorConfig_TriggerType$1[ModuleConfig_DetectionSensorConfig_TriggerType$1["LOGIC_LOW"] = 0] = "LOGIC_LOW";
  ModuleConfig_DetectionSensorConfig_TriggerType$1[ModuleConfig_DetectionSensorConfig_TriggerType$1["LOGIC_HIGH"] = 1] = "LOGIC_HIGH";
  ModuleConfig_DetectionSensorConfig_TriggerType$1[ModuleConfig_DetectionSensorConfig_TriggerType$1["FALLING_EDGE"] = 2] = "FALLING_EDGE";
  ModuleConfig_DetectionSensorConfig_TriggerType$1[ModuleConfig_DetectionSensorConfig_TriggerType$1["RISING_EDGE"] = 3] = "RISING_EDGE";
  ModuleConfig_DetectionSensorConfig_TriggerType$1[ModuleConfig_DetectionSensorConfig_TriggerType$1["EITHER_EDGE_ACTIVE_LOW"] = 4] = "EITHER_EDGE_ACTIVE_LOW";
  ModuleConfig_DetectionSensorConfig_TriggerType$1[ModuleConfig_DetectionSensorConfig_TriggerType$1["EITHER_EDGE_ACTIVE_HIGH"] = 5] = "EITHER_EDGE_ACTIVE_HIGH";
  return ModuleConfig_DetectionSensorConfig_TriggerType$1;
}({});
var ModuleConfig_DetectionSensorConfig_TriggerTypeSchema = /* @__PURE__ */ enumDesc(file_module_config, 0, 4, 0);
var ModuleConfig_AudioConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 5);
var ModuleConfig_AudioConfig_Audio_Baud = /* @__PURE__ */ function(ModuleConfig_AudioConfig_Audio_Baud$1) {
  ModuleConfig_AudioConfig_Audio_Baud$1[ModuleConfig_AudioConfig_Audio_Baud$1["CODEC2_DEFAULT"] = 0] = "CODEC2_DEFAULT";
  ModuleConfig_AudioConfig_Audio_Baud$1[ModuleConfig_AudioConfig_Audio_Baud$1["CODEC2_3200"] = 1] = "CODEC2_3200";
  ModuleConfig_AudioConfig_Audio_Baud$1[ModuleConfig_AudioConfig_Audio_Baud$1["CODEC2_2400"] = 2] = "CODEC2_2400";
  ModuleConfig_AudioConfig_Audio_Baud$1[ModuleConfig_AudioConfig_Audio_Baud$1["CODEC2_1600"] = 3] = "CODEC2_1600";
  ModuleConfig_AudioConfig_Audio_Baud$1[ModuleConfig_AudioConfig_Audio_Baud$1["CODEC2_1400"] = 4] = "CODEC2_1400";
  ModuleConfig_AudioConfig_Audio_Baud$1[ModuleConfig_AudioConfig_Audio_Baud$1["CODEC2_1300"] = 5] = "CODEC2_1300";
  ModuleConfig_AudioConfig_Audio_Baud$1[ModuleConfig_AudioConfig_Audio_Baud$1["CODEC2_1200"] = 6] = "CODEC2_1200";
  ModuleConfig_AudioConfig_Audio_Baud$1[ModuleConfig_AudioConfig_Audio_Baud$1["CODEC2_700"] = 7] = "CODEC2_700";
  ModuleConfig_AudioConfig_Audio_Baud$1[ModuleConfig_AudioConfig_Audio_Baud$1["CODEC2_700B"] = 8] = "CODEC2_700B";
  return ModuleConfig_AudioConfig_Audio_Baud$1;
}({});
var ModuleConfig_AudioConfig_Audio_BaudSchema = /* @__PURE__ */ enumDesc(file_module_config, 0, 5, 0);
var ModuleConfig_PaxcounterConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 6);
var ModuleConfig_SerialConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 7);
var ModuleConfig_SerialConfig_Serial_Baud = /* @__PURE__ */ function(ModuleConfig_SerialConfig_Serial_Baud$1) {
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_DEFAULT"] = 0] = "BAUD_DEFAULT";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_110"] = 1] = "BAUD_110";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_300"] = 2] = "BAUD_300";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_600"] = 3] = "BAUD_600";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_1200"] = 4] = "BAUD_1200";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_2400"] = 5] = "BAUD_2400";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_4800"] = 6] = "BAUD_4800";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_9600"] = 7] = "BAUD_9600";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_19200"] = 8] = "BAUD_19200";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_38400"] = 9] = "BAUD_38400";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_57600"] = 10] = "BAUD_57600";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_115200"] = 11] = "BAUD_115200";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_230400"] = 12] = "BAUD_230400";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_460800"] = 13] = "BAUD_460800";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_576000"] = 14] = "BAUD_576000";
  ModuleConfig_SerialConfig_Serial_Baud$1[ModuleConfig_SerialConfig_Serial_Baud$1["BAUD_921600"] = 15] = "BAUD_921600";
  return ModuleConfig_SerialConfig_Serial_Baud$1;
}({});
var ModuleConfig_SerialConfig_Serial_BaudSchema = /* @__PURE__ */ enumDesc(file_module_config, 0, 7, 0);
var ModuleConfig_SerialConfig_Serial_Mode = /* @__PURE__ */ function(ModuleConfig_SerialConfig_Serial_Mode$1) {
  ModuleConfig_SerialConfig_Serial_Mode$1[ModuleConfig_SerialConfig_Serial_Mode$1["DEFAULT"] = 0] = "DEFAULT";
  ModuleConfig_SerialConfig_Serial_Mode$1[ModuleConfig_SerialConfig_Serial_Mode$1["SIMPLE"] = 1] = "SIMPLE";
  ModuleConfig_SerialConfig_Serial_Mode$1[ModuleConfig_SerialConfig_Serial_Mode$1["PROTO"] = 2] = "PROTO";
  ModuleConfig_SerialConfig_Serial_Mode$1[ModuleConfig_SerialConfig_Serial_Mode$1["TEXTMSG"] = 3] = "TEXTMSG";
  ModuleConfig_SerialConfig_Serial_Mode$1[ModuleConfig_SerialConfig_Serial_Mode$1["NMEA"] = 4] = "NMEA";
  ModuleConfig_SerialConfig_Serial_Mode$1[ModuleConfig_SerialConfig_Serial_Mode$1["CALTOPO"] = 5] = "CALTOPO";
  ModuleConfig_SerialConfig_Serial_Mode$1[ModuleConfig_SerialConfig_Serial_Mode$1["WS85"] = 6] = "WS85";
  ModuleConfig_SerialConfig_Serial_Mode$1[ModuleConfig_SerialConfig_Serial_Mode$1["VE_DIRECT"] = 7] = "VE_DIRECT";
  return ModuleConfig_SerialConfig_Serial_Mode$1;
}({});
var ModuleConfig_SerialConfig_Serial_ModeSchema = /* @__PURE__ */ enumDesc(file_module_config, 0, 7, 1);
var ModuleConfig_ExternalNotificationConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 8);
var ModuleConfig_StoreForwardConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 9);
var ModuleConfig_RangeTestConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 10);
var ModuleConfig_TelemetryConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 11);
var ModuleConfig_CannedMessageConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 12);
var ModuleConfig_CannedMessageConfig_InputEventChar = /* @__PURE__ */ function(ModuleConfig_CannedMessageConfig_InputEventChar$1) {
  ModuleConfig_CannedMessageConfig_InputEventChar$1[ModuleConfig_CannedMessageConfig_InputEventChar$1["NONE"] = 0] = "NONE";
  ModuleConfig_CannedMessageConfig_InputEventChar$1[ModuleConfig_CannedMessageConfig_InputEventChar$1["UP"] = 17] = "UP";
  ModuleConfig_CannedMessageConfig_InputEventChar$1[ModuleConfig_CannedMessageConfig_InputEventChar$1["DOWN"] = 18] = "DOWN";
  ModuleConfig_CannedMessageConfig_InputEventChar$1[ModuleConfig_CannedMessageConfig_InputEventChar$1["LEFT"] = 19] = "LEFT";
  ModuleConfig_CannedMessageConfig_InputEventChar$1[ModuleConfig_CannedMessageConfig_InputEventChar$1["RIGHT"] = 20] = "RIGHT";
  ModuleConfig_CannedMessageConfig_InputEventChar$1[ModuleConfig_CannedMessageConfig_InputEventChar$1["SELECT"] = 10] = "SELECT";
  ModuleConfig_CannedMessageConfig_InputEventChar$1[ModuleConfig_CannedMessageConfig_InputEventChar$1["BACK"] = 27] = "BACK";
  ModuleConfig_CannedMessageConfig_InputEventChar$1[ModuleConfig_CannedMessageConfig_InputEventChar$1["CANCEL"] = 24] = "CANCEL";
  return ModuleConfig_CannedMessageConfig_InputEventChar$1;
}({});
var ModuleConfig_CannedMessageConfig_InputEventCharSchema = /* @__PURE__ */ enumDesc(file_module_config, 0, 12, 0);
var ModuleConfig_AmbientLightingConfigSchema = /* @__PURE__ */ messageDesc(file_module_config, 0, 13);
var RemoteHardwarePinSchema = /* @__PURE__ */ messageDesc(file_module_config, 1);
var RemoteHardwarePinType = /* @__PURE__ */ function(RemoteHardwarePinType$1) {
  RemoteHardwarePinType$1[RemoteHardwarePinType$1["UNKNOWN"] = 0] = "UNKNOWN";
  RemoteHardwarePinType$1[RemoteHardwarePinType$1["DIGITAL_READ"] = 1] = "DIGITAL_READ";
  RemoteHardwarePinType$1[RemoteHardwarePinType$1["DIGITAL_WRITE"] = 2] = "DIGITAL_WRITE";
  return RemoteHardwarePinType$1;
}({});
var RemoteHardwarePinTypeSchema = /* @__PURE__ */ enumDesc(file_module_config, 0);
var portnums_pb_exports = __export2({
  PortNum: () => PortNum,
  PortNumSchema: () => PortNumSchema,
  file_portnums: () => file_portnums
});
var file_portnums = /* @__PURE__ */ fileDesc("Cg5wb3J0bnVtcy5wcm90bxIKbWVzaHRhc3RpYyrlBAoHUG9ydE51bRIPCgtVTktOT1dOX0FQUBAAEhQKEFRFWFRfTUVTU0FHRV9BUFAQARIXChNSRU1PVEVfSEFSRFdBUkVfQVBQEAISEAoMUE9TSVRJT05fQVBQEAMSEAoMTk9ERUlORk9fQVBQEAQSDwoLUk9VVElOR19BUFAQBRINCglBRE1JTl9BUFAQBhIfChtURVhUX01FU1NBR0VfQ09NUFJFU1NFRF9BUFAQBxIQCgxXQVlQT0lOVF9BUFAQCBINCglBVURJT19BUFAQCRIYChRERVRFQ1RJT05fU0VOU09SX0FQUBAKEg0KCUFMRVJUX0FQUBALEhgKFEtFWV9WRVJJRklDQVRJT05fQVBQEAwSDQoJUkVQTFlfQVBQECASEQoNSVBfVFVOTkVMX0FQUBAhEhIKDlBBWENPVU5URVJfQVBQECISDgoKU0VSSUFMX0FQUBBAEhUKEVNUT1JFX0ZPUldBUkRfQVBQEEESEgoOUkFOR0VfVEVTVF9BUFAQQhIRCg1URUxFTUVUUllfQVBQEEMSCwoHWlBTX0FQUBBEEhEKDVNJTVVMQVRPUl9BUFAQRRISCg5UUkFDRVJPVVRFX0FQUBBGEhQKEE5FSUdIQk9SSU5GT19BUFAQRxIPCgtBVEFLX1BMVUdJThBIEhIKDk1BUF9SRVBPUlRfQVBQEEkSEwoPUE9XRVJTVFJFU1NfQVBQEEoSGAoUUkVUSUNVTFVNX1RVTk5FTF9BUFAQTBIQCgtQUklWQVRFX0FQUBCAAhITCg5BVEFLX0ZPUldBUkRFUhCBAhIICgNNQVgQ/wNCXQoTY29tLmdlZWtzdmlsbGUubWVzaEIIUG9ydG51bXNaImdpdGh1Yi5jb20vbWVzaHRhc3RpYy9nby9nZW5lcmF0ZWSqAhRNZXNodGFzdGljLlByb3RvYnVmc7oCAGIGcHJvdG8z");
var PortNum = /* @__PURE__ */ function(PortNum$1) {
  PortNum$1[PortNum$1["UNKNOWN_APP"] = 0] = "UNKNOWN_APP";
  PortNum$1[PortNum$1["TEXT_MESSAGE_APP"] = 1] = "TEXT_MESSAGE_APP";
  PortNum$1[PortNum$1["REMOTE_HARDWARE_APP"] = 2] = "REMOTE_HARDWARE_APP";
  PortNum$1[PortNum$1["POSITION_APP"] = 3] = "POSITION_APP";
  PortNum$1[PortNum$1["NODEINFO_APP"] = 4] = "NODEINFO_APP";
  PortNum$1[PortNum$1["ROUTING_APP"] = 5] = "ROUTING_APP";
  PortNum$1[PortNum$1["ADMIN_APP"] = 6] = "ADMIN_APP";
  PortNum$1[PortNum$1["TEXT_MESSAGE_COMPRESSED_APP"] = 7] = "TEXT_MESSAGE_COMPRESSED_APP";
  PortNum$1[PortNum$1["WAYPOINT_APP"] = 8] = "WAYPOINT_APP";
  PortNum$1[PortNum$1["AUDIO_APP"] = 9] = "AUDIO_APP";
  PortNum$1[PortNum$1["DETECTION_SENSOR_APP"] = 10] = "DETECTION_SENSOR_APP";
  PortNum$1[PortNum$1["ALERT_APP"] = 11] = "ALERT_APP";
  PortNum$1[PortNum$1["KEY_VERIFICATION_APP"] = 12] = "KEY_VERIFICATION_APP";
  PortNum$1[PortNum$1["REPLY_APP"] = 32] = "REPLY_APP";
  PortNum$1[PortNum$1["IP_TUNNEL_APP"] = 33] = "IP_TUNNEL_APP";
  PortNum$1[PortNum$1["PAXCOUNTER_APP"] = 34] = "PAXCOUNTER_APP";
  PortNum$1[PortNum$1["SERIAL_APP"] = 64] = "SERIAL_APP";
  PortNum$1[PortNum$1["STORE_FORWARD_APP"] = 65] = "STORE_FORWARD_APP";
  PortNum$1[PortNum$1["RANGE_TEST_APP"] = 66] = "RANGE_TEST_APP";
  PortNum$1[PortNum$1["TELEMETRY_APP"] = 67] = "TELEMETRY_APP";
  PortNum$1[PortNum$1["ZPS_APP"] = 68] = "ZPS_APP";
  PortNum$1[PortNum$1["SIMULATOR_APP"] = 69] = "SIMULATOR_APP";
  PortNum$1[PortNum$1["TRACEROUTE_APP"] = 70] = "TRACEROUTE_APP";
  PortNum$1[PortNum$1["NEIGHBORINFO_APP"] = 71] = "NEIGHBORINFO_APP";
  PortNum$1[PortNum$1["ATAK_PLUGIN"] = 72] = "ATAK_PLUGIN";
  PortNum$1[PortNum$1["MAP_REPORT_APP"] = 73] = "MAP_REPORT_APP";
  PortNum$1[PortNum$1["POWERSTRESS_APP"] = 74] = "POWERSTRESS_APP";
  PortNum$1[PortNum$1["RETICULUM_TUNNEL_APP"] = 76] = "RETICULUM_TUNNEL_APP";
  PortNum$1[PortNum$1["PRIVATE_APP"] = 256] = "PRIVATE_APP";
  PortNum$1[PortNum$1["ATAK_FORWARDER"] = 257] = "ATAK_FORWARDER";
  PortNum$1[PortNum$1["MAX"] = 511] = "MAX";
  return PortNum$1;
}({});
var PortNumSchema = /* @__PURE__ */ enumDesc(file_portnums, 0);
var telemetry_pb_exports = __export2({
  AirQualityMetricsSchema: () => AirQualityMetricsSchema,
  DeviceMetricsSchema: () => DeviceMetricsSchema,
  EnvironmentMetricsSchema: () => EnvironmentMetricsSchema,
  HealthMetricsSchema: () => HealthMetricsSchema,
  HostMetricsSchema: () => HostMetricsSchema,
  LocalStatsSchema: () => LocalStatsSchema,
  Nau7802ConfigSchema: () => Nau7802ConfigSchema,
  PowerMetricsSchema: () => PowerMetricsSchema,
  TelemetrySchema: () => TelemetrySchema,
  TelemetrySensorType: () => TelemetrySensorType,
  TelemetrySensorTypeSchema: () => TelemetrySensorTypeSchema,
  file_telemetry: () => file_telemetry
});
var file_telemetry = /* @__PURE__ */ fileDesc("Cg90ZWxlbWV0cnkucHJvdG8SCm1lc2h0YXN0aWMi8wEKDURldmljZU1ldHJpY3MSGgoNYmF0dGVyeV9sZXZlbBgBIAEoDUgAiAEBEhQKB3ZvbHRhZ2UYAiABKAJIAYgBARIgChNjaGFubmVsX3V0aWxpemF0aW9uGAMgASgCSAKIAQESGAoLYWlyX3V0aWxfdHgYBCABKAJIA4gBARIbCg51cHRpbWVfc2Vjb25kcxgFIAEoDUgEiAEBQhAKDl9iYXR0ZXJ5X2xldmVsQgoKCF92b2x0YWdlQhYKFF9jaGFubmVsX3V0aWxpemF0aW9uQg4KDF9haXJfdXRpbF90eEIRCg9fdXB0aW1lX3NlY29uZHMiggcKEkVudmlyb25tZW50TWV0cmljcxIYCgt0ZW1wZXJhdHVyZRgBIAEoAkgAiAEBEh4KEXJlbGF0aXZlX2h1bWlkaXR5GAIgASgCSAGIAQESIAoTYmFyb21ldHJpY19wcmVzc3VyZRgDIAEoAkgCiAEBEhsKDmdhc19yZXNpc3RhbmNlGAQgASgCSAOIAQESFAoHdm9sdGFnZRgFIAEoAkgEiAEBEhQKB2N1cnJlbnQYBiABKAJIBYgBARIQCgNpYXEYByABKA1IBogBARIVCghkaXN0YW5jZRgIIAEoAkgHiAEBEhAKA2x1eBgJIAEoAkgIiAEBEhYKCXdoaXRlX2x1eBgKIAEoAkgJiAEBEhMKBmlyX2x1eBgLIAEoAkgKiAEBEhMKBnV2X2x1eBgMIAEoAkgLiAEBEhsKDndpbmRfZGlyZWN0aW9uGA0gASgNSAyIAQESFwoKd2luZF9zcGVlZBgOIAEoAkgNiAEBEhMKBndlaWdodBgPIAEoAkgOiAEBEhYKCXdpbmRfZ3VzdBgQIAEoAkgPiAEBEhYKCXdpbmRfbHVsbBgRIAEoAkgQiAEBEhYKCXJhZGlhdGlvbhgSIAEoAkgRiAEBEhgKC3JhaW5mYWxsXzFoGBMgASgCSBKIAQESGQoMcmFpbmZhbGxfMjRoGBQgASgCSBOIAQESGgoNc29pbF9tb2lzdHVyZRgVIAEoDUgUiAEBEh0KEHNvaWxfdGVtcGVyYXR1cmUYFiABKAJIFYgBAUIOCgxfdGVtcGVyYXR1cmVCFAoSX3JlbGF0aXZlX2h1bWlkaXR5QhYKFF9iYXJvbWV0cmljX3ByZXNzdXJlQhEKD19nYXNfcmVzaXN0YW5jZUIKCghfdm9sdGFnZUIKCghfY3VycmVudEIGCgRfaWFxQgsKCV9kaXN0YW5jZUIGCgRfbHV4QgwKCl93aGl0ZV9sdXhCCQoHX2lyX2x1eEIJCgdfdXZfbHV4QhEKD193aW5kX2RpcmVjdGlvbkINCgtfd2luZF9zcGVlZEIJCgdfd2VpZ2h0QgwKCl93aW5kX2d1c3RCDAoKX3dpbmRfbHVsbEIMCgpfcmFkaWF0aW9uQg4KDF9yYWluZmFsbF8xaEIPCg1fcmFpbmZhbGxfMjRoQhAKDl9zb2lsX21vaXN0dXJlQhMKEV9zb2lsX3RlbXBlcmF0dXJlIooCCgxQb3dlck1ldHJpY3MSGAoLY2gxX3ZvbHRhZ2UYASABKAJIAIgBARIYCgtjaDFfY3VycmVudBgCIAEoAkgBiAEBEhgKC2NoMl92b2x0YWdlGAMgASgCSAKIAQESGAoLY2gyX2N1cnJlbnQYBCABKAJIA4gBARIYCgtjaDNfdm9sdGFnZRgFIAEoAkgEiAEBEhgKC2NoM19jdXJyZW50GAYgASgCSAWIAQFCDgoMX2NoMV92b2x0YWdlQg4KDF9jaDFfY3VycmVudEIOCgxfY2gyX3ZvbHRhZ2VCDgoMX2NoMl9jdXJyZW50Qg4KDF9jaDNfdm9sdGFnZUIOCgxfY2gzX2N1cnJlbnQihQUKEUFpclF1YWxpdHlNZXRyaWNzEhoKDXBtMTBfc3RhbmRhcmQYASABKA1IAIgBARIaCg1wbTI1X3N0YW5kYXJkGAIgASgNSAGIAQESGwoOcG0xMDBfc3RhbmRhcmQYAyABKA1IAogBARIfChJwbTEwX2Vudmlyb25tZW50YWwYBCABKA1IA4gBARIfChJwbTI1X2Vudmlyb25tZW50YWwYBSABKA1IBIgBARIgChNwbTEwMF9lbnZpcm9ubWVudGFsGAYgASgNSAWIAQESGwoOcGFydGljbGVzXzAzdW0YByABKA1IBogBARIbCg5wYXJ0aWNsZXNfMDV1bRgIIAEoDUgHiAEBEhsKDnBhcnRpY2xlc18xMHVtGAkgASgNSAiIAQESGwoOcGFydGljbGVzXzI1dW0YCiABKA1ICYgBARIbCg5wYXJ0aWNsZXNfNTB1bRgLIAEoDUgKiAEBEhwKD3BhcnRpY2xlc18xMDB1bRgMIAEoDUgLiAEBEhAKA2NvMhgNIAEoDUgMiAEBQhAKDl9wbTEwX3N0YW5kYXJkQhAKDl9wbTI1X3N0YW5kYXJkQhEKD19wbTEwMF9zdGFuZGFyZEIVChNfcG0xMF9lbnZpcm9ubWVudGFsQhUKE19wbTI1X2Vudmlyb25tZW50YWxCFgoUX3BtMTAwX2Vudmlyb25tZW50YWxCEQoPX3BhcnRpY2xlc18wM3VtQhEKD19wYXJ0aWNsZXNfMDV1bUIRCg9fcGFydGljbGVzXzEwdW1CEQoPX3BhcnRpY2xlc18yNXVtQhEKD19wYXJ0aWNsZXNfNTB1bUISChBfcGFydGljbGVzXzEwMHVtQgYKBF9jbzIi0gIKCkxvY2FsU3RhdHMSFgoOdXB0aW1lX3NlY29uZHMYASABKA0SGwoTY2hhbm5lbF91dGlsaXphdGlvbhgCIAEoAhITCgthaXJfdXRpbF90eBgDIAEoAhIWCg5udW1fcGFja2V0c190eBgEIAEoDRIWCg5udW1fcGFja2V0c19yeBgFIAEoDRIaChJudW1fcGFja2V0c19yeF9iYWQYBiABKA0SGAoQbnVtX29ubGluZV9ub2RlcxgHIAEoDRIXCg9udW1fdG90YWxfbm9kZXMYCCABKA0SEwoLbnVtX3J4X2R1cGUYCSABKA0SFAoMbnVtX3R4X3JlbGF5GAogASgNEh0KFW51bV90eF9yZWxheV9jYW5jZWxlZBgLIAEoDRIYChBoZWFwX3RvdGFsX2J5dGVzGAwgASgNEhcKD2hlYXBfZnJlZV9ieXRlcxgNIAEoDSJ7Cg1IZWFsdGhNZXRyaWNzEhYKCWhlYXJ0X2JwbRgBIAEoDUgAiAEBEhEKBHNwTzIYAiABKA1IAYgBARIYCgt0ZW1wZXJhdHVyZRgDIAEoAkgCiAEBQgwKCl9oZWFydF9icG1CBwoFX3NwTzJCDgoMX3RlbXBlcmF0dXJlIpECCgtIb3N0TWV0cmljcxIWCg51cHRpbWVfc2Vjb25kcxgBIAEoDRIVCg1mcmVlbWVtX2J5dGVzGAIgASgEEhcKD2Rpc2tmcmVlMV9ieXRlcxgDIAEoBBIcCg9kaXNrZnJlZTJfYnl0ZXMYBCABKARIAIgBARIcCg9kaXNrZnJlZTNfYnl0ZXMYBSABKARIAYgBARINCgVsb2FkMRgGIAEoDRINCgVsb2FkNRgHIAEoDRIOCgZsb2FkMTUYCCABKA0SGAoLdXNlcl9zdHJpbmcYCSABKAlIAogBAUISChBfZGlza2ZyZWUyX2J5dGVzQhIKEF9kaXNrZnJlZTNfYnl0ZXNCDgoMX3VzZXJfc3RyaW5nIp4DCglUZWxlbWV0cnkSDAoEdGltZRgBIAEoBxIzCg5kZXZpY2VfbWV0cmljcxgCIAEoCzIZLm1lc2h0YXN0aWMuRGV2aWNlTWV0cmljc0gAEj0KE2Vudmlyb25tZW50X21ldHJpY3MYAyABKAsyHi5tZXNodGFzdGljLkVudmlyb25tZW50TWV0cmljc0gAEjwKE2Fpcl9xdWFsaXR5X21ldHJpY3MYBCABKAsyHS5tZXNodGFzdGljLkFpclF1YWxpdHlNZXRyaWNzSAASMQoNcG93ZXJfbWV0cmljcxgFIAEoCzIYLm1lc2h0YXN0aWMuUG93ZXJNZXRyaWNzSAASLQoLbG9jYWxfc3RhdHMYBiABKAsyFi5tZXNodGFzdGljLkxvY2FsU3RhdHNIABIzCg5oZWFsdGhfbWV0cmljcxgHIAEoCzIZLm1lc2h0YXN0aWMuSGVhbHRoTWV0cmljc0gAEi8KDGhvc3RfbWV0cmljcxgIIAEoCzIXLm1lc2h0YXN0aWMuSG9zdE1ldHJpY3NIAEIJCgd2YXJpYW50Ij4KDU5hdTc4MDJDb25maWcSEgoKemVyb09mZnNldBgBIAEoBRIZChFjYWxpYnJhdGlvbkZhY3RvchgCIAEoAiqsBAoTVGVsZW1ldHJ5U2Vuc29yVHlwZRIQCgxTRU5TT1JfVU5TRVQQABIKCgZCTUUyODAQARIKCgZCTUU2ODAQAhILCgdNQ1A5ODA4EAMSCgoGSU5BMjYwEAQSCgoGSU5BMjE5EAUSCgoGQk1QMjgwEAYSCQoFU0hUQzMQBxIJCgVMUFMyMhAIEgsKB1FNQzYzMTAQCRILCgdRTUk4NjU4EAoSDAoIUU1DNTg4M0wQCxIJCgVTSFQzMRAMEgwKCFBNU0EwMDNJEA0SCwoHSU5BMzIyMRAOEgoKBkJNUDA4NRAPEgwKCFJDV0w5NjIwEBASCQoFU0hUNFgQERIMCghWRU1MNzcwMBASEgwKCE1MWDkwNjMyEBMSCwoHT1BUMzAwMRAUEgwKCExUUjM5MFVWEBUSDgoKVFNMMjU5MTFGThAWEgkKBUFIVDEwEBcSEAoMREZST0JPVF9MQVJLEBgSCwoHTkFVNzgwMhAZEgoKBkJNUDNYWBAaEgwKCElDTTIwOTQ4EBsSDAoITUFYMTcwNDgQHBIRCg1DVVNUT01fU0VOU09SEB0SDAoITUFYMzAxMDIQHhIMCghNTFg5MDYxNBAfEgkKBVNDRDRYECASCwoHUkFEU0VOUxAhEgoKBklOQTIyNhAiEhAKDERGUk9CT1RfUkFJThAjEgoKBkRQUzMxMBAkEgwKCFJBSzEyMDM1ECUSDAoITUFYMTcyNjEQJhILCgdQQ1QyMDc1ECdCZAoTY29tLmdlZWtzdmlsbGUubWVzaEIPVGVsZW1ldHJ5UHJvdG9zWiJnaXRodWIuY29tL21lc2h0YXN0aWMvZ28vZ2VuZXJhdGVkqgIUTWVzaHRhc3RpYy5Qcm90b2J1ZnO6AgBiBnByb3RvMw");
var DeviceMetricsSchema = /* @__PURE__ */ messageDesc(file_telemetry, 0);
var EnvironmentMetricsSchema = /* @__PURE__ */ messageDesc(file_telemetry, 1);
var PowerMetricsSchema = /* @__PURE__ */ messageDesc(file_telemetry, 2);
var AirQualityMetricsSchema = /* @__PURE__ */ messageDesc(file_telemetry, 3);
var LocalStatsSchema = /* @__PURE__ */ messageDesc(file_telemetry, 4);
var HealthMetricsSchema = /* @__PURE__ */ messageDesc(file_telemetry, 5);
var HostMetricsSchema = /* @__PURE__ */ messageDesc(file_telemetry, 6);
var TelemetrySchema = /* @__PURE__ */ messageDesc(file_telemetry, 7);
var Nau7802ConfigSchema = /* @__PURE__ */ messageDesc(file_telemetry, 8);
var TelemetrySensorType = /* @__PURE__ */ function(TelemetrySensorType$1) {
  TelemetrySensorType$1[TelemetrySensorType$1["SENSOR_UNSET"] = 0] = "SENSOR_UNSET";
  TelemetrySensorType$1[TelemetrySensorType$1["BME280"] = 1] = "BME280";
  TelemetrySensorType$1[TelemetrySensorType$1["BME680"] = 2] = "BME680";
  TelemetrySensorType$1[TelemetrySensorType$1["MCP9808"] = 3] = "MCP9808";
  TelemetrySensorType$1[TelemetrySensorType$1["INA260"] = 4] = "INA260";
  TelemetrySensorType$1[TelemetrySensorType$1["INA219"] = 5] = "INA219";
  TelemetrySensorType$1[TelemetrySensorType$1["BMP280"] = 6] = "BMP280";
  TelemetrySensorType$1[TelemetrySensorType$1["SHTC3"] = 7] = "SHTC3";
  TelemetrySensorType$1[TelemetrySensorType$1["LPS22"] = 8] = "LPS22";
  TelemetrySensorType$1[TelemetrySensorType$1["QMC6310"] = 9] = "QMC6310";
  TelemetrySensorType$1[TelemetrySensorType$1["QMI8658"] = 10] = "QMI8658";
  TelemetrySensorType$1[TelemetrySensorType$1["QMC5883L"] = 11] = "QMC5883L";
  TelemetrySensorType$1[TelemetrySensorType$1["SHT31"] = 12] = "SHT31";
  TelemetrySensorType$1[TelemetrySensorType$1["PMSA003I"] = 13] = "PMSA003I";
  TelemetrySensorType$1[TelemetrySensorType$1["INA3221"] = 14] = "INA3221";
  TelemetrySensorType$1[TelemetrySensorType$1["BMP085"] = 15] = "BMP085";
  TelemetrySensorType$1[TelemetrySensorType$1["RCWL9620"] = 16] = "RCWL9620";
  TelemetrySensorType$1[TelemetrySensorType$1["SHT4X"] = 17] = "SHT4X";
  TelemetrySensorType$1[TelemetrySensorType$1["VEML7700"] = 18] = "VEML7700";
  TelemetrySensorType$1[TelemetrySensorType$1["MLX90632"] = 19] = "MLX90632";
  TelemetrySensorType$1[TelemetrySensorType$1["OPT3001"] = 20] = "OPT3001";
  TelemetrySensorType$1[TelemetrySensorType$1["LTR390UV"] = 21] = "LTR390UV";
  TelemetrySensorType$1[TelemetrySensorType$1["TSL25911FN"] = 22] = "TSL25911FN";
  TelemetrySensorType$1[TelemetrySensorType$1["AHT10"] = 23] = "AHT10";
  TelemetrySensorType$1[TelemetrySensorType$1["DFROBOT_LARK"] = 24] = "DFROBOT_LARK";
  TelemetrySensorType$1[TelemetrySensorType$1["NAU7802"] = 25] = "NAU7802";
  TelemetrySensorType$1[TelemetrySensorType$1["BMP3XX"] = 26] = "BMP3XX";
  TelemetrySensorType$1[TelemetrySensorType$1["ICM20948"] = 27] = "ICM20948";
  TelemetrySensorType$1[TelemetrySensorType$1["MAX17048"] = 28] = "MAX17048";
  TelemetrySensorType$1[TelemetrySensorType$1["CUSTOM_SENSOR"] = 29] = "CUSTOM_SENSOR";
  TelemetrySensorType$1[TelemetrySensorType$1["MAX30102"] = 30] = "MAX30102";
  TelemetrySensorType$1[TelemetrySensorType$1["MLX90614"] = 31] = "MLX90614";
  TelemetrySensorType$1[TelemetrySensorType$1["SCD4X"] = 32] = "SCD4X";
  TelemetrySensorType$1[TelemetrySensorType$1["RADSENS"] = 33] = "RADSENS";
  TelemetrySensorType$1[TelemetrySensorType$1["INA226"] = 34] = "INA226";
  TelemetrySensorType$1[TelemetrySensorType$1["DFROBOT_RAIN"] = 35] = "DFROBOT_RAIN";
  TelemetrySensorType$1[TelemetrySensorType$1["DPS310"] = 36] = "DPS310";
  TelemetrySensorType$1[TelemetrySensorType$1["RAK12035"] = 37] = "RAK12035";
  TelemetrySensorType$1[TelemetrySensorType$1["MAX17261"] = 38] = "MAX17261";
  TelemetrySensorType$1[TelemetrySensorType$1["PCT2075"] = 39] = "PCT2075";
  return TelemetrySensorType$1;
}({});
var TelemetrySensorTypeSchema = /* @__PURE__ */ enumDesc(file_telemetry, 0);
var xmodem_pb_exports = __export2({
  XModemSchema: () => XModemSchema,
  XModem_Control: () => XModem_Control,
  XModem_ControlSchema: () => XModem_ControlSchema,
  file_xmodem: () => file_xmodem
});
var file_xmodem = /* @__PURE__ */ fileDesc("Cgx4bW9kZW0ucHJvdG8SCm1lc2h0YXN0aWMitgEKBlhNb2RlbRIrCgdjb250cm9sGAEgASgOMhoubWVzaHRhc3RpYy5YTW9kZW0uQ29udHJvbBILCgNzZXEYAiABKA0SDQoFY3JjMTYYAyABKA0SDgoGYnVmZmVyGAQgASgMIlMKB0NvbnRyb2wSBwoDTlVMEAASBwoDU09IEAESBwoDU1RYEAISBwoDRU9UEAQSBwoDQUNLEAYSBwoDTkFLEBUSBwoDQ0FOEBgSCQoFQ1RSTFoQGkJhChNjb20uZ2Vla3N2aWxsZS5tZXNoQgxYbW9kZW1Qcm90b3NaImdpdGh1Yi5jb20vbWVzaHRhc3RpYy9nby9nZW5lcmF0ZWSqAhRNZXNodGFzdGljLlByb3RvYnVmc7oCAGIGcHJvdG8z");
var XModemSchema = /* @__PURE__ */ messageDesc(file_xmodem, 0);
var XModem_Control = /* @__PURE__ */ function(XModem_Control$1) {
  XModem_Control$1[XModem_Control$1["NUL"] = 0] = "NUL";
  XModem_Control$1[XModem_Control$1["SOH"] = 1] = "SOH";
  XModem_Control$1[XModem_Control$1["STX"] = 2] = "STX";
  XModem_Control$1[XModem_Control$1["EOT"] = 4] = "EOT";
  XModem_Control$1[XModem_Control$1["ACK"] = 6] = "ACK";
  XModem_Control$1[XModem_Control$1["NAK"] = 21] = "NAK";
  XModem_Control$1[XModem_Control$1["CAN"] = 24] = "CAN";
  XModem_Control$1[XModem_Control$1["CTRLZ"] = 26] = "CTRLZ";
  return XModem_Control$1;
}({});
var XModem_ControlSchema = /* @__PURE__ */ enumDesc(file_xmodem, 0, 0);
var mesh_pb_exports = __export2({
  ChunkedPayloadResponseSchema: () => ChunkedPayloadResponseSchema,
  ChunkedPayloadSchema: () => ChunkedPayloadSchema,
  ClientNotificationSchema: () => ClientNotificationSchema,
  CompressedSchema: () => CompressedSchema,
  Constants: () => Constants$1,
  ConstantsSchema: () => ConstantsSchema,
  CriticalErrorCode: () => CriticalErrorCode,
  CriticalErrorCodeSchema: () => CriticalErrorCodeSchema,
  DataSchema: () => DataSchema,
  DeviceMetadataSchema: () => DeviceMetadataSchema,
  DuplicatedPublicKeySchema: () => DuplicatedPublicKeySchema,
  ExcludedModules: () => ExcludedModules,
  ExcludedModulesSchema: () => ExcludedModulesSchema,
  FileInfoSchema: () => FileInfoSchema,
  FromRadioSchema: () => FromRadioSchema,
  HardwareModel: () => HardwareModel,
  HardwareModelSchema: () => HardwareModelSchema,
  HeartbeatSchema: () => HeartbeatSchema,
  KeyVerificationFinalSchema: () => KeyVerificationFinalSchema,
  KeyVerificationNumberInformSchema: () => KeyVerificationNumberInformSchema,
  KeyVerificationNumberRequestSchema: () => KeyVerificationNumberRequestSchema,
  KeyVerificationSchema: () => KeyVerificationSchema,
  LogRecordSchema: () => LogRecordSchema,
  LogRecord_Level: () => LogRecord_Level,
  LogRecord_LevelSchema: () => LogRecord_LevelSchema,
  LowEntropyKeySchema: () => LowEntropyKeySchema,
  MeshPacketSchema: () => MeshPacketSchema,
  MeshPacket_Delayed: () => MeshPacket_Delayed,
  MeshPacket_DelayedSchema: () => MeshPacket_DelayedSchema,
  MeshPacket_Priority: () => MeshPacket_Priority,
  MeshPacket_PrioritySchema: () => MeshPacket_PrioritySchema,
  MqttClientProxyMessageSchema: () => MqttClientProxyMessageSchema,
  MyNodeInfoSchema: () => MyNodeInfoSchema,
  NeighborInfoSchema: () => NeighborInfoSchema,
  NeighborSchema: () => NeighborSchema,
  NodeInfoSchema: () => NodeInfoSchema,
  NodeRemoteHardwarePinSchema: () => NodeRemoteHardwarePinSchema,
  PositionSchema: () => PositionSchema,
  Position_AltSource: () => Position_AltSource,
  Position_AltSourceSchema: () => Position_AltSourceSchema,
  Position_LocSource: () => Position_LocSource,
  Position_LocSourceSchema: () => Position_LocSourceSchema,
  QueueStatusSchema: () => QueueStatusSchema,
  RouteDiscoverySchema: () => RouteDiscoverySchema,
  RoutingSchema: () => RoutingSchema,
  Routing_Error: () => Routing_Error,
  Routing_ErrorSchema: () => Routing_ErrorSchema,
  ToRadioSchema: () => ToRadioSchema,
  UserSchema: () => UserSchema,
  WaypointSchema: () => WaypointSchema,
  file_mesh: () => file_mesh,
  resend_chunksSchema: () => resend_chunksSchema
});
var file_mesh = /* @__PURE__ */ fileDesc("CgptZXNoLnByb3RvEgptZXNodGFzdGljIocHCghQb3NpdGlvbhIXCgpsYXRpdHVkZV9pGAEgASgPSACIAQESGAoLbG9uZ2l0dWRlX2kYAiABKA9IAYgBARIVCghhbHRpdHVkZRgDIAEoBUgCiAEBEgwKBHRpbWUYBCABKAcSNwoPbG9jYXRpb25fc291cmNlGAUgASgOMh4ubWVzaHRhc3RpYy5Qb3NpdGlvbi5Mb2NTb3VyY2USNwoPYWx0aXR1ZGVfc291cmNlGAYgASgOMh4ubWVzaHRhc3RpYy5Qb3NpdGlvbi5BbHRTb3VyY2USEQoJdGltZXN0YW1wGAcgASgHEh8KF3RpbWVzdGFtcF9taWxsaXNfYWRqdXN0GAggASgFEhkKDGFsdGl0dWRlX2hhZRgJIAEoEUgDiAEBEigKG2FsdGl0dWRlX2dlb2lkYWxfc2VwYXJhdGlvbhgKIAEoEUgEiAEBEgwKBFBET1AYCyABKA0SDAoESERPUBgMIAEoDRIMCgRWRE9QGA0gASgNEhQKDGdwc19hY2N1cmFjeRgOIAEoDRIZCgxncm91bmRfc3BlZWQYDyABKA1IBYgBARIZCgxncm91bmRfdHJhY2sYECABKA1IBogBARITCgtmaXhfcXVhbGl0eRgRIAEoDRIQCghmaXhfdHlwZRgSIAEoDRIUCgxzYXRzX2luX3ZpZXcYEyABKA0SEQoJc2Vuc29yX2lkGBQgASgNEhMKC25leHRfdXBkYXRlGBUgASgNEhIKCnNlcV9udW1iZXIYFiABKA0SFgoOcHJlY2lzaW9uX2JpdHMYFyABKA0iTgoJTG9jU291cmNlEg0KCUxPQ19VTlNFVBAAEg4KCkxPQ19NQU5VQUwQARIQCgxMT0NfSU5URVJOQUwQAhIQCgxMT0NfRVhURVJOQUwQAyJiCglBbHRTb3VyY2USDQoJQUxUX1VOU0VUEAASDgoKQUxUX01BTlVBTBABEhAKDEFMVF9JTlRFUk5BTBACEhAKDEFMVF9FWFRFUk5BTBADEhIKDkFMVF9CQVJPTUVUUklDEARCDQoLX2xhdGl0dWRlX2lCDgoMX2xvbmdpdHVkZV9pQgsKCV9hbHRpdHVkZUIPCg1fYWx0aXR1ZGVfaGFlQh4KHF9hbHRpdHVkZV9nZW9pZGFsX3NlcGFyYXRpb25CDwoNX2dyb3VuZF9zcGVlZEIPCg1fZ3JvdW5kX3RyYWNrIooCCgRVc2VyEgoKAmlkGAEgASgJEhEKCWxvbmdfbmFtZRgCIAEoCRISCgpzaG9ydF9uYW1lGAMgASgJEhMKB21hY2FkZHIYBCABKAxCAhgBEisKCGh3X21vZGVsGAUgASgOMhkubWVzaHRhc3RpYy5IYXJkd2FyZU1vZGVsEhMKC2lzX2xpY2Vuc2VkGAYgASgIEjIKBHJvbGUYByABKA4yJC5tZXNodGFzdGljLkNvbmZpZy5EZXZpY2VDb25maWcuUm9sZRISCgpwdWJsaWNfa2V5GAggASgMEhwKD2lzX3VubWVzc2FnYWJsZRgJIAEoCEgAiAEBQhIKEF9pc191bm1lc3NhZ2FibGUiWgoOUm91dGVEaXNjb3ZlcnkSDQoFcm91dGUYASADKAcSEwoLc25yX3Rvd2FyZHMYAiADKAUSEgoKcm91dGVfYmFjaxgDIAMoBxIQCghzbnJfYmFjaxgEIAMoBSLiAwoHUm91dGluZxIzCg1yb3V0ZV9yZXF1ZXN0GAEgASgLMhoubWVzaHRhc3RpYy5Sb3V0ZURpc2NvdmVyeUgAEjEKC3JvdXRlX3JlcGx5GAIgASgLMhoubWVzaHRhc3RpYy5Sb3V0ZURpc2NvdmVyeUgAEjEKDGVycm9yX3JlYXNvbhgDIAEoDjIZLm1lc2h0YXN0aWMuUm91dGluZy5FcnJvckgAIrACCgVFcnJvchIICgROT05FEAASDAoITk9fUk9VVEUQARILCgdHT1RfTkFLEAISCwoHVElNRU9VVBADEhAKDE5PX0lOVEVSRkFDRRAEEhIKDk1BWF9SRVRSQU5TTUlUEAUSDgoKTk9fQ0hBTk5FTBAGEg0KCVRPT19MQVJHRRAHEg8KC05PX1JFU1BPTlNFEAgSFAoQRFVUWV9DWUNMRV9MSU1JVBAJEg8KC0JBRF9SRVFVRVNUECASEgoOTk9UX0FVVEhPUklaRUQQIRIOCgpQS0lfRkFJTEVEECISFgoSUEtJX1VOS05PV05fUFVCS0VZECMSGQoVQURNSU5fQkFEX1NFU1NJT05fS0VZECQSIQodQURNSU5fUFVCTElDX0tFWV9VTkFVVEhPUklaRUQQJUIJCgd2YXJpYW50IssBCgREYXRhEiQKB3BvcnRudW0YASABKA4yEy5tZXNodGFzdGljLlBvcnROdW0SDwoHcGF5bG9hZBgCIAEoDBIVCg13YW50X3Jlc3BvbnNlGAMgASgIEgwKBGRlc3QYBCABKAcSDgoGc291cmNlGAUgASgHEhIKCnJlcXVlc3RfaWQYBiABKAcSEAoIcmVwbHlfaWQYByABKAcSDQoFZW1vamkYCCABKAcSFQoIYml0ZmllbGQYCSABKA1IAIgBAUILCglfYml0ZmllbGQiPgoPS2V5VmVyaWZpY2F0aW9uEg0KBW5vbmNlGAEgASgEEg0KBWhhc2gxGAIgASgMEg0KBWhhc2gyGAMgASgMIrwBCghXYXlwb2ludBIKCgJpZBgBIAEoDRIXCgpsYXRpdHVkZV9pGAIgASgPSACIAQESGAoLbG9uZ2l0dWRlX2kYAyABKA9IAYgBARIOCgZleHBpcmUYBCABKA0SEQoJbG9ja2VkX3RvGAUgASgNEgwKBG5hbWUYBiABKAkSEwoLZGVzY3JpcHRpb24YByABKAkSDAoEaWNvbhgIIAEoB0INCgtfbGF0aXR1ZGVfaUIOCgxfbG9uZ2l0dWRlX2kibAoWTXF0dENsaWVudFByb3h5TWVzc2FnZRINCgV0b3BpYxgBIAEoCRIOCgRkYXRhGAIgASgMSAASDgoEdGV4dBgDIAEoCUgAEhAKCHJldGFpbmVkGAQgASgIQhEKD3BheWxvYWRfdmFyaWFudCKbBQoKTWVzaFBhY2tldBIMCgRmcm9tGAEgASgHEgoKAnRvGAIgASgHEg8KB2NoYW5uZWwYAyABKA0SIwoHZGVjb2RlZBgEIAEoCzIQLm1lc2h0YXN0aWMuRGF0YUgAEhMKCWVuY3J5cHRlZBgFIAEoDEgAEgoKAmlkGAYgASgHEg8KB3J4X3RpbWUYByABKAcSDgoGcnhfc25yGAggASgCEhEKCWhvcF9saW1pdBgJIAEoDRIQCgh3YW50X2FjaxgKIAEoCBIxCghwcmlvcml0eRgLIAEoDjIfLm1lc2h0YXN0aWMuTWVzaFBhY2tldC5Qcmlvcml0eRIPCgdyeF9yc3NpGAwgASgFEjMKB2RlbGF5ZWQYDSABKA4yHi5tZXNodGFzdGljLk1lc2hQYWNrZXQuRGVsYXllZEICGAESEAoIdmlhX21xdHQYDiABKAgSEQoJaG9wX3N0YXJ0GA8gASgNEhIKCnB1YmxpY19rZXkYECABKAwSFQoNcGtpX2VuY3J5cHRlZBgRIAEoCBIQCghuZXh0X2hvcBgSIAEoDRISCgpyZWxheV9ub2RlGBMgASgNEhAKCHR4X2FmdGVyGBQgASgNIn4KCFByaW9yaXR5EgkKBVVOU0VUEAASBwoDTUlOEAESDgoKQkFDS0dST1VORBAKEgsKB0RFRkFVTFQQQBIMCghSRUxJQUJMRRBGEgwKCFJFU1BPTlNFEFASCAoESElHSBBkEgkKBUFMRVJUEG4SBwoDQUNLEHgSBwoDTUFYEH8iQgoHRGVsYXllZBIMCghOT19ERUxBWRAAEhUKEURFTEFZRURfQlJPQURDQVNUEAESEgoOREVMQVlFRF9ESVJFQ1QQAkIRCg9wYXlsb2FkX3ZhcmlhbnQixwIKCE5vZGVJbmZvEgsKA251bRgBIAEoDRIeCgR1c2VyGAIgASgLMhAubWVzaHRhc3RpYy5Vc2VyEiYKCHBvc2l0aW9uGAMgASgLMhQubWVzaHRhc3RpYy5Qb3NpdGlvbhILCgNzbnIYBCABKAISEgoKbGFzdF9oZWFyZBgFIAEoBxIxCg5kZXZpY2VfbWV0cmljcxgGIAEoCzIZLm1lc2h0YXN0aWMuRGV2aWNlTWV0cmljcxIPCgdjaGFubmVsGAcgASgNEhAKCHZpYV9tcXR0GAggASgIEhYKCWhvcHNfYXdheRgJIAEoDUgAiAEBEhMKC2lzX2Zhdm9yaXRlGAogASgIEhIKCmlzX2lnbm9yZWQYCyABKAgSIAoYaXNfa2V5X21hbnVhbGx5X3ZlcmlmaWVkGAwgASgIQgwKCl9ob3BzX2F3YXkidAoKTXlOb2RlSW5mbxITCgtteV9ub2RlX251bRgBIAEoDRIUCgxyZWJvb3RfY291bnQYCCABKA0SFwoPbWluX2FwcF92ZXJzaW9uGAsgASgNEhEKCWRldmljZV9pZBgMIAEoDBIPCgdwaW9fZW52GA0gASgJIsABCglMb2dSZWNvcmQSDwoHbWVzc2FnZRgBIAEoCRIMCgR0aW1lGAIgASgHEg4KBnNvdXJjZRgDIAEoCRIqCgVsZXZlbBgEIAEoDjIbLm1lc2h0YXN0aWMuTG9nUmVjb3JkLkxldmVsIlgKBUxldmVsEgkKBVVOU0VUEAASDAoIQ1JJVElDQUwQMhIJCgVFUlJPUhAoEgsKB1dBUk5JTkcQHhIICgRJTkZPEBQSCQoFREVCVUcQChIJCgVUUkFDRRAFIlAKC1F1ZXVlU3RhdHVzEgsKA3JlcxgBIAEoBRIMCgRmcmVlGAIgASgNEg4KBm1heGxlbhgDIAEoDRIWCg5tZXNoX3BhY2tldF9pZBgEIAEoDSL5BQoJRnJvbVJhZGlvEgoKAmlkGAEgASgNEigKBnBhY2tldBgCIAEoCzIWLm1lc2h0YXN0aWMuTWVzaFBhY2tldEgAEikKB215X2luZm8YAyABKAsyFi5tZXNodGFzdGljLk15Tm9kZUluZm9IABIpCglub2RlX2luZm8YBCABKAsyFC5tZXNodGFzdGljLk5vZGVJbmZvSAASJAoGY29uZmlnGAUgASgLMhIubWVzaHRhc3RpYy5Db25maWdIABIrCgpsb2dfcmVjb3JkGAYgASgLMhUubWVzaHRhc3RpYy5Mb2dSZWNvcmRIABIcChJjb25maWdfY29tcGxldGVfaWQYByABKA1IABISCghyZWJvb3RlZBgIIAEoCEgAEjAKDG1vZHVsZUNvbmZpZxgJIAEoCzIYLm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnSAASJgoHY2hhbm5lbBgKIAEoCzITLm1lc2h0YXN0aWMuQ2hhbm5lbEgAEi4KC3F1ZXVlU3RhdHVzGAsgASgLMhcubWVzaHRhc3RpYy5RdWV1ZVN0YXR1c0gAEioKDHhtb2RlbVBhY2tldBgMIAEoCzISLm1lc2h0YXN0aWMuWE1vZGVtSAASLgoIbWV0YWRhdGEYDSABKAsyGi5tZXNodGFzdGljLkRldmljZU1ldGFkYXRhSAASRAoWbXF0dENsaWVudFByb3h5TWVzc2FnZRgOIAEoCzIiLm1lc2h0YXN0aWMuTXF0dENsaWVudFByb3h5TWVzc2FnZUgAEigKCGZpbGVJbmZvGA8gASgLMhQubWVzaHRhc3RpYy5GaWxlSW5mb0gAEjwKEmNsaWVudE5vdGlmaWNhdGlvbhgQIAEoCzIeLm1lc2h0YXN0aWMuQ2xpZW50Tm90aWZpY2F0aW9uSAASNAoOZGV2aWNldWlDb25maWcYESABKAsyGi5tZXNodGFzdGljLkRldmljZVVJQ29uZmlnSABCEQoPcGF5bG9hZF92YXJpYW50IvoDChJDbGllbnROb3RpZmljYXRpb24SFQoIcmVwbHlfaWQYASABKA1IAYgBARIMCgR0aW1lGAIgASgHEioKBWxldmVsGAMgASgOMhsubWVzaHRhc3RpYy5Mb2dSZWNvcmQuTGV2ZWwSDwoHbWVzc2FnZRgEIAEoCRJRCh5rZXlfdmVyaWZpY2F0aW9uX251bWJlcl9pbmZvcm0YCyABKAsyJy5tZXNodGFzdGljLktleVZlcmlmaWNhdGlvbk51bWJlckluZm9ybUgAElMKH2tleV92ZXJpZmljYXRpb25fbnVtYmVyX3JlcXVlc3QYDCABKAsyKC5tZXNodGFzdGljLktleVZlcmlmaWNhdGlvbk51bWJlclJlcXVlc3RIABJCChZrZXlfdmVyaWZpY2F0aW9uX2ZpbmFsGA0gASgLMiAubWVzaHRhc3RpYy5LZXlWZXJpZmljYXRpb25GaW5hbEgAEkAKFWR1cGxpY2F0ZWRfcHVibGljX2tleRgOIAEoCzIfLm1lc2h0YXN0aWMuRHVwbGljYXRlZFB1YmxpY0tleUgAEjQKD2xvd19lbnRyb3B5X2tleRgPIAEoCzIZLm1lc2h0YXN0aWMuTG93RW50cm9weUtleUgAQhEKD3BheWxvYWRfdmFyaWFudEILCglfcmVwbHlfaWQiXgobS2V5VmVyaWZpY2F0aW9uTnVtYmVySW5mb3JtEg0KBW5vbmNlGAEgASgEEhcKD3JlbW90ZV9sb25nbmFtZRgCIAEoCRIXCg9zZWN1cml0eV9udW1iZXIYAyABKA0iRgocS2V5VmVyaWZpY2F0aW9uTnVtYmVyUmVxdWVzdBINCgVub25jZRgBIAEoBBIXCg9yZW1vdGVfbG9uZ25hbWUYAiABKAkicQoUS2V5VmVyaWZpY2F0aW9uRmluYWwSDQoFbm9uY2UYASABKAQSFwoPcmVtb3RlX2xvbmduYW1lGAIgASgJEhAKCGlzU2VuZGVyGAMgASgIEh8KF3ZlcmlmaWNhdGlvbl9jaGFyYWN0ZXJzGAQgASgJIhUKE0R1cGxpY2F0ZWRQdWJsaWNLZXkiDwoNTG93RW50cm9weUtleSIxCghGaWxlSW5mbxIRCglmaWxlX25hbWUYASABKAkSEgoKc2l6ZV9ieXRlcxgCIAEoDSKUAgoHVG9SYWRpbxIoCgZwYWNrZXQYASABKAsyFi5tZXNodGFzdGljLk1lc2hQYWNrZXRIABIYCg53YW50X2NvbmZpZ19pZBgDIAEoDUgAEhQKCmRpc2Nvbm5lY3QYBCABKAhIABIqCgx4bW9kZW1QYWNrZXQYBSABKAsyEi5tZXNodGFzdGljLlhNb2RlbUgAEkQKFm1xdHRDbGllbnRQcm94eU1lc3NhZ2UYBiABKAsyIi5tZXNodGFzdGljLk1xdHRDbGllbnRQcm94eU1lc3NhZ2VIABIqCgloZWFydGJlYXQYByABKAsyFS5tZXNodGFzdGljLkhlYXJ0YmVhdEgAQhEKD3BheWxvYWRfdmFyaWFudCJACgpDb21wcmVzc2VkEiQKB3BvcnRudW0YASABKA4yEy5tZXNodGFzdGljLlBvcnROdW0SDAoEZGF0YRgCIAEoDCKHAQoMTmVpZ2hib3JJbmZvEg8KB25vZGVfaWQYASABKA0SFwoPbGFzdF9zZW50X2J5X2lkGAIgASgNEiQKHG5vZGVfYnJvYWRjYXN0X2ludGVydmFsX3NlY3MYAyABKA0SJwoJbmVpZ2hib3JzGAQgAygLMhQubWVzaHRhc3RpYy5OZWlnaGJvciJkCghOZWlnaGJvchIPCgdub2RlX2lkGAEgASgNEgsKA3NuchgCIAEoAhIUCgxsYXN0X3J4X3RpbWUYAyABKAcSJAocbm9kZV9icm9hZGNhc3RfaW50ZXJ2YWxfc2VjcxgEIAEoDSLXAgoORGV2aWNlTWV0YWRhdGESGAoQZmlybXdhcmVfdmVyc2lvbhgBIAEoCRIcChRkZXZpY2Vfc3RhdGVfdmVyc2lvbhgCIAEoDRITCgtjYW5TaHV0ZG93bhgDIAEoCBIPCgdoYXNXaWZpGAQgASgIEhQKDGhhc0JsdWV0b290aBgFIAEoCBITCgtoYXNFdGhlcm5ldBgGIAEoCBIyCgRyb2xlGAcgASgOMiQubWVzaHRhc3RpYy5Db25maWcuRGV2aWNlQ29uZmlnLlJvbGUSFgoOcG9zaXRpb25fZmxhZ3MYCCABKA0SKwoIaHdfbW9kZWwYCSABKA4yGS5tZXNodGFzdGljLkhhcmR3YXJlTW9kZWwSGQoRaGFzUmVtb3RlSGFyZHdhcmUYCiABKAgSDgoGaGFzUEtDGAsgASgIEhgKEGV4Y2x1ZGVkX21vZHVsZXMYDCABKA0iCwoJSGVhcnRiZWF0IlUKFU5vZGVSZW1vdGVIYXJkd2FyZVBpbhIQCghub2RlX251bRgBIAEoDRIqCgNwaW4YAiABKAsyHS5tZXNodGFzdGljLlJlbW90ZUhhcmR3YXJlUGluImUKDkNodW5rZWRQYXlsb2FkEhIKCnBheWxvYWRfaWQYASABKA0SEwoLY2h1bmtfY291bnQYAiABKA0SEwoLY2h1bmtfaW5kZXgYAyABKA0SFQoNcGF5bG9hZF9jaHVuaxgEIAEoDCIfCg1yZXNlbmRfY2h1bmtzEg4KBmNodW5rcxgBIAMoDSKqAQoWQ2h1bmtlZFBheWxvYWRSZXNwb25zZRISCgpwYXlsb2FkX2lkGAEgASgNEhoKEHJlcXVlc3RfdHJhbnNmZXIYAiABKAhIABIZCg9hY2NlcHRfdHJhbnNmZXIYAyABKAhIABIyCg1yZXNlbmRfY2h1bmtzGAQgASgLMhkubWVzaHRhc3RpYy5yZXNlbmRfY2h1bmtzSABCEQoPcGF5bG9hZF92YXJpYW50KvcPCg1IYXJkd2FyZU1vZGVsEgkKBVVOU0VUEAASDAoIVExPUkFfVjIQARIMCghUTE9SQV9WMRACEhIKDlRMT1JBX1YyXzFfMVA2EAMSCQoFVEJFQU0QBBIPCgtIRUxURUNfVjJfMBAFEg4KClRCRUFNX1YwUDcQBhIKCgZUX0VDSE8QBxIQCgxUTE9SQV9WMV8xUDMQCBILCgdSQUs0NjMxEAkSDwoLSEVMVEVDX1YyXzEQChINCglIRUxURUNfVjEQCxIYChRMSUxZR09fVEJFQU1fUzNfQ09SRRAMEgwKCFJBSzExMjAwEA0SCwoHTkFOT19HMRAOEhIKDlRMT1JBX1YyXzFfMVA4EA8SDwoLVExPUkFfVDNfUzMQEBIUChBOQU5PX0cxX0VYUExPUkVSEBESEQoNTkFOT19HMl9VTFRSQRASEg0KCUxPUkFfVFlQRRATEgsKB1dJUEhPTkUQFBIOCgpXSU9fV00xMTEwEBUSCwoHUkFLMjU2MBAWEhMKD0hFTFRFQ19IUlVfMzYwMRAXEhoKFkhFTFRFQ19XSVJFTEVTU19CUklER0UQGBIOCgpTVEFUSU9OX0cxEBkSDAoIUkFLMTEzMTAQGhIUChBTRU5TRUxPUkFfUlAyMDQwEBsSEAoMU0VOU0VMT1JBX1MzEBwSDQoJQ0FOQVJZT05FEB0SDwoLUlAyMDQwX0xPUkEQHhIOCgpTVEFUSU9OX0cyEB8SEQoNTE9SQV9SRUxBWV9WMRAgEg4KCk5SRjUyODQwREsQIRIHCgNQUFIQIhIPCgtHRU5JRUJMT0NLUxAjEhEKDU5SRjUyX1VOS05PV04QJBINCglQT1JURFVJTk8QJRIPCgtBTkRST0lEX1NJTRAmEgoKBkRJWV9WMRAnEhUKEU5SRjUyODQwX1BDQTEwMDU5ECgSCgoGRFJfREVWECkSCwoHTTVTVEFDSxAqEg0KCUhFTFRFQ19WMxArEhEKDUhFTFRFQ19XU0xfVjMQLBITCg9CRVRBRlBWXzI0MDBfVFgQLRIXChNCRVRBRlBWXzkwMF9OQU5PX1RYEC4SDAoIUlBJX1BJQ08QLxIbChdIRUxURUNfV0lSRUxFU1NfVFJBQ0tFUhAwEhkKFUhFTFRFQ19XSVJFTEVTU19QQVBFUhAxEgoKBlRfREVDSxAyEg4KClRfV0FUQ0hfUzMQMxIRCg1QSUNPTVBVVEVSX1MzEDQSDwoLSEVMVEVDX0hUNjIQNRISCg5FQllURV9FU1AzMl9TMxA2EhEKDUVTUDMyX1MzX1BJQ08QNxINCglDSEFUVEVSXzIQOBIeChpIRUxURUNfV0lSRUxFU1NfUEFQRVJfVjFfMBA5EiAKHEhFTFRFQ19XSVJFTEVTU19UUkFDS0VSX1YxXzAQOhILCgdVTlBIT05FEDsSDAoIVERfTE9SQUMQPBITCg9DREVCWVRFX0VPUkFfUzMQPRIPCgtUV0NfTUVTSF9WNBA+EhYKEk5SRjUyX1BST01JQ1JPX0RJWRA/Eh8KG1JBRElPTUFTVEVSXzkwMF9CQU5ESVRfTkFOTxBAEhwKGEhFTFRFQ19DQVBTVUxFX1NFTlNPUl9WMxBBEh0KGUhFTFRFQ19WSVNJT05fTUFTVEVSX1QxOTAQQhIdChlIRUxURUNfVklTSU9OX01BU1RFUl9FMjEzEEMSHQoZSEVMVEVDX1ZJU0lPTl9NQVNURVJfRTI5MBBEEhkKFUhFTFRFQ19NRVNIX05PREVfVDExNBBFEhYKElNFTlNFQ0FQX0lORElDQVRPUhBGEhMKD1RSQUNLRVJfVDEwMDBfRRBHEgsKB1JBSzMxNzIQSBIKCgZXSU9fRTUQSRIaChZSQURJT01BU1RFUl85MDBfQkFORElUEEoSEwoPTUUyNUxTMDFfNFkxMFREEEsSGAoUUlAyMDQwX0ZFQVRIRVJfUkZNOTUQTBIVChFNNVNUQUNLX0NPUkVCQVNJQxBNEhEKDU01U1RBQ0tfQ09SRTIQThINCglSUElfUElDTzIQTxISCg5NNVNUQUNLX0NPUkVTMxBQEhEKDVNFRUVEX1hJQU9fUzMQURILCgdNUzI0U0YxEFISDAoIVExPUkFfQzYQUxIPCgtXSVNNRVNIX1RBUBBUEg0KCVJPVVRBU1RJQxBVEgwKCE1FU0hfVEFCEFYSDAoITUVTSExJTksQVxISCg5YSUFPX05SRjUyX0tJVBBYEhAKDFRISU5LTk9ERV9NMRBZEhAKDFRISU5LTk9ERV9NMhBaEg8KC1RfRVRIX0VMSVRFEFsSFQoRSEVMVEVDX1NFTlNPUl9IVUIQXBIaChZSRVNFUlZFRF9GUklFRF9DSElDS0VOEF0SFgoSSEVMVEVDX01FU0hfUE9DS0VUEF4SFAoQU0VFRURfU09MQVJfTk9ERRBfEhgKFE5PTUFEU1RBUl9NRVRFT1JfUFJPEGASDQoJQ1JPV1BBTkVMEGESCwoHTElOS18zMhBiEhgKFFNFRUVEX1dJT19UUkFDS0VSX0wxEGMSHQoZU0VFRURfV0lPX1RSQUNLRVJfTDFfRUlOSxBkEhQKEFFXQU5UWl9USU5ZX0FSTVMQZRIOCgpUX0RFQ0tfUFJPEGYSEAoMVF9MT1JBX1BBR0VSEGcSHQoZR0FUNTYyX01FU0hfVFJJQUxfVFJBQ0tFUhBoEg8KClBSSVZBVEVfSFcQ/wEqLAoJQ29uc3RhbnRzEggKBFpFUk8QABIVChBEQVRBX1BBWUxPQURfTEVOEOkBKrQCChFDcml0aWNhbEVycm9yQ29kZRIICgROT05FEAASDwoLVFhfV0FUQ0hET0cQARIUChBTTEVFUF9FTlRFUl9XQUlUEAISDAoITk9fUkFESU8QAxIPCgtVTlNQRUNJRklFRBAEEhUKEVVCTE9YX1VOSVRfRkFJTEVEEAUSDQoJTk9fQVhQMTkyEAYSGQoVSU5WQUxJRF9SQURJT19TRVRUSU5HEAcSEwoPVFJBTlNNSVRfRkFJTEVEEAgSDAoIQlJPV05PVVQQCRISCg5TWDEyNjJfRkFJTFVSRRAKEhEKDVJBRElPX1NQSV9CVUcQCxIgChxGTEFTSF9DT1JSVVBUSU9OX1JFQ09WRVJBQkxFEAwSIgoeRkxBU0hfQ09SUlVQVElPTl9VTlJFQ09WRVJBQkxFEA0qgAMKD0V4Y2x1ZGVkTW9kdWxlcxIRCg1FWENMVURFRF9OT05FEAASDwoLTVFUVF9DT05GSUcQARIRCg1TRVJJQUxfQ09ORklHEAISEwoPRVhUTk9USUZfQ09ORklHEAQSFwoTU1RPUkVGT1JXQVJEX0NPTkZJRxAIEhQKEFJBTkdFVEVTVF9DT05GSUcQEBIUChBURUxFTUVUUllfQ09ORklHECASFAoQQ0FOTkVETVNHX0NPTkZJRxBAEhEKDEFVRElPX0NPTkZJRxCAARIaChVSRU1PVEVIQVJEV0FSRV9DT05GSUcQgAISGAoTTkVJR0hCT1JJTkZPX0NPTkZJRxCABBIbChZBTUJJRU5UTElHSFRJTkdfQ09ORklHEIAIEhsKFkRFVEVDVElPTlNFTlNPUl9DT05GSUcQgBASFgoRUEFYQ09VTlRFUl9DT05GSUcQgCASFQoQQkxVRVRPT1RIX0NPTkZJRxCAQBIUCg5ORVRXT1JLX0NPTkZJRxCAgAFCXwoTY29tLmdlZWtzdmlsbGUubWVzaEIKTWVzaFByb3Rvc1oiZ2l0aHViLmNvbS9tZXNodGFzdGljL2dvL2dlbmVyYXRlZKoCFE1lc2h0YXN0aWMuUHJvdG9idWZzugIAYgZwcm90bzM", [
  file_channel,
  file_config,
  file_module_config,
  file_portnums,
  file_telemetry,
  file_xmodem,
  file_device_ui
]);
var PositionSchema = /* @__PURE__ */ messageDesc(file_mesh, 0);
var Position_LocSource = /* @__PURE__ */ function(Position_LocSource$1) {
  Position_LocSource$1[Position_LocSource$1["LOC_UNSET"] = 0] = "LOC_UNSET";
  Position_LocSource$1[Position_LocSource$1["LOC_MANUAL"] = 1] = "LOC_MANUAL";
  Position_LocSource$1[Position_LocSource$1["LOC_INTERNAL"] = 2] = "LOC_INTERNAL";
  Position_LocSource$1[Position_LocSource$1["LOC_EXTERNAL"] = 3] = "LOC_EXTERNAL";
  return Position_LocSource$1;
}({});
var Position_LocSourceSchema = /* @__PURE__ */ enumDesc(file_mesh, 0, 0);
var Position_AltSource = /* @__PURE__ */ function(Position_AltSource$1) {
  Position_AltSource$1[Position_AltSource$1["ALT_UNSET"] = 0] = "ALT_UNSET";
  Position_AltSource$1[Position_AltSource$1["ALT_MANUAL"] = 1] = "ALT_MANUAL";
  Position_AltSource$1[Position_AltSource$1["ALT_INTERNAL"] = 2] = "ALT_INTERNAL";
  Position_AltSource$1[Position_AltSource$1["ALT_EXTERNAL"] = 3] = "ALT_EXTERNAL";
  Position_AltSource$1[Position_AltSource$1["ALT_BAROMETRIC"] = 4] = "ALT_BAROMETRIC";
  return Position_AltSource$1;
}({});
var Position_AltSourceSchema = /* @__PURE__ */ enumDesc(file_mesh, 0, 1);
var UserSchema = /* @__PURE__ */ messageDesc(file_mesh, 1);
var RouteDiscoverySchema = /* @__PURE__ */ messageDesc(file_mesh, 2);
var RoutingSchema = /* @__PURE__ */ messageDesc(file_mesh, 3);
var Routing_Error = /* @__PURE__ */ function(Routing_Error$1) {
  Routing_Error$1[Routing_Error$1["NONE"] = 0] = "NONE";
  Routing_Error$1[Routing_Error$1["NO_ROUTE"] = 1] = "NO_ROUTE";
  Routing_Error$1[Routing_Error$1["GOT_NAK"] = 2] = "GOT_NAK";
  Routing_Error$1[Routing_Error$1["TIMEOUT"] = 3] = "TIMEOUT";
  Routing_Error$1[Routing_Error$1["NO_INTERFACE"] = 4] = "NO_INTERFACE";
  Routing_Error$1[Routing_Error$1["MAX_RETRANSMIT"] = 5] = "MAX_RETRANSMIT";
  Routing_Error$1[Routing_Error$1["NO_CHANNEL"] = 6] = "NO_CHANNEL";
  Routing_Error$1[Routing_Error$1["TOO_LARGE"] = 7] = "TOO_LARGE";
  Routing_Error$1[Routing_Error$1["NO_RESPONSE"] = 8] = "NO_RESPONSE";
  Routing_Error$1[Routing_Error$1["DUTY_CYCLE_LIMIT"] = 9] = "DUTY_CYCLE_LIMIT";
  Routing_Error$1[Routing_Error$1["BAD_REQUEST"] = 32] = "BAD_REQUEST";
  Routing_Error$1[Routing_Error$1["NOT_AUTHORIZED"] = 33] = "NOT_AUTHORIZED";
  Routing_Error$1[Routing_Error$1["PKI_FAILED"] = 34] = "PKI_FAILED";
  Routing_Error$1[Routing_Error$1["PKI_UNKNOWN_PUBKEY"] = 35] = "PKI_UNKNOWN_PUBKEY";
  Routing_Error$1[Routing_Error$1["ADMIN_BAD_SESSION_KEY"] = 36] = "ADMIN_BAD_SESSION_KEY";
  Routing_Error$1[Routing_Error$1["ADMIN_PUBLIC_KEY_UNAUTHORIZED"] = 37] = "ADMIN_PUBLIC_KEY_UNAUTHORIZED";
  return Routing_Error$1;
}({});
var Routing_ErrorSchema = /* @__PURE__ */ enumDesc(file_mesh, 3, 0);
var DataSchema = /* @__PURE__ */ messageDesc(file_mesh, 4);
var KeyVerificationSchema = /* @__PURE__ */ messageDesc(file_mesh, 5);
var WaypointSchema = /* @__PURE__ */ messageDesc(file_mesh, 6);
var MqttClientProxyMessageSchema = /* @__PURE__ */ messageDesc(file_mesh, 7);
var MeshPacketSchema = /* @__PURE__ */ messageDesc(file_mesh, 8);
var MeshPacket_Priority = /* @__PURE__ */ function(MeshPacket_Priority$1) {
  MeshPacket_Priority$1[MeshPacket_Priority$1["UNSET"] = 0] = "UNSET";
  MeshPacket_Priority$1[MeshPacket_Priority$1["MIN"] = 1] = "MIN";
  MeshPacket_Priority$1[MeshPacket_Priority$1["BACKGROUND"] = 10] = "BACKGROUND";
  MeshPacket_Priority$1[MeshPacket_Priority$1["DEFAULT"] = 64] = "DEFAULT";
  MeshPacket_Priority$1[MeshPacket_Priority$1["RELIABLE"] = 70] = "RELIABLE";
  MeshPacket_Priority$1[MeshPacket_Priority$1["RESPONSE"] = 80] = "RESPONSE";
  MeshPacket_Priority$1[MeshPacket_Priority$1["HIGH"] = 100] = "HIGH";
  MeshPacket_Priority$1[MeshPacket_Priority$1["ALERT"] = 110] = "ALERT";
  MeshPacket_Priority$1[MeshPacket_Priority$1["ACK"] = 120] = "ACK";
  MeshPacket_Priority$1[MeshPacket_Priority$1["MAX"] = 127] = "MAX";
  return MeshPacket_Priority$1;
}({});
var MeshPacket_PrioritySchema = /* @__PURE__ */ enumDesc(file_mesh, 8, 0);
var MeshPacket_Delayed = /* @__PURE__ */ function(MeshPacket_Delayed$1) {
  MeshPacket_Delayed$1[MeshPacket_Delayed$1["NO_DELAY"] = 0] = "NO_DELAY";
  MeshPacket_Delayed$1[MeshPacket_Delayed$1["DELAYED_BROADCAST"] = 1] = "DELAYED_BROADCAST";
  MeshPacket_Delayed$1[MeshPacket_Delayed$1["DELAYED_DIRECT"] = 2] = "DELAYED_DIRECT";
  return MeshPacket_Delayed$1;
}({});
var MeshPacket_DelayedSchema = /* @__PURE__ */ enumDesc(file_mesh, 8, 1);
var NodeInfoSchema = /* @__PURE__ */ messageDesc(file_mesh, 9);
var MyNodeInfoSchema = /* @__PURE__ */ messageDesc(file_mesh, 10);
var LogRecordSchema = /* @__PURE__ */ messageDesc(file_mesh, 11);
var LogRecord_Level = /* @__PURE__ */ function(LogRecord_Level$1) {
  LogRecord_Level$1[LogRecord_Level$1["UNSET"] = 0] = "UNSET";
  LogRecord_Level$1[LogRecord_Level$1["CRITICAL"] = 50] = "CRITICAL";
  LogRecord_Level$1[LogRecord_Level$1["ERROR"] = 40] = "ERROR";
  LogRecord_Level$1[LogRecord_Level$1["WARNING"] = 30] = "WARNING";
  LogRecord_Level$1[LogRecord_Level$1["INFO"] = 20] = "INFO";
  LogRecord_Level$1[LogRecord_Level$1["DEBUG"] = 10] = "DEBUG";
  LogRecord_Level$1[LogRecord_Level$1["TRACE"] = 5] = "TRACE";
  return LogRecord_Level$1;
}({});
var LogRecord_LevelSchema = /* @__PURE__ */ enumDesc(file_mesh, 11, 0);
var QueueStatusSchema = /* @__PURE__ */ messageDesc(file_mesh, 12);
var FromRadioSchema = /* @__PURE__ */ messageDesc(file_mesh, 13);
var ClientNotificationSchema = /* @__PURE__ */ messageDesc(file_mesh, 14);
var KeyVerificationNumberInformSchema = /* @__PURE__ */ messageDesc(file_mesh, 15);
var KeyVerificationNumberRequestSchema = /* @__PURE__ */ messageDesc(file_mesh, 16);
var KeyVerificationFinalSchema = /* @__PURE__ */ messageDesc(file_mesh, 17);
var DuplicatedPublicKeySchema = /* @__PURE__ */ messageDesc(file_mesh, 18);
var LowEntropyKeySchema = /* @__PURE__ */ messageDesc(file_mesh, 19);
var FileInfoSchema = /* @__PURE__ */ messageDesc(file_mesh, 20);
var ToRadioSchema = /* @__PURE__ */ messageDesc(file_mesh, 21);
var CompressedSchema = /* @__PURE__ */ messageDesc(file_mesh, 22);
var NeighborInfoSchema = /* @__PURE__ */ messageDesc(file_mesh, 23);
var NeighborSchema = /* @__PURE__ */ messageDesc(file_mesh, 24);
var DeviceMetadataSchema = /* @__PURE__ */ messageDesc(file_mesh, 25);
var HeartbeatSchema = /* @__PURE__ */ messageDesc(file_mesh, 26);
var NodeRemoteHardwarePinSchema = /* @__PURE__ */ messageDesc(file_mesh, 27);
var ChunkedPayloadSchema = /* @__PURE__ */ messageDesc(file_mesh, 28);
var resend_chunksSchema = /* @__PURE__ */ messageDesc(file_mesh, 29);
var ChunkedPayloadResponseSchema = /* @__PURE__ */ messageDesc(file_mesh, 30);
var HardwareModel = /* @__PURE__ */ function(HardwareModel$1) {
  HardwareModel$1[HardwareModel$1["UNSET"] = 0] = "UNSET";
  HardwareModel$1[HardwareModel$1["TLORA_V2"] = 1] = "TLORA_V2";
  HardwareModel$1[HardwareModel$1["TLORA_V1"] = 2] = "TLORA_V1";
  HardwareModel$1[HardwareModel$1["TLORA_V2_1_1P6"] = 3] = "TLORA_V2_1_1P6";
  HardwareModel$1[HardwareModel$1["TBEAM"] = 4] = "TBEAM";
  HardwareModel$1[HardwareModel$1["HELTEC_V2_0"] = 5] = "HELTEC_V2_0";
  HardwareModel$1[HardwareModel$1["TBEAM_V0P7"] = 6] = "TBEAM_V0P7";
  HardwareModel$1[HardwareModel$1["T_ECHO"] = 7] = "T_ECHO";
  HardwareModel$1[HardwareModel$1["TLORA_V1_1P3"] = 8] = "TLORA_V1_1P3";
  HardwareModel$1[HardwareModel$1["RAK4631"] = 9] = "RAK4631";
  HardwareModel$1[HardwareModel$1["HELTEC_V2_1"] = 10] = "HELTEC_V2_1";
  HardwareModel$1[HardwareModel$1["HELTEC_V1"] = 11] = "HELTEC_V1";
  HardwareModel$1[HardwareModel$1["LILYGO_TBEAM_S3_CORE"] = 12] = "LILYGO_TBEAM_S3_CORE";
  HardwareModel$1[HardwareModel$1["RAK11200"] = 13] = "RAK11200";
  HardwareModel$1[HardwareModel$1["NANO_G1"] = 14] = "NANO_G1";
  HardwareModel$1[HardwareModel$1["TLORA_V2_1_1P8"] = 15] = "TLORA_V2_1_1P8";
  HardwareModel$1[HardwareModel$1["TLORA_T3_S3"] = 16] = "TLORA_T3_S3";
  HardwareModel$1[HardwareModel$1["NANO_G1_EXPLORER"] = 17] = "NANO_G1_EXPLORER";
  HardwareModel$1[HardwareModel$1["NANO_G2_ULTRA"] = 18] = "NANO_G2_ULTRA";
  HardwareModel$1[HardwareModel$1["LORA_TYPE"] = 19] = "LORA_TYPE";
  HardwareModel$1[HardwareModel$1["WIPHONE"] = 20] = "WIPHONE";
  HardwareModel$1[HardwareModel$1["WIO_WM1110"] = 21] = "WIO_WM1110";
  HardwareModel$1[HardwareModel$1["RAK2560"] = 22] = "RAK2560";
  HardwareModel$1[HardwareModel$1["HELTEC_HRU_3601"] = 23] = "HELTEC_HRU_3601";
  HardwareModel$1[HardwareModel$1["HELTEC_WIRELESS_BRIDGE"] = 24] = "HELTEC_WIRELESS_BRIDGE";
  HardwareModel$1[HardwareModel$1["STATION_G1"] = 25] = "STATION_G1";
  HardwareModel$1[HardwareModel$1["RAK11310"] = 26] = "RAK11310";
  HardwareModel$1[HardwareModel$1["SENSELORA_RP2040"] = 27] = "SENSELORA_RP2040";
  HardwareModel$1[HardwareModel$1["SENSELORA_S3"] = 28] = "SENSELORA_S3";
  HardwareModel$1[HardwareModel$1["CANARYONE"] = 29] = "CANARYONE";
  HardwareModel$1[HardwareModel$1["RP2040_LORA"] = 30] = "RP2040_LORA";
  HardwareModel$1[HardwareModel$1["STATION_G2"] = 31] = "STATION_G2";
  HardwareModel$1[HardwareModel$1["LORA_RELAY_V1"] = 32] = "LORA_RELAY_V1";
  HardwareModel$1[HardwareModel$1["NRF52840DK"] = 33] = "NRF52840DK";
  HardwareModel$1[HardwareModel$1["PPR"] = 34] = "PPR";
  HardwareModel$1[HardwareModel$1["GENIEBLOCKS"] = 35] = "GENIEBLOCKS";
  HardwareModel$1[HardwareModel$1["NRF52_UNKNOWN"] = 36] = "NRF52_UNKNOWN";
  HardwareModel$1[HardwareModel$1["PORTDUINO"] = 37] = "PORTDUINO";
  HardwareModel$1[HardwareModel$1["ANDROID_SIM"] = 38] = "ANDROID_SIM";
  HardwareModel$1[HardwareModel$1["DIY_V1"] = 39] = "DIY_V1";
  HardwareModel$1[HardwareModel$1["NRF52840_PCA10059"] = 40] = "NRF52840_PCA10059";
  HardwareModel$1[HardwareModel$1["DR_DEV"] = 41] = "DR_DEV";
  HardwareModel$1[HardwareModel$1["M5STACK"] = 42] = "M5STACK";
  HardwareModel$1[HardwareModel$1["HELTEC_V3"] = 43] = "HELTEC_V3";
  HardwareModel$1[HardwareModel$1["HELTEC_WSL_V3"] = 44] = "HELTEC_WSL_V3";
  HardwareModel$1[HardwareModel$1["BETAFPV_2400_TX"] = 45] = "BETAFPV_2400_TX";
  HardwareModel$1[HardwareModel$1["BETAFPV_900_NANO_TX"] = 46] = "BETAFPV_900_NANO_TX";
  HardwareModel$1[HardwareModel$1["RPI_PICO"] = 47] = "RPI_PICO";
  HardwareModel$1[HardwareModel$1["HELTEC_WIRELESS_TRACKER"] = 48] = "HELTEC_WIRELESS_TRACKER";
  HardwareModel$1[HardwareModel$1["HELTEC_WIRELESS_PAPER"] = 49] = "HELTEC_WIRELESS_PAPER";
  HardwareModel$1[HardwareModel$1["T_DECK"] = 50] = "T_DECK";
  HardwareModel$1[HardwareModel$1["T_WATCH_S3"] = 51] = "T_WATCH_S3";
  HardwareModel$1[HardwareModel$1["PICOMPUTER_S3"] = 52] = "PICOMPUTER_S3";
  HardwareModel$1[HardwareModel$1["HELTEC_HT62"] = 53] = "HELTEC_HT62";
  HardwareModel$1[HardwareModel$1["EBYTE_ESP32_S3"] = 54] = "EBYTE_ESP32_S3";
  HardwareModel$1[HardwareModel$1["ESP32_S3_PICO"] = 55] = "ESP32_S3_PICO";
  HardwareModel$1[HardwareModel$1["CHATTER_2"] = 56] = "CHATTER_2";
  HardwareModel$1[HardwareModel$1["HELTEC_WIRELESS_PAPER_V1_0"] = 57] = "HELTEC_WIRELESS_PAPER_V1_0";
  HardwareModel$1[HardwareModel$1["HELTEC_WIRELESS_TRACKER_V1_0"] = 58] = "HELTEC_WIRELESS_TRACKER_V1_0";
  HardwareModel$1[HardwareModel$1["UNPHONE"] = 59] = "UNPHONE";
  HardwareModel$1[HardwareModel$1["TD_LORAC"] = 60] = "TD_LORAC";
  HardwareModel$1[HardwareModel$1["CDEBYTE_EORA_S3"] = 61] = "CDEBYTE_EORA_S3";
  HardwareModel$1[HardwareModel$1["TWC_MESH_V4"] = 62] = "TWC_MESH_V4";
  HardwareModel$1[HardwareModel$1["NRF52_PROMICRO_DIY"] = 63] = "NRF52_PROMICRO_DIY";
  HardwareModel$1[HardwareModel$1["RADIOMASTER_900_BANDIT_NANO"] = 64] = "RADIOMASTER_900_BANDIT_NANO";
  HardwareModel$1[HardwareModel$1["HELTEC_CAPSULE_SENSOR_V3"] = 65] = "HELTEC_CAPSULE_SENSOR_V3";
  HardwareModel$1[HardwareModel$1["HELTEC_VISION_MASTER_T190"] = 66] = "HELTEC_VISION_MASTER_T190";
  HardwareModel$1[HardwareModel$1["HELTEC_VISION_MASTER_E213"] = 67] = "HELTEC_VISION_MASTER_E213";
  HardwareModel$1[HardwareModel$1["HELTEC_VISION_MASTER_E290"] = 68] = "HELTEC_VISION_MASTER_E290";
  HardwareModel$1[HardwareModel$1["HELTEC_MESH_NODE_T114"] = 69] = "HELTEC_MESH_NODE_T114";
  HardwareModel$1[HardwareModel$1["SENSECAP_INDICATOR"] = 70] = "SENSECAP_INDICATOR";
  HardwareModel$1[HardwareModel$1["TRACKER_T1000_E"] = 71] = "TRACKER_T1000_E";
  HardwareModel$1[HardwareModel$1["RAK3172"] = 72] = "RAK3172";
  HardwareModel$1[HardwareModel$1["WIO_E5"] = 73] = "WIO_E5";
  HardwareModel$1[HardwareModel$1["RADIOMASTER_900_BANDIT"] = 74] = "RADIOMASTER_900_BANDIT";
  HardwareModel$1[HardwareModel$1["ME25LS01_4Y10TD"] = 75] = "ME25LS01_4Y10TD";
  HardwareModel$1[HardwareModel$1["RP2040_FEATHER_RFM95"] = 76] = "RP2040_FEATHER_RFM95";
  HardwareModel$1[HardwareModel$1["M5STACK_COREBASIC"] = 77] = "M5STACK_COREBASIC";
  HardwareModel$1[HardwareModel$1["M5STACK_CORE2"] = 78] = "M5STACK_CORE2";
  HardwareModel$1[HardwareModel$1["RPI_PICO2"] = 79] = "RPI_PICO2";
  HardwareModel$1[HardwareModel$1["M5STACK_CORES3"] = 80] = "M5STACK_CORES3";
  HardwareModel$1[HardwareModel$1["SEEED_XIAO_S3"] = 81] = "SEEED_XIAO_S3";
  HardwareModel$1[HardwareModel$1["MS24SF1"] = 82] = "MS24SF1";
  HardwareModel$1[HardwareModel$1["TLORA_C6"] = 83] = "TLORA_C6";
  HardwareModel$1[HardwareModel$1["WISMESH_TAP"] = 84] = "WISMESH_TAP";
  HardwareModel$1[HardwareModel$1["ROUTASTIC"] = 85] = "ROUTASTIC";
  HardwareModel$1[HardwareModel$1["MESH_TAB"] = 86] = "MESH_TAB";
  HardwareModel$1[HardwareModel$1["MESHLINK"] = 87] = "MESHLINK";
  HardwareModel$1[HardwareModel$1["XIAO_NRF52_KIT"] = 88] = "XIAO_NRF52_KIT";
  HardwareModel$1[HardwareModel$1["THINKNODE_M1"] = 89] = "THINKNODE_M1";
  HardwareModel$1[HardwareModel$1["THINKNODE_M2"] = 90] = "THINKNODE_M2";
  HardwareModel$1[HardwareModel$1["T_ETH_ELITE"] = 91] = "T_ETH_ELITE";
  HardwareModel$1[HardwareModel$1["HELTEC_SENSOR_HUB"] = 92] = "HELTEC_SENSOR_HUB";
  HardwareModel$1[HardwareModel$1["RESERVED_FRIED_CHICKEN"] = 93] = "RESERVED_FRIED_CHICKEN";
  HardwareModel$1[HardwareModel$1["HELTEC_MESH_POCKET"] = 94] = "HELTEC_MESH_POCKET";
  HardwareModel$1[HardwareModel$1["SEEED_SOLAR_NODE"] = 95] = "SEEED_SOLAR_NODE";
  HardwareModel$1[HardwareModel$1["NOMADSTAR_METEOR_PRO"] = 96] = "NOMADSTAR_METEOR_PRO";
  HardwareModel$1[HardwareModel$1["CROWPANEL"] = 97] = "CROWPANEL";
  HardwareModel$1[HardwareModel$1["LINK_32"] = 98] = "LINK_32";
  HardwareModel$1[HardwareModel$1["SEEED_WIO_TRACKER_L1"] = 99] = "SEEED_WIO_TRACKER_L1";
  HardwareModel$1[HardwareModel$1["SEEED_WIO_TRACKER_L1_EINK"] = 100] = "SEEED_WIO_TRACKER_L1_EINK";
  HardwareModel$1[HardwareModel$1["QWANTZ_TINY_ARMS"] = 101] = "QWANTZ_TINY_ARMS";
  HardwareModel$1[HardwareModel$1["T_DECK_PRO"] = 102] = "T_DECK_PRO";
  HardwareModel$1[HardwareModel$1["T_LORA_PAGER"] = 103] = "T_LORA_PAGER";
  HardwareModel$1[HardwareModel$1["GAT562_MESH_TRIAL_TRACKER"] = 104] = "GAT562_MESH_TRIAL_TRACKER";
  HardwareModel$1[HardwareModel$1["PRIVATE_HW"] = 255] = "PRIVATE_HW";
  return HardwareModel$1;
}({});
var HardwareModelSchema = /* @__PURE__ */ enumDesc(file_mesh, 0);
var Constants$1 = /* @__PURE__ */ function(Constants$1$1) {
  Constants$1$1[Constants$1$1["ZERO"] = 0] = "ZERO";
  Constants$1$1[Constants$1$1["DATA_PAYLOAD_LEN"] = 233] = "DATA_PAYLOAD_LEN";
  return Constants$1$1;
}({});
var ConstantsSchema = /* @__PURE__ */ enumDesc(file_mesh, 1);
var CriticalErrorCode = /* @__PURE__ */ function(CriticalErrorCode$1) {
  CriticalErrorCode$1[CriticalErrorCode$1["NONE"] = 0] = "NONE";
  CriticalErrorCode$1[CriticalErrorCode$1["TX_WATCHDOG"] = 1] = "TX_WATCHDOG";
  CriticalErrorCode$1[CriticalErrorCode$1["SLEEP_ENTER_WAIT"] = 2] = "SLEEP_ENTER_WAIT";
  CriticalErrorCode$1[CriticalErrorCode$1["NO_RADIO"] = 3] = "NO_RADIO";
  CriticalErrorCode$1[CriticalErrorCode$1["UNSPECIFIED"] = 4] = "UNSPECIFIED";
  CriticalErrorCode$1[CriticalErrorCode$1["UBLOX_UNIT_FAILED"] = 5] = "UBLOX_UNIT_FAILED";
  CriticalErrorCode$1[CriticalErrorCode$1["NO_AXP192"] = 6] = "NO_AXP192";
  CriticalErrorCode$1[CriticalErrorCode$1["INVALID_RADIO_SETTING"] = 7] = "INVALID_RADIO_SETTING";
  CriticalErrorCode$1[CriticalErrorCode$1["TRANSMIT_FAILED"] = 8] = "TRANSMIT_FAILED";
  CriticalErrorCode$1[CriticalErrorCode$1["BROWNOUT"] = 9] = "BROWNOUT";
  CriticalErrorCode$1[CriticalErrorCode$1["SX1262_FAILURE"] = 10] = "SX1262_FAILURE";
  CriticalErrorCode$1[CriticalErrorCode$1["RADIO_SPI_BUG"] = 11] = "RADIO_SPI_BUG";
  CriticalErrorCode$1[CriticalErrorCode$1["FLASH_CORRUPTION_RECOVERABLE"] = 12] = "FLASH_CORRUPTION_RECOVERABLE";
  CriticalErrorCode$1[CriticalErrorCode$1["FLASH_CORRUPTION_UNRECOVERABLE"] = 13] = "FLASH_CORRUPTION_UNRECOVERABLE";
  return CriticalErrorCode$1;
}({});
var CriticalErrorCodeSchema = /* @__PURE__ */ enumDesc(file_mesh, 2);
var ExcludedModules = /* @__PURE__ */ function(ExcludedModules$1) {
  ExcludedModules$1[ExcludedModules$1["EXCLUDED_NONE"] = 0] = "EXCLUDED_NONE";
  ExcludedModules$1[ExcludedModules$1["MQTT_CONFIG"] = 1] = "MQTT_CONFIG";
  ExcludedModules$1[ExcludedModules$1["SERIAL_CONFIG"] = 2] = "SERIAL_CONFIG";
  ExcludedModules$1[ExcludedModules$1["EXTNOTIF_CONFIG"] = 4] = "EXTNOTIF_CONFIG";
  ExcludedModules$1[ExcludedModules$1["STOREFORWARD_CONFIG"] = 8] = "STOREFORWARD_CONFIG";
  ExcludedModules$1[ExcludedModules$1["RANGETEST_CONFIG"] = 16] = "RANGETEST_CONFIG";
  ExcludedModules$1[ExcludedModules$1["TELEMETRY_CONFIG"] = 32] = "TELEMETRY_CONFIG";
  ExcludedModules$1[ExcludedModules$1["CANNEDMSG_CONFIG"] = 64] = "CANNEDMSG_CONFIG";
  ExcludedModules$1[ExcludedModules$1["AUDIO_CONFIG"] = 128] = "AUDIO_CONFIG";
  ExcludedModules$1[ExcludedModules$1["REMOTEHARDWARE_CONFIG"] = 256] = "REMOTEHARDWARE_CONFIG";
  ExcludedModules$1[ExcludedModules$1["NEIGHBORINFO_CONFIG"] = 512] = "NEIGHBORINFO_CONFIG";
  ExcludedModules$1[ExcludedModules$1["AMBIENTLIGHTING_CONFIG"] = 1024] = "AMBIENTLIGHTING_CONFIG";
  ExcludedModules$1[ExcludedModules$1["DETECTIONSENSOR_CONFIG"] = 2048] = "DETECTIONSENSOR_CONFIG";
  ExcludedModules$1[ExcludedModules$1["PAXCOUNTER_CONFIG"] = 4096] = "PAXCOUNTER_CONFIG";
  ExcludedModules$1[ExcludedModules$1["BLUETOOTH_CONFIG"] = 8192] = "BLUETOOTH_CONFIG";
  ExcludedModules$1[ExcludedModules$1["NETWORK_CONFIG"] = 16384] = "NETWORK_CONFIG";
  return ExcludedModules$1;
}({});
var ExcludedModulesSchema = /* @__PURE__ */ enumDesc(file_mesh, 3);
var admin_pb_exports = __export2({
  AdminMessageSchema: () => AdminMessageSchema,
  AdminMessage_BackupLocation: () => AdminMessage_BackupLocation,
  AdminMessage_BackupLocationSchema: () => AdminMessage_BackupLocationSchema,
  AdminMessage_ConfigType: () => AdminMessage_ConfigType,
  AdminMessage_ConfigTypeSchema: () => AdminMessage_ConfigTypeSchema,
  AdminMessage_InputEventSchema: () => AdminMessage_InputEventSchema,
  AdminMessage_ModuleConfigType: () => AdminMessage_ModuleConfigType,
  AdminMessage_ModuleConfigTypeSchema: () => AdminMessage_ModuleConfigTypeSchema,
  HamParametersSchema: () => HamParametersSchema,
  KeyVerificationAdminSchema: () => KeyVerificationAdminSchema,
  KeyVerificationAdmin_MessageType: () => KeyVerificationAdmin_MessageType,
  KeyVerificationAdmin_MessageTypeSchema: () => KeyVerificationAdmin_MessageTypeSchema,
  NodeRemoteHardwarePinsResponseSchema: () => NodeRemoteHardwarePinsResponseSchema,
  SharedContactSchema: () => SharedContactSchema,
  file_admin: () => file_admin
});
var file_admin = /* @__PURE__ */ fileDesc("CgthZG1pbi5wcm90bxIKbWVzaHRhc3RpYyLWGAoMQWRtaW5NZXNzYWdlEhcKD3Nlc3Npb25fcGFzc2tleRhlIAEoDBIdChNnZXRfY2hhbm5lbF9yZXF1ZXN0GAEgASgNSAASMwoUZ2V0X2NoYW5uZWxfcmVzcG9uc2UYAiABKAsyEy5tZXNodGFzdGljLkNoYW5uZWxIABIbChFnZXRfb3duZXJfcmVxdWVzdBgDIAEoCEgAEi4KEmdldF9vd25lcl9yZXNwb25zZRgEIAEoCzIQLm1lc2h0YXN0aWMuVXNlckgAEkEKEmdldF9jb25maWdfcmVxdWVzdBgFIAEoDjIjLm1lc2h0YXN0aWMuQWRtaW5NZXNzYWdlLkNvbmZpZ1R5cGVIABIxChNnZXRfY29uZmlnX3Jlc3BvbnNlGAYgASgLMhIubWVzaHRhc3RpYy5Db25maWdIABJOChlnZXRfbW9kdWxlX2NvbmZpZ19yZXF1ZXN0GAcgASgOMikubWVzaHRhc3RpYy5BZG1pbk1lc3NhZ2UuTW9kdWxlQ29uZmlnVHlwZUgAEj4KGmdldF9tb2R1bGVfY29uZmlnX3Jlc3BvbnNlGAggASgLMhgubWVzaHRhc3RpYy5Nb2R1bGVDb25maWdIABI0CipnZXRfY2FubmVkX21lc3NhZ2VfbW9kdWxlX21lc3NhZ2VzX3JlcXVlc3QYCiABKAhIABI1CitnZXRfY2FubmVkX21lc3NhZ2VfbW9kdWxlX21lc3NhZ2VzX3Jlc3BvbnNlGAsgASgJSAASJQobZ2V0X2RldmljZV9tZXRhZGF0YV9yZXF1ZXN0GAwgASgISAASQgocZ2V0X2RldmljZV9tZXRhZGF0YV9yZXNwb25zZRgNIAEoCzIaLm1lc2h0YXN0aWMuRGV2aWNlTWV0YWRhdGFIABIeChRnZXRfcmluZ3RvbmVfcmVxdWVzdBgOIAEoCEgAEh8KFWdldF9yaW5ndG9uZV9yZXNwb25zZRgPIAEoCUgAEi4KJGdldF9kZXZpY2VfY29ubmVjdGlvbl9zdGF0dXNfcmVxdWVzdBgQIAEoCEgAElMKJWdldF9kZXZpY2VfY29ubmVjdGlvbl9zdGF0dXNfcmVzcG9uc2UYESABKAsyIi5tZXNodGFzdGljLkRldmljZUNvbm5lY3Rpb25TdGF0dXNIABIxCgxzZXRfaGFtX21vZGUYEiABKAsyGS5tZXNodGFzdGljLkhhbVBhcmFtZXRlcnNIABIvCiVnZXRfbm9kZV9yZW1vdGVfaGFyZHdhcmVfcGluc19yZXF1ZXN0GBMgASgISAASXAomZ2V0X25vZGVfcmVtb3RlX2hhcmR3YXJlX3BpbnNfcmVzcG9uc2UYFCABKAsyKi5tZXNodGFzdGljLk5vZGVSZW1vdGVIYXJkd2FyZVBpbnNSZXNwb25zZUgAEiAKFmVudGVyX2RmdV9tb2RlX3JlcXVlc3QYFSABKAhIABIdChNkZWxldGVfZmlsZV9yZXF1ZXN0GBYgASgJSAASEwoJc2V0X3NjYWxlGBcgASgNSAASRQoSYmFja3VwX3ByZWZlcmVuY2VzGBggASgOMicubWVzaHRhc3RpYy5BZG1pbk1lc3NhZ2UuQmFja3VwTG9jYXRpb25IABJGChNyZXN0b3JlX3ByZWZlcmVuY2VzGBkgASgOMicubWVzaHRhc3RpYy5BZG1pbk1lc3NhZ2UuQmFja3VwTG9jYXRpb25IABJMChlyZW1vdmVfYmFja3VwX3ByZWZlcmVuY2VzGBogASgOMicubWVzaHRhc3RpYy5BZG1pbk1lc3NhZ2UuQmFja3VwTG9jYXRpb25IABI/ChBzZW5kX2lucHV0X2V2ZW50GBsgASgLMiMubWVzaHRhc3RpYy5BZG1pbk1lc3NhZ2UuSW5wdXRFdmVudEgAEiUKCXNldF9vd25lchggIAEoCzIQLm1lc2h0YXN0aWMuVXNlckgAEioKC3NldF9jaGFubmVsGCEgASgLMhMubWVzaHRhc3RpYy5DaGFubmVsSAASKAoKc2V0X2NvbmZpZxgiIAEoCzISLm1lc2h0YXN0aWMuQ29uZmlnSAASNQoRc2V0X21vZHVsZV9jb25maWcYIyABKAsyGC5tZXNodGFzdGljLk1vZHVsZUNvbmZpZ0gAEiwKInNldF9jYW5uZWRfbWVzc2FnZV9tb2R1bGVfbWVzc2FnZXMYJCABKAlIABIeChRzZXRfcmluZ3RvbmVfbWVzc2FnZRglIAEoCUgAEhsKEXJlbW92ZV9ieV9ub2RlbnVtGCYgASgNSAASGwoRc2V0X2Zhdm9yaXRlX25vZGUYJyABKA1IABIeChRyZW1vdmVfZmF2b3JpdGVfbm9kZRgoIAEoDUgAEjIKEnNldF9maXhlZF9wb3NpdGlvbhgpIAEoCzIULm1lc2h0YXN0aWMuUG9zaXRpb25IABIfChVyZW1vdmVfZml4ZWRfcG9zaXRpb24YKiABKAhIABIXCg1zZXRfdGltZV9vbmx5GCsgASgHSAASHwoVZ2V0X3VpX2NvbmZpZ19yZXF1ZXN0GCwgASgISAASPAoWZ2V0X3VpX2NvbmZpZ19yZXNwb25zZRgtIAEoCzIaLm1lc2h0YXN0aWMuRGV2aWNlVUlDb25maWdIABI1Cg9zdG9yZV91aV9jb25maWcYLiABKAsyGi5tZXNodGFzdGljLkRldmljZVVJQ29uZmlnSAASGgoQc2V0X2lnbm9yZWRfbm9kZRgvIAEoDUgAEh0KE3JlbW92ZV9pZ25vcmVkX25vZGUYMCABKA1IABIdChNiZWdpbl9lZGl0X3NldHRpbmdzGEAgASgISAASHgoUY29tbWl0X2VkaXRfc2V0dGluZ3MYQSABKAhIABIwCgthZGRfY29udGFjdBhCIAEoCzIZLm1lc2h0YXN0aWMuU2hhcmVkQ29udGFjdEgAEjwKEGtleV92ZXJpZmljYXRpb24YQyABKAsyIC5tZXNodGFzdGljLktleVZlcmlmaWNhdGlvbkFkbWluSAASHgoUZmFjdG9yeV9yZXNldF9kZXZpY2UYXiABKAVIABIcChJyZWJvb3Rfb3RhX3NlY29uZHMYXyABKAVIABIYCg5leGl0X3NpbXVsYXRvchhgIAEoCEgAEhgKDnJlYm9vdF9zZWNvbmRzGGEgASgFSAASGgoQc2h1dGRvd25fc2Vjb25kcxhiIAEoBUgAEh4KFGZhY3RvcnlfcmVzZXRfY29uZmlnGGMgASgFSAASFgoMbm9kZWRiX3Jlc2V0GGQgASgFSAAaUwoKSW5wdXRFdmVudBISCgpldmVudF9jb2RlGAEgASgNEg8KB2tiX2NoYXIYAiABKA0SDwoHdG91Y2hfeBgDIAEoDRIPCgd0b3VjaF95GAQgASgNItYBCgpDb25maWdUeXBlEhEKDURFVklDRV9DT05GSUcQABITCg9QT1NJVElPTl9DT05GSUcQARIQCgxQT1dFUl9DT05GSUcQAhISCg5ORVRXT1JLX0NPTkZJRxADEhIKDkRJU1BMQVlfQ09ORklHEAQSDwoLTE9SQV9DT05GSUcQBRIUChBCTFVFVE9PVEhfQ09ORklHEAYSEwoPU0VDVVJJVFlfQ09ORklHEAcSFQoRU0VTU0lPTktFWV9DT05GSUcQCBITCg9ERVZJQ0VVSV9DT05GSUcQCSK7AgoQTW9kdWxlQ29uZmlnVHlwZRIPCgtNUVRUX0NPTkZJRxAAEhEKDVNFUklBTF9DT05GSUcQARITCg9FWFROT1RJRl9DT05GSUcQAhIXChNTVE9SRUZPUldBUkRfQ09ORklHEAMSFAoQUkFOR0VURVNUX0NPTkZJRxAEEhQKEFRFTEVNRVRSWV9DT05GSUcQBRIUChBDQU5ORURNU0dfQ09ORklHEAYSEAoMQVVESU9fQ09ORklHEAcSGQoVUkVNT1RFSEFSRFdBUkVfQ09ORklHEAgSFwoTTkVJR0hCT1JJTkZPX0NPTkZJRxAJEhoKFkFNQklFTlRMSUdIVElOR19DT05GSUcQChIaChZERVRFQ1RJT05TRU5TT1JfQ09ORklHEAsSFQoRUEFYQ09VTlRFUl9DT05GSUcQDCIjCg5CYWNrdXBMb2NhdGlvbhIJCgVGTEFTSBAAEgYKAlNEEAFCEQoPcGF5bG9hZF92YXJpYW50IlsKDUhhbVBhcmFtZXRlcnMSEQoJY2FsbF9zaWduGAEgASgJEhAKCHR4X3Bvd2VyGAIgASgFEhEKCWZyZXF1ZW5jeRgDIAEoAhISCgpzaG9ydF9uYW1lGAQgASgJImYKHk5vZGVSZW1vdGVIYXJkd2FyZVBpbnNSZXNwb25zZRJEChlub2RlX3JlbW90ZV9oYXJkd2FyZV9waW5zGAEgAygLMiEubWVzaHRhc3RpYy5Ob2RlUmVtb3RlSGFyZHdhcmVQaW4iWAoNU2hhcmVkQ29udGFjdBIQCghub2RlX251bRgBIAEoDRIeCgR1c2VyGAIgASgLMhAubWVzaHRhc3RpYy5Vc2VyEhUKDXNob3VsZF9pZ25vcmUYAyABKAginAIKFEtleVZlcmlmaWNhdGlvbkFkbWluEkIKDG1lc3NhZ2VfdHlwZRgBIAEoDjIsLm1lc2h0YXN0aWMuS2V5VmVyaWZpY2F0aW9uQWRtaW4uTWVzc2FnZVR5cGUSFgoOcmVtb3RlX25vZGVudW0YAiABKA0SDQoFbm9uY2UYAyABKAQSHAoPc2VjdXJpdHlfbnVtYmVyGAQgASgNSACIAQEiZwoLTWVzc2FnZVR5cGUSGQoVSU5JVElBVEVfVkVSSUZJQ0FUSU9OEAASGwoXUFJPVklERV9TRUNVUklUWV9OVU1CRVIQARINCglET19WRVJJRlkQAhIRCg1ET19OT1RfVkVSSUZZEANCEgoQX3NlY3VyaXR5X251bWJlckJgChNjb20uZ2Vla3N2aWxsZS5tZXNoQgtBZG1pblByb3Rvc1oiZ2l0aHViLmNvbS9tZXNodGFzdGljL2dvL2dlbmVyYXRlZKoCFE1lc2h0YXN0aWMuUHJvdG9idWZzugIAYgZwcm90bzM", [
  file_channel,
  file_config,
  file_connection_status,
  file_mesh,
  file_module_config,
  file_device_ui
]);
var AdminMessageSchema = /* @__PURE__ */ messageDesc(file_admin, 0);
var AdminMessage_InputEventSchema = /* @__PURE__ */ messageDesc(file_admin, 0, 0);
var AdminMessage_ConfigType = /* @__PURE__ */ function(AdminMessage_ConfigType$1) {
  AdminMessage_ConfigType$1[AdminMessage_ConfigType$1["DEVICE_CONFIG"] = 0] = "DEVICE_CONFIG";
  AdminMessage_ConfigType$1[AdminMessage_ConfigType$1["POSITION_CONFIG"] = 1] = "POSITION_CONFIG";
  AdminMessage_ConfigType$1[AdminMessage_ConfigType$1["POWER_CONFIG"] = 2] = "POWER_CONFIG";
  AdminMessage_ConfigType$1[AdminMessage_ConfigType$1["NETWORK_CONFIG"] = 3] = "NETWORK_CONFIG";
  AdminMessage_ConfigType$1[AdminMessage_ConfigType$1["DISPLAY_CONFIG"] = 4] = "DISPLAY_CONFIG";
  AdminMessage_ConfigType$1[AdminMessage_ConfigType$1["LORA_CONFIG"] = 5] = "LORA_CONFIG";
  AdminMessage_ConfigType$1[AdminMessage_ConfigType$1["BLUETOOTH_CONFIG"] = 6] = "BLUETOOTH_CONFIG";
  AdminMessage_ConfigType$1[AdminMessage_ConfigType$1["SECURITY_CONFIG"] = 7] = "SECURITY_CONFIG";
  AdminMessage_ConfigType$1[AdminMessage_ConfigType$1["SESSIONKEY_CONFIG"] = 8] = "SESSIONKEY_CONFIG";
  AdminMessage_ConfigType$1[AdminMessage_ConfigType$1["DEVICEUI_CONFIG"] = 9] = "DEVICEUI_CONFIG";
  return AdminMessage_ConfigType$1;
}({});
var AdminMessage_ConfigTypeSchema = /* @__PURE__ */ enumDesc(file_admin, 0, 0);
var AdminMessage_ModuleConfigType = /* @__PURE__ */ function(AdminMessage_ModuleConfigType$1) {
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["MQTT_CONFIG"] = 0] = "MQTT_CONFIG";
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["SERIAL_CONFIG"] = 1] = "SERIAL_CONFIG";
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["EXTNOTIF_CONFIG"] = 2] = "EXTNOTIF_CONFIG";
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["STOREFORWARD_CONFIG"] = 3] = "STOREFORWARD_CONFIG";
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["RANGETEST_CONFIG"] = 4] = "RANGETEST_CONFIG";
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["TELEMETRY_CONFIG"] = 5] = "TELEMETRY_CONFIG";
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["CANNEDMSG_CONFIG"] = 6] = "CANNEDMSG_CONFIG";
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["AUDIO_CONFIG"] = 7] = "AUDIO_CONFIG";
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["REMOTEHARDWARE_CONFIG"] = 8] = "REMOTEHARDWARE_CONFIG";
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["NEIGHBORINFO_CONFIG"] = 9] = "NEIGHBORINFO_CONFIG";
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["AMBIENTLIGHTING_CONFIG"] = 10] = "AMBIENTLIGHTING_CONFIG";
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["DETECTIONSENSOR_CONFIG"] = 11] = "DETECTIONSENSOR_CONFIG";
  AdminMessage_ModuleConfigType$1[AdminMessage_ModuleConfigType$1["PAXCOUNTER_CONFIG"] = 12] = "PAXCOUNTER_CONFIG";
  return AdminMessage_ModuleConfigType$1;
}({});
var AdminMessage_ModuleConfigTypeSchema = /* @__PURE__ */ enumDesc(file_admin, 0, 1);
var AdminMessage_BackupLocation = /* @__PURE__ */ function(AdminMessage_BackupLocation$1) {
  AdminMessage_BackupLocation$1[AdminMessage_BackupLocation$1["FLASH"] = 0] = "FLASH";
  AdminMessage_BackupLocation$1[AdminMessage_BackupLocation$1["SD"] = 1] = "SD";
  return AdminMessage_BackupLocation$1;
}({});
var AdminMessage_BackupLocationSchema = /* @__PURE__ */ enumDesc(file_admin, 0, 2);
var HamParametersSchema = /* @__PURE__ */ messageDesc(file_admin, 1);
var NodeRemoteHardwarePinsResponseSchema = /* @__PURE__ */ messageDesc(file_admin, 2);
var SharedContactSchema = /* @__PURE__ */ messageDesc(file_admin, 3);
var KeyVerificationAdminSchema = /* @__PURE__ */ messageDesc(file_admin, 4);
var KeyVerificationAdmin_MessageType = /* @__PURE__ */ function(KeyVerificationAdmin_MessageType$1) {
  KeyVerificationAdmin_MessageType$1[KeyVerificationAdmin_MessageType$1["INITIATE_VERIFICATION"] = 0] = "INITIATE_VERIFICATION";
  KeyVerificationAdmin_MessageType$1[KeyVerificationAdmin_MessageType$1["PROVIDE_SECURITY_NUMBER"] = 1] = "PROVIDE_SECURITY_NUMBER";
  KeyVerificationAdmin_MessageType$1[KeyVerificationAdmin_MessageType$1["DO_VERIFY"] = 2] = "DO_VERIFY";
  KeyVerificationAdmin_MessageType$1[KeyVerificationAdmin_MessageType$1["DO_NOT_VERIFY"] = 3] = "DO_NOT_VERIFY";
  return KeyVerificationAdmin_MessageType$1;
}({});
var KeyVerificationAdmin_MessageTypeSchema = /* @__PURE__ */ enumDesc(file_admin, 4, 0);
var apponly_pb_exports = __export2({
  ChannelSetSchema: () => ChannelSetSchema,
  file_apponly: () => file_apponly
});
var file_apponly = /* @__PURE__ */ fileDesc("Cg1hcHBvbmx5LnByb3RvEgptZXNodGFzdGljIm8KCkNoYW5uZWxTZXQSLQoIc2V0dGluZ3MYASADKAsyGy5tZXNodGFzdGljLkNoYW5uZWxTZXR0aW5ncxIyCgtsb3JhX2NvbmZpZxgCIAEoCzIdLm1lc2h0YXN0aWMuQ29uZmlnLkxvUmFDb25maWdCYgoTY29tLmdlZWtzdmlsbGUubWVzaEINQXBwT25seVByb3Rvc1oiZ2l0aHViLmNvbS9tZXNodGFzdGljL2dvL2dlbmVyYXRlZKoCFE1lc2h0YXN0aWMuUHJvdG9idWZzugIAYgZwcm90bzM", [file_channel, file_config]);
var ChannelSetSchema = /* @__PURE__ */ messageDesc(file_apponly, 0);
var atak_pb_exports = __export2({
  ContactSchema: () => ContactSchema,
  GeoChatSchema: () => GeoChatSchema,
  GroupSchema: () => GroupSchema,
  MemberRole: () => MemberRole,
  MemberRoleSchema: () => MemberRoleSchema,
  PLISchema: () => PLISchema,
  StatusSchema: () => StatusSchema,
  TAKPacketSchema: () => TAKPacketSchema,
  Team: () => Team,
  TeamSchema: () => TeamSchema,
  file_atak: () => file_atak
});
var file_atak = /* @__PURE__ */ fileDesc("CgphdGFrLnByb3RvEgptZXNodGFzdGljIvgBCglUQUtQYWNrZXQSFQoNaXNfY29tcHJlc3NlZBgBIAEoCBIkCgdjb250YWN0GAIgASgLMhMubWVzaHRhc3RpYy5Db250YWN0EiAKBWdyb3VwGAMgASgLMhEubWVzaHRhc3RpYy5Hcm91cBIiCgZzdGF0dXMYBCABKAsyEi5tZXNodGFzdGljLlN0YXR1cxIeCgNwbGkYBSABKAsyDy5tZXNodGFzdGljLlBMSUgAEiMKBGNoYXQYBiABKAsyEy5tZXNodGFzdGljLkdlb0NoYXRIABIQCgZkZXRhaWwYByABKAxIAEIRCg9wYXlsb2FkX3ZhcmlhbnQiXAoHR2VvQ2hhdBIPCgdtZXNzYWdlGAEgASgJEg8KAnRvGAIgASgJSACIAQESGAoLdG9fY2FsbHNpZ24YAyABKAlIAYgBAUIFCgNfdG9CDgoMX3RvX2NhbGxzaWduIk0KBUdyb3VwEiQKBHJvbGUYASABKA4yFi5tZXNodGFzdGljLk1lbWJlclJvbGUSHgoEdGVhbRgCIAEoDjIQLm1lc2h0YXN0aWMuVGVhbSIZCgZTdGF0dXMSDwoHYmF0dGVyeRgBIAEoDSI0CgdDb250YWN0EhAKCGNhbGxzaWduGAEgASgJEhcKD2RldmljZV9jYWxsc2lnbhgCIAEoCSJfCgNQTEkSEgoKbGF0aXR1ZGVfaRgBIAEoDxITCgtsb25naXR1ZGVfaRgCIAEoDxIQCghhbHRpdHVkZRgDIAEoBRINCgVzcGVlZBgEIAEoDRIOCgZjb3Vyc2UYBSABKA0qwAEKBFRlYW0SFAoQVW5zcGVjaWZlZF9Db2xvchAAEgkKBVdoaXRlEAESCgoGWWVsbG93EAISCgoGT3JhbmdlEAMSCwoHTWFnZW50YRAEEgcKA1JlZBAFEgoKBk1hcm9vbhAGEgoKBlB1cnBsZRAHEg0KCURhcmtfQmx1ZRAIEggKBEJsdWUQCRIICgRDeWFuEAoSCAoEVGVhbBALEgkKBUdyZWVuEAwSDgoKRGFya19HcmVlbhANEgkKBUJyb3duEA4qfwoKTWVtYmVyUm9sZRIOCgpVbnNwZWNpZmVkEAASDgoKVGVhbU1lbWJlchABEgwKCFRlYW1MZWFkEAISBgoCSFEQAxIKCgZTbmlwZXIQBBIJCgVNZWRpYxAFEhMKD0ZvcndhcmRPYnNlcnZlchAGEgcKA1JUTxAHEgYKAks5EAhCXwoTY29tLmdlZWtzdmlsbGUubWVzaEIKQVRBS1Byb3Rvc1oiZ2l0aHViLmNvbS9tZXNodGFzdGljL2dvL2dlbmVyYXRlZKoCFE1lc2h0YXN0aWMuUHJvdG9idWZzugIAYgZwcm90bzM");
var TAKPacketSchema = /* @__PURE__ */ messageDesc(file_atak, 0);
var GeoChatSchema = /* @__PURE__ */ messageDesc(file_atak, 1);
var GroupSchema = /* @__PURE__ */ messageDesc(file_atak, 2);
var StatusSchema = /* @__PURE__ */ messageDesc(file_atak, 3);
var ContactSchema = /* @__PURE__ */ messageDesc(file_atak, 4);
var PLISchema = /* @__PURE__ */ messageDesc(file_atak, 5);
var Team = /* @__PURE__ */ function(Team$1) {
  Team$1[Team$1["Unspecifed_Color"] = 0] = "Unspecifed_Color";
  Team$1[Team$1["White"] = 1] = "White";
  Team$1[Team$1["Yellow"] = 2] = "Yellow";
  Team$1[Team$1["Orange"] = 3] = "Orange";
  Team$1[Team$1["Magenta"] = 4] = "Magenta";
  Team$1[Team$1["Red"] = 5] = "Red";
  Team$1[Team$1["Maroon"] = 6] = "Maroon";
  Team$1[Team$1["Purple"] = 7] = "Purple";
  Team$1[Team$1["Dark_Blue"] = 8] = "Dark_Blue";
  Team$1[Team$1["Blue"] = 9] = "Blue";
  Team$1[Team$1["Cyan"] = 10] = "Cyan";
  Team$1[Team$1["Teal"] = 11] = "Teal";
  Team$1[Team$1["Green"] = 12] = "Green";
  Team$1[Team$1["Dark_Green"] = 13] = "Dark_Green";
  Team$1[Team$1["Brown"] = 14] = "Brown";
  return Team$1;
}({});
var TeamSchema = /* @__PURE__ */ enumDesc(file_atak, 0);
var MemberRole = /* @__PURE__ */ function(MemberRole$1) {
  MemberRole$1[MemberRole$1["Unspecifed"] = 0] = "Unspecifed";
  MemberRole$1[MemberRole$1["TeamMember"] = 1] = "TeamMember";
  MemberRole$1[MemberRole$1["TeamLead"] = 2] = "TeamLead";
  MemberRole$1[MemberRole$1["HQ"] = 3] = "HQ";
  MemberRole$1[MemberRole$1["Sniper"] = 4] = "Sniper";
  MemberRole$1[MemberRole$1["Medic"] = 5] = "Medic";
  MemberRole$1[MemberRole$1["ForwardObserver"] = 6] = "ForwardObserver";
  MemberRole$1[MemberRole$1["RTO"] = 7] = "RTO";
  MemberRole$1[MemberRole$1["K9"] = 8] = "K9";
  return MemberRole$1;
}({});
var MemberRoleSchema = /* @__PURE__ */ enumDesc(file_atak, 1);
var cannedmessages_pb_exports = __export2({
  CannedMessageModuleConfigSchema: () => CannedMessageModuleConfigSchema,
  file_cannedmessages: () => file_cannedmessages
});
var file_cannedmessages = /* @__PURE__ */ fileDesc("ChRjYW5uZWRtZXNzYWdlcy5wcm90bxIKbWVzaHRhc3RpYyItChlDYW5uZWRNZXNzYWdlTW9kdWxlQ29uZmlnEhAKCG1lc3NhZ2VzGAEgASgJQm4KE2NvbS5nZWVrc3ZpbGxlLm1lc2hCGUNhbm5lZE1lc3NhZ2VDb25maWdQcm90b3NaImdpdGh1Yi5jb20vbWVzaHRhc3RpYy9nby9nZW5lcmF0ZWSqAhRNZXNodGFzdGljLlByb3RvYnVmc7oCAGIGcHJvdG8z");
var CannedMessageModuleConfigSchema = /* @__PURE__ */ messageDesc(file_cannedmessages, 0);
var localonly_pb_exports = __export2({
  LocalConfigSchema: () => LocalConfigSchema,
  LocalModuleConfigSchema: () => LocalModuleConfigSchema,
  file_localonly: () => file_localonly
});
var file_localonly = /* @__PURE__ */ fileDesc("Cg9sb2NhbG9ubHkucHJvdG8SCm1lc2h0YXN0aWMisgMKC0xvY2FsQ29uZmlnEi8KBmRldmljZRgBIAEoCzIfLm1lc2h0YXN0aWMuQ29uZmlnLkRldmljZUNvbmZpZxIzCghwb3NpdGlvbhgCIAEoCzIhLm1lc2h0YXN0aWMuQ29uZmlnLlBvc2l0aW9uQ29uZmlnEi0KBXBvd2VyGAMgASgLMh4ubWVzaHRhc3RpYy5Db25maWcuUG93ZXJDb25maWcSMQoHbmV0d29yaxgEIAEoCzIgLm1lc2h0YXN0aWMuQ29uZmlnLk5ldHdvcmtDb25maWcSMQoHZGlzcGxheRgFIAEoCzIgLm1lc2h0YXN0aWMuQ29uZmlnLkRpc3BsYXlDb25maWcSKwoEbG9yYRgGIAEoCzIdLm1lc2h0YXN0aWMuQ29uZmlnLkxvUmFDb25maWcSNQoJYmx1ZXRvb3RoGAcgASgLMiIubWVzaHRhc3RpYy5Db25maWcuQmx1ZXRvb3RoQ29uZmlnEg8KB3ZlcnNpb24YCCABKA0SMwoIc2VjdXJpdHkYCSABKAsyIS5tZXNodGFzdGljLkNvbmZpZy5TZWN1cml0eUNvbmZpZyL7BgoRTG9jYWxNb2R1bGVDb25maWcSMQoEbXF0dBgBIAEoCzIjLm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnLk1RVFRDb25maWcSNQoGc2VyaWFsGAIgASgLMiUubWVzaHRhc3RpYy5Nb2R1bGVDb25maWcuU2VyaWFsQ29uZmlnElIKFWV4dGVybmFsX25vdGlmaWNhdGlvbhgDIAEoCzIzLm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnLkV4dGVybmFsTm90aWZpY2F0aW9uQ29uZmlnEkIKDXN0b3JlX2ZvcndhcmQYBCABKAsyKy5tZXNodGFzdGljLk1vZHVsZUNvbmZpZy5TdG9yZUZvcndhcmRDb25maWcSPAoKcmFuZ2VfdGVzdBgFIAEoCzIoLm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnLlJhbmdlVGVzdENvbmZpZxI7Cgl0ZWxlbWV0cnkYBiABKAsyKC5tZXNodGFzdGljLk1vZHVsZUNvbmZpZy5UZWxlbWV0cnlDb25maWcSRAoOY2FubmVkX21lc3NhZ2UYByABKAsyLC5tZXNodGFzdGljLk1vZHVsZUNvbmZpZy5DYW5uZWRNZXNzYWdlQ29uZmlnEjMKBWF1ZGlvGAkgASgLMiQubWVzaHRhc3RpYy5Nb2R1bGVDb25maWcuQXVkaW9Db25maWcSRgoPcmVtb3RlX2hhcmR3YXJlGAogASgLMi0ubWVzaHRhc3RpYy5Nb2R1bGVDb25maWcuUmVtb3RlSGFyZHdhcmVDb25maWcSQgoNbmVpZ2hib3JfaW5mbxgLIAEoCzIrLm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnLk5laWdoYm9ySW5mb0NvbmZpZxJIChBhbWJpZW50X2xpZ2h0aW5nGAwgASgLMi4ubWVzaHRhc3RpYy5Nb2R1bGVDb25maWcuQW1iaWVudExpZ2h0aW5nQ29uZmlnEkgKEGRldGVjdGlvbl9zZW5zb3IYDSABKAsyLi5tZXNodGFzdGljLk1vZHVsZUNvbmZpZy5EZXRlY3Rpb25TZW5zb3JDb25maWcSPQoKcGF4Y291bnRlchgOIAEoCzIpLm1lc2h0YXN0aWMuTW9kdWxlQ29uZmlnLlBheGNvdW50ZXJDb25maWcSDwoHdmVyc2lvbhgIIAEoDUJkChNjb20uZ2Vla3N2aWxsZS5tZXNoQg9Mb2NhbE9ubHlQcm90b3NaImdpdGh1Yi5jb20vbWVzaHRhc3RpYy9nby9nZW5lcmF0ZWSqAhRNZXNodGFzdGljLlByb3RvYnVmc7oCAGIGcHJvdG8z", [file_config, file_module_config]);
var LocalConfigSchema = /* @__PURE__ */ messageDesc(file_localonly, 0);
var LocalModuleConfigSchema = /* @__PURE__ */ messageDesc(file_localonly, 1);
var clientonly_pb_exports = __export2({
  DeviceProfileSchema: () => DeviceProfileSchema,
  file_clientonly: () => file_clientonly
});
var file_clientonly = /* @__PURE__ */ fileDesc("ChBjbGllbnRvbmx5LnByb3RvEgptZXNodGFzdGljIqkDCg1EZXZpY2VQcm9maWxlEhYKCWxvbmdfbmFtZRgBIAEoCUgAiAEBEhcKCnNob3J0X25hbWUYAiABKAlIAYgBARIYCgtjaGFubmVsX3VybBgDIAEoCUgCiAEBEiwKBmNvbmZpZxgEIAEoCzIXLm1lc2h0YXN0aWMuTG9jYWxDb25maWdIA4gBARI5Cg1tb2R1bGVfY29uZmlnGAUgASgLMh0ubWVzaHRhc3RpYy5Mb2NhbE1vZHVsZUNvbmZpZ0gEiAEBEjEKDmZpeGVkX3Bvc2l0aW9uGAYgASgLMhQubWVzaHRhc3RpYy5Qb3NpdGlvbkgFiAEBEhUKCHJpbmd0b25lGAcgASgJSAaIAQESHAoPY2FubmVkX21lc3NhZ2VzGAggASgJSAeIAQFCDAoKX2xvbmdfbmFtZUINCgtfc2hvcnRfbmFtZUIOCgxfY2hhbm5lbF91cmxCCQoHX2NvbmZpZ0IQCg5fbW9kdWxlX2NvbmZpZ0IRCg9fZml4ZWRfcG9zaXRpb25CCwoJX3Jpbmd0b25lQhIKEF9jYW5uZWRfbWVzc2FnZXNCZQoTY29tLmdlZWtzdmlsbGUubWVzaEIQQ2xpZW50T25seVByb3Rvc1oiZ2l0aHViLmNvbS9tZXNodGFzdGljL2dvL2dlbmVyYXRlZKoCFE1lc2h0YXN0aWMuUHJvdG9idWZzugIAYgZwcm90bzM", [file_localonly, file_mesh]);
var DeviceProfileSchema = /* @__PURE__ */ messageDesc(file_clientonly, 0);
var mqtt_pb_exports = __export2({
  MapReportSchema: () => MapReportSchema,
  ServiceEnvelopeSchema: () => ServiceEnvelopeSchema,
  file_mqtt: () => file_mqtt
});
var file_mqtt = /* @__PURE__ */ fileDesc("CgptcXR0LnByb3RvEgptZXNodGFzdGljImEKD1NlcnZpY2VFbnZlbG9wZRImCgZwYWNrZXQYASABKAsyFi5tZXNodGFzdGljLk1lc2hQYWNrZXQSEgoKY2hhbm5lbF9pZBgCIAEoCRISCgpnYXRld2F5X2lkGAMgASgJIt8DCglNYXBSZXBvcnQSEQoJbG9uZ19uYW1lGAEgASgJEhIKCnNob3J0X25hbWUYAiABKAkSMgoEcm9sZRgDIAEoDjIkLm1lc2h0YXN0aWMuQ29uZmlnLkRldmljZUNvbmZpZy5Sb2xlEisKCGh3X21vZGVsGAQgASgOMhkubWVzaHRhc3RpYy5IYXJkd2FyZU1vZGVsEhgKEGZpcm13YXJlX3ZlcnNpb24YBSABKAkSOAoGcmVnaW9uGAYgASgOMigubWVzaHRhc3RpYy5Db25maWcuTG9SYUNvbmZpZy5SZWdpb25Db2RlEj8KDG1vZGVtX3ByZXNldBgHIAEoDjIpLm1lc2h0YXN0aWMuQ29uZmlnLkxvUmFDb25maWcuTW9kZW1QcmVzZXQSGwoTaGFzX2RlZmF1bHRfY2hhbm5lbBgIIAEoCBISCgpsYXRpdHVkZV9pGAkgASgPEhMKC2xvbmdpdHVkZV9pGAogASgPEhAKCGFsdGl0dWRlGAsgASgFEhoKEnBvc2l0aW9uX3ByZWNpc2lvbhgMIAEoDRIeChZudW1fb25saW5lX2xvY2FsX25vZGVzGA0gASgNEiEKGWhhc19vcHRlZF9yZXBvcnRfbG9jYXRpb24YDiABKAhCXwoTY29tLmdlZWtzdmlsbGUubWVzaEIKTVFUVFByb3Rvc1oiZ2l0aHViLmNvbS9tZXNodGFzdGljL2dvL2dlbmVyYXRlZKoCFE1lc2h0YXN0aWMuUHJvdG9idWZzugIAYgZwcm90bzM", [file_config, file_mesh]);
var ServiceEnvelopeSchema = /* @__PURE__ */ messageDesc(file_mqtt, 0);
var MapReportSchema = /* @__PURE__ */ messageDesc(file_mqtt, 1);
var paxcount_pb_exports = __export2({
  PaxcountSchema: () => PaxcountSchema,
  file_paxcount: () => file_paxcount
});
var file_paxcount = /* @__PURE__ */ fileDesc("Cg5wYXhjb3VudC5wcm90bxIKbWVzaHRhc3RpYyI1CghQYXhjb3VudBIMCgR3aWZpGAEgASgNEgsKA2JsZRgCIAEoDRIOCgZ1cHRpbWUYAyABKA1CYwoTY29tLmdlZWtzdmlsbGUubWVzaEIOUGF4Y291bnRQcm90b3NaImdpdGh1Yi5jb20vbWVzaHRhc3RpYy9nby9nZW5lcmF0ZWSqAhRNZXNodGFzdGljLlByb3RvYnVmc7oCAGIGcHJvdG8z");
var PaxcountSchema = /* @__PURE__ */ messageDesc(file_paxcount, 0);
var powermon_pb_exports = __export2({
  PowerMonSchema: () => PowerMonSchema,
  PowerMon_State: () => PowerMon_State,
  PowerMon_StateSchema: () => PowerMon_StateSchema,
  PowerStressMessageSchema: () => PowerStressMessageSchema,
  PowerStressMessage_Opcode: () => PowerStressMessage_Opcode,
  PowerStressMessage_OpcodeSchema: () => PowerStressMessage_OpcodeSchema,
  file_powermon: () => file_powermon
});
var file_powermon = /* @__PURE__ */ fileDesc("Cg5wb3dlcm1vbi5wcm90bxIKbWVzaHRhc3RpYyLgAQoIUG93ZXJNb24i0wEKBVN0YXRlEggKBE5vbmUQABIRCg1DUFVfRGVlcFNsZWVwEAESEgoOQ1BVX0xpZ2h0U2xlZXAQAhIMCghWZXh0MV9PbhAEEg0KCUxvcmFfUlhPbhAIEg0KCUxvcmFfVFhPbhAQEhEKDUxvcmFfUlhBY3RpdmUQIBIJCgVCVF9PbhBAEgsKBkxFRF9PbhCAARIOCglTY3JlZW5fT24QgAISEwoOU2NyZWVuX0RyYXdpbmcQgAQSDAoHV2lmaV9PbhCACBIPCgpHUFNfQWN0aXZlEIAQIv8CChJQb3dlclN0cmVzc01lc3NhZ2USMgoDY21kGAEgASgOMiUubWVzaHRhc3RpYy5Qb3dlclN0cmVzc01lc3NhZ2UuT3Bjb2RlEhMKC251bV9zZWNvbmRzGAIgASgCIp8CCgZPcGNvZGUSCQoFVU5TRVQQABIOCgpQUklOVF9JTkZPEAESDwoLRk9SQ0VfUVVJRVQQAhINCglFTkRfUVVJRVQQAxINCglTQ1JFRU5fT04QEBIOCgpTQ1JFRU5fT0ZGEBESDAoIQ1BVX0lETEUQIBIRCg1DUFVfREVFUFNMRUVQECESDgoKQ1BVX0ZVTExPThAiEgoKBkxFRF9PThAwEgsKB0xFRF9PRkYQMRIMCghMT1JBX09GRhBAEgsKB0xPUkFfVFgQQRILCgdMT1JBX1JYEEISCgoGQlRfT0ZGEFASCQoFQlRfT04QURIMCghXSUZJX09GRhBgEgsKB1dJRklfT04QYRILCgdHUFNfT0ZGEHASCgoGR1BTX09OEHFCYwoTY29tLmdlZWtzdmlsbGUubWVzaEIOUG93ZXJNb25Qcm90b3NaImdpdGh1Yi5jb20vbWVzaHRhc3RpYy9nby9nZW5lcmF0ZWSqAhRNZXNodGFzdGljLlByb3RvYnVmc7oCAGIGcHJvdG8z");
var PowerMonSchema = /* @__PURE__ */ messageDesc(file_powermon, 0);
var PowerMon_State = /* @__PURE__ */ function(PowerMon_State$1) {
  PowerMon_State$1[PowerMon_State$1["None"] = 0] = "None";
  PowerMon_State$1[PowerMon_State$1["CPU_DeepSleep"] = 1] = "CPU_DeepSleep";
  PowerMon_State$1[PowerMon_State$1["CPU_LightSleep"] = 2] = "CPU_LightSleep";
  PowerMon_State$1[PowerMon_State$1["Vext1_On"] = 4] = "Vext1_On";
  PowerMon_State$1[PowerMon_State$1["Lora_RXOn"] = 8] = "Lora_RXOn";
  PowerMon_State$1[PowerMon_State$1["Lora_TXOn"] = 16] = "Lora_TXOn";
  PowerMon_State$1[PowerMon_State$1["Lora_RXActive"] = 32] = "Lora_RXActive";
  PowerMon_State$1[PowerMon_State$1["BT_On"] = 64] = "BT_On";
  PowerMon_State$1[PowerMon_State$1["LED_On"] = 128] = "LED_On";
  PowerMon_State$1[PowerMon_State$1["Screen_On"] = 256] = "Screen_On";
  PowerMon_State$1[PowerMon_State$1["Screen_Drawing"] = 512] = "Screen_Drawing";
  PowerMon_State$1[PowerMon_State$1["Wifi_On"] = 1024] = "Wifi_On";
  PowerMon_State$1[PowerMon_State$1["GPS_Active"] = 2048] = "GPS_Active";
  return PowerMon_State$1;
}({});
var PowerMon_StateSchema = /* @__PURE__ */ enumDesc(file_powermon, 0, 0);
var PowerStressMessageSchema = /* @__PURE__ */ messageDesc(file_powermon, 1);
var PowerStressMessage_Opcode = /* @__PURE__ */ function(PowerStressMessage_Opcode$1) {
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["UNSET"] = 0] = "UNSET";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["PRINT_INFO"] = 1] = "PRINT_INFO";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["FORCE_QUIET"] = 2] = "FORCE_QUIET";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["END_QUIET"] = 3] = "END_QUIET";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["SCREEN_ON"] = 16] = "SCREEN_ON";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["SCREEN_OFF"] = 17] = "SCREEN_OFF";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["CPU_IDLE"] = 32] = "CPU_IDLE";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["CPU_DEEPSLEEP"] = 33] = "CPU_DEEPSLEEP";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["CPU_FULLON"] = 34] = "CPU_FULLON";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["LED_ON"] = 48] = "LED_ON";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["LED_OFF"] = 49] = "LED_OFF";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["LORA_OFF"] = 64] = "LORA_OFF";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["LORA_TX"] = 65] = "LORA_TX";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["LORA_RX"] = 66] = "LORA_RX";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["BT_OFF"] = 80] = "BT_OFF";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["BT_ON"] = 81] = "BT_ON";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["WIFI_OFF"] = 96] = "WIFI_OFF";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["WIFI_ON"] = 97] = "WIFI_ON";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["GPS_OFF"] = 112] = "GPS_OFF";
  PowerStressMessage_Opcode$1[PowerStressMessage_Opcode$1["GPS_ON"] = 113] = "GPS_ON";
  return PowerStressMessage_Opcode$1;
}({});
var PowerStressMessage_OpcodeSchema = /* @__PURE__ */ enumDesc(file_powermon, 1, 0);
var remote_hardware_pb_exports = __export2({
  HardwareMessageSchema: () => HardwareMessageSchema,
  HardwareMessage_Type: () => HardwareMessage_Type,
  HardwareMessage_TypeSchema: () => HardwareMessage_TypeSchema,
  file_remote_hardware: () => file_remote_hardware
});
var file_remote_hardware = /* @__PURE__ */ fileDesc("ChVyZW1vdGVfaGFyZHdhcmUucHJvdG8SCm1lc2h0YXN0aWMi1gEKD0hhcmR3YXJlTWVzc2FnZRIuCgR0eXBlGAEgASgOMiAubWVzaHRhc3RpYy5IYXJkd2FyZU1lc3NhZ2UuVHlwZRIRCglncGlvX21hc2sYAiABKAQSEgoKZ3Bpb192YWx1ZRgDIAEoBCJsCgRUeXBlEgkKBVVOU0VUEAASDwoLV1JJVEVfR1BJT1MQARIPCgtXQVRDSF9HUElPUxACEhEKDUdQSU9TX0NIQU5HRUQQAxIOCgpSRUFEX0dQSU9TEAQSFAoQUkVBRF9HUElPU19SRVBMWRAFQmMKE2NvbS5nZWVrc3ZpbGxlLm1lc2hCDlJlbW90ZUhhcmR3YXJlWiJnaXRodWIuY29tL21lc2h0YXN0aWMvZ28vZ2VuZXJhdGVkqgIUTWVzaHRhc3RpYy5Qcm90b2J1ZnO6AgBiBnByb3RvMw");
var HardwareMessageSchema = /* @__PURE__ */ messageDesc(file_remote_hardware, 0);
var HardwareMessage_Type = /* @__PURE__ */ function(HardwareMessage_Type$1) {
  HardwareMessage_Type$1[HardwareMessage_Type$1["UNSET"] = 0] = "UNSET";
  HardwareMessage_Type$1[HardwareMessage_Type$1["WRITE_GPIOS"] = 1] = "WRITE_GPIOS";
  HardwareMessage_Type$1[HardwareMessage_Type$1["WATCH_GPIOS"] = 2] = "WATCH_GPIOS";
  HardwareMessage_Type$1[HardwareMessage_Type$1["GPIOS_CHANGED"] = 3] = "GPIOS_CHANGED";
  HardwareMessage_Type$1[HardwareMessage_Type$1["READ_GPIOS"] = 4] = "READ_GPIOS";
  HardwareMessage_Type$1[HardwareMessage_Type$1["READ_GPIOS_REPLY"] = 5] = "READ_GPIOS_REPLY";
  return HardwareMessage_Type$1;
}({});
var HardwareMessage_TypeSchema = /* @__PURE__ */ enumDesc(file_remote_hardware, 0, 0);
var rtttl_pb_exports = __export2({
  RTTTLConfigSchema: () => RTTTLConfigSchema,
  file_rtttl: () => file_rtttl
});
var file_rtttl = /* @__PURE__ */ fileDesc("CgtydHR0bC5wcm90bxIKbWVzaHRhc3RpYyIfCgtSVFRUTENvbmZpZxIQCghyaW5ndG9uZRgBIAEoCUJmChNjb20uZ2Vla3N2aWxsZS5tZXNoQhFSVFRUTENvbmZpZ1Byb3Rvc1oiZ2l0aHViLmNvbS9tZXNodGFzdGljL2dvL2dlbmVyYXRlZKoCFE1lc2h0YXN0aWMuUHJvdG9idWZzugIAYgZwcm90bzM");
var RTTTLConfigSchema = /* @__PURE__ */ messageDesc(file_rtttl, 0);
var storeforward_pb_exports = __export2({
  StoreAndForwardSchema: () => StoreAndForwardSchema,
  StoreAndForward_HeartbeatSchema: () => StoreAndForward_HeartbeatSchema,
  StoreAndForward_HistorySchema: () => StoreAndForward_HistorySchema,
  StoreAndForward_RequestResponse: () => StoreAndForward_RequestResponse,
  StoreAndForward_RequestResponseSchema: () => StoreAndForward_RequestResponseSchema,
  StoreAndForward_StatisticsSchema: () => StoreAndForward_StatisticsSchema,
  file_storeforward: () => file_storeforward
});
var file_storeforward = /* @__PURE__ */ fileDesc("ChJzdG9yZWZvcndhcmQucHJvdG8SCm1lc2h0YXN0aWMinAcKD1N0b3JlQW5kRm9yd2FyZBI3CgJychgBIAEoDjIrLm1lc2h0YXN0aWMuU3RvcmVBbmRGb3J3YXJkLlJlcXVlc3RSZXNwb25zZRI3CgVzdGF0cxgCIAEoCzImLm1lc2h0YXN0aWMuU3RvcmVBbmRGb3J3YXJkLlN0YXRpc3RpY3NIABI2CgdoaXN0b3J5GAMgASgLMiMubWVzaHRhc3RpYy5TdG9yZUFuZEZvcndhcmQuSGlzdG9yeUgAEjoKCWhlYXJ0YmVhdBgEIAEoCzIlLm1lc2h0YXN0aWMuU3RvcmVBbmRGb3J3YXJkLkhlYXJ0YmVhdEgAEg4KBHRleHQYBSABKAxIABrNAQoKU3RhdGlzdGljcxIWCg5tZXNzYWdlc190b3RhbBgBIAEoDRIWCg5tZXNzYWdlc19zYXZlZBgCIAEoDRIUCgxtZXNzYWdlc19tYXgYAyABKA0SDwoHdXBfdGltZRgEIAEoDRIQCghyZXF1ZXN0cxgFIAEoDRIYChByZXF1ZXN0c19oaXN0b3J5GAYgASgNEhEKCWhlYXJ0YmVhdBgHIAEoCBISCgpyZXR1cm5fbWF4GAggASgNEhUKDXJldHVybl93aW5kb3cYCSABKA0aSQoHSGlzdG9yeRIYChBoaXN0b3J5X21lc3NhZ2VzGAEgASgNEg4KBndpbmRvdxgCIAEoDRIUCgxsYXN0X3JlcXVlc3QYAyABKA0aLgoJSGVhcnRiZWF0Eg4KBnBlcmlvZBgBIAEoDRIRCglzZWNvbmRhcnkYAiABKA0ivAIKD1JlcXVlc3RSZXNwb25zZRIJCgVVTlNFVBAAEhAKDFJPVVRFUl9FUlJPUhABEhQKEFJPVVRFUl9IRUFSVEJFQVQQAhIPCgtST1VURVJfUElORxADEg8KC1JPVVRFUl9QT05HEAQSDwoLUk9VVEVSX0JVU1kQBRISCg5ST1VURVJfSElTVE9SWRAGEhAKDFJPVVRFUl9TVEFUUxAHEhYKElJPVVRFUl9URVhUX0RJUkVDVBAIEhkKFVJPVVRFUl9URVhUX0JST0FEQ0FTVBAJEhAKDENMSUVOVF9FUlJPUhBAEhIKDkNMSUVOVF9ISVNUT1JZEEESEAoMQ0xJRU5UX1NUQVRTEEISDwoLQ0xJRU5UX1BJTkcQQxIPCgtDTElFTlRfUE9ORxBEEhAKDENMSUVOVF9BQk9SVBBqQgkKB3ZhcmlhbnRCagoTY29tLmdlZWtzdmlsbGUubWVzaEIVU3RvcmVBbmRGb3J3YXJkUHJvdG9zWiJnaXRodWIuY29tL21lc2h0YXN0aWMvZ28vZ2VuZXJhdGVkqgIUTWVzaHRhc3RpYy5Qcm90b2J1ZnO6AgBiBnByb3RvMw");
var StoreAndForwardSchema = /* @__PURE__ */ messageDesc(file_storeforward, 0);
var StoreAndForward_StatisticsSchema = /* @__PURE__ */ messageDesc(file_storeforward, 0, 0);
var StoreAndForward_HistorySchema = /* @__PURE__ */ messageDesc(file_storeforward, 0, 1);
var StoreAndForward_HeartbeatSchema = /* @__PURE__ */ messageDesc(file_storeforward, 0, 2);
var StoreAndForward_RequestResponse = /* @__PURE__ */ function(StoreAndForward_RequestResponse$1) {
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["UNSET"] = 0] = "UNSET";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["ROUTER_ERROR"] = 1] = "ROUTER_ERROR";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["ROUTER_HEARTBEAT"] = 2] = "ROUTER_HEARTBEAT";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["ROUTER_PING"] = 3] = "ROUTER_PING";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["ROUTER_PONG"] = 4] = "ROUTER_PONG";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["ROUTER_BUSY"] = 5] = "ROUTER_BUSY";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["ROUTER_HISTORY"] = 6] = "ROUTER_HISTORY";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["ROUTER_STATS"] = 7] = "ROUTER_STATS";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["ROUTER_TEXT_DIRECT"] = 8] = "ROUTER_TEXT_DIRECT";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["ROUTER_TEXT_BROADCAST"] = 9] = "ROUTER_TEXT_BROADCAST";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["CLIENT_ERROR"] = 64] = "CLIENT_ERROR";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["CLIENT_HISTORY"] = 65] = "CLIENT_HISTORY";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["CLIENT_STATS"] = 66] = "CLIENT_STATS";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["CLIENT_PING"] = 67] = "CLIENT_PING";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["CLIENT_PONG"] = 68] = "CLIENT_PONG";
  StoreAndForward_RequestResponse$1[StoreAndForward_RequestResponse$1["CLIENT_ABORT"] = 106] = "CLIENT_ABORT";
  return StoreAndForward_RequestResponse$1;
}({});
var StoreAndForward_RequestResponseSchema = /* @__PURE__ */ enumDesc(file_storeforward, 0, 0);
var meta = {
  runtime: "Nodejs",
  runtimeVersion: process?.version,
  hostname: import_os.hostname ? (0, import_os.hostname)() : void 0
};
var types_exports = __export({
  ChannelNumber: () => ChannelNumber,
  DeviceStatusEnum: () => DeviceStatusEnum,
  Emitter: () => Emitter,
  EmitterScope: () => EmitterScope
});
var DeviceStatusEnum = /* @__PURE__ */ function(DeviceStatusEnum$1) {
  DeviceStatusEnum$1[DeviceStatusEnum$1["DeviceRestarting"] = 1] = "DeviceRestarting";
  DeviceStatusEnum$1[DeviceStatusEnum$1["DeviceDisconnected"] = 2] = "DeviceDisconnected";
  DeviceStatusEnum$1[DeviceStatusEnum$1["DeviceConnecting"] = 3] = "DeviceConnecting";
  DeviceStatusEnum$1[DeviceStatusEnum$1["DeviceReconnecting"] = 4] = "DeviceReconnecting";
  DeviceStatusEnum$1[DeviceStatusEnum$1["DeviceConnected"] = 5] = "DeviceConnected";
  DeviceStatusEnum$1[DeviceStatusEnum$1["DeviceConfiguring"] = 6] = "DeviceConfiguring";
  DeviceStatusEnum$1[DeviceStatusEnum$1["DeviceConfigured"] = 7] = "DeviceConfigured";
  return DeviceStatusEnum$1;
}({});
var EmitterScope = /* @__PURE__ */ function(EmitterScope$1) {
  EmitterScope$1[EmitterScope$1["MeshDevice"] = 1] = "MeshDevice";
  EmitterScope$1[EmitterScope$1["SerialConnection"] = 2] = "SerialConnection";
  EmitterScope$1[EmitterScope$1["NodeSerialConnection"] = 3] = "NodeSerialConnection";
  EmitterScope$1[EmitterScope$1["BleConnection"] = 4] = "BleConnection";
  EmitterScope$1[EmitterScope$1["HttpConnection"] = 5] = "HttpConnection";
  return EmitterScope$1;
}({});
var Emitter = /* @__PURE__ */ function(Emitter$1) {
  Emitter$1[Emitter$1["Constructor"] = 0] = "Constructor";
  Emitter$1[Emitter$1["SendText"] = 1] = "SendText";
  Emitter$1[Emitter$1["SendWaypoint"] = 2] = "SendWaypoint";
  Emitter$1[Emitter$1["SendPacket"] = 3] = "SendPacket";
  Emitter$1[Emitter$1["SendRaw"] = 4] = "SendRaw";
  Emitter$1[Emitter$1["SetConfig"] = 5] = "SetConfig";
  Emitter$1[Emitter$1["SetModuleConfig"] = 6] = "SetModuleConfig";
  Emitter$1[Emitter$1["ConfirmSetConfig"] = 7] = "ConfirmSetConfig";
  Emitter$1[Emitter$1["SetOwner"] = 8] = "SetOwner";
  Emitter$1[Emitter$1["SetChannel"] = 9] = "SetChannel";
  Emitter$1[Emitter$1["ConfirmSetChannel"] = 10] = "ConfirmSetChannel";
  Emitter$1[Emitter$1["ClearChannel"] = 11] = "ClearChannel";
  Emitter$1[Emitter$1["GetChannel"] = 12] = "GetChannel";
  Emitter$1[Emitter$1["GetAllChannels"] = 13] = "GetAllChannels";
  Emitter$1[Emitter$1["GetConfig"] = 14] = "GetConfig";
  Emitter$1[Emitter$1["GetModuleConfig"] = 15] = "GetModuleConfig";
  Emitter$1[Emitter$1["GetOwner"] = 16] = "GetOwner";
  Emitter$1[Emitter$1["Configure"] = 17] = "Configure";
  Emitter$1[Emitter$1["HandleFromRadio"] = 18] = "HandleFromRadio";
  Emitter$1[Emitter$1["HandleMeshPacket"] = 19] = "HandleMeshPacket";
  Emitter$1[Emitter$1["Connect"] = 20] = "Connect";
  Emitter$1[Emitter$1["Ping"] = 21] = "Ping";
  Emitter$1[Emitter$1["ReadFromRadio"] = 22] = "ReadFromRadio";
  Emitter$1[Emitter$1["WriteToRadio"] = 23] = "WriteToRadio";
  Emitter$1[Emitter$1["SetDebugMode"] = 24] = "SetDebugMode";
  Emitter$1[Emitter$1["GetMetadata"] = 25] = "GetMetadata";
  Emitter$1[Emitter$1["ResetNodes"] = 26] = "ResetNodes";
  Emitter$1[Emitter$1["Shutdown"] = 27] = "Shutdown";
  Emitter$1[Emitter$1["Reboot"] = 28] = "Reboot";
  Emitter$1[Emitter$1["RebootOta"] = 29] = "RebootOta";
  Emitter$1[Emitter$1["FactoryReset"] = 30] = "FactoryReset";
  Emitter$1[Emitter$1["EnterDfuMode"] = 31] = "EnterDfuMode";
  Emitter$1[Emitter$1["RemoveNodeByNum"] = 32] = "RemoveNodeByNum";
  Emitter$1[Emitter$1["SetCannedMessages"] = 33] = "SetCannedMessages";
  Emitter$1[Emitter$1["Disconnect"] = 34] = "Disconnect";
  Emitter$1[Emitter$1["ConnectionStatus"] = 35] = "ConnectionStatus";
  return Emitter$1;
}({});
var ChannelNumber = /* @__PURE__ */ function(ChannelNumber$1) {
  ChannelNumber$1[ChannelNumber$1["Primary"] = 0] = "Primary";
  ChannelNumber$1[ChannelNumber$1["Channel1"] = 1] = "Channel1";
  ChannelNumber$1[ChannelNumber$1["Channel2"] = 2] = "Channel2";
  ChannelNumber$1[ChannelNumber$1["Channel3"] = 3] = "Channel3";
  ChannelNumber$1[ChannelNumber$1["Channel4"] = 4] = "Channel4";
  ChannelNumber$1[ChannelNumber$1["Channel5"] = 5] = "Channel5";
  ChannelNumber$1[ChannelNumber$1["Channel6"] = 6] = "Channel6";
  ChannelNumber$1[ChannelNumber$1["Admin"] = 7] = "Admin";
  return ChannelNumber$1;
}({});
var require_DispatcherWrapper = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-core@3.0.11/node_modules/ste-core/dist/dispatching/DispatcherWrapper.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  var DispatcherWrapper = class {
    /**
    * Creates an instance of DispatcherWrapper.
    * @param {ISubscribable<TEventHandler>} dispatcher
    *
    * @memberOf DispatcherWrapper
    */
    constructor(dispatcher) {
      this._subscribe = (fn) => dispatcher.subscribe(fn);
      this._unsubscribe = (fn) => dispatcher.unsubscribe(fn);
      this._one = (fn) => dispatcher.one(fn);
      this._has = (fn) => dispatcher.has(fn);
      this._clear = () => dispatcher.clear();
      this._count = () => dispatcher.count;
      this._onSubscriptionChange = () => dispatcher.onSubscriptionChange;
    }
    /**
    * Triggered when subscriptions are changed (added or removed).
    *
    * @readonly
    * @type {ISubscribable<SubscriptionChangeEventHandler>}
    * @memberOf DispatcherWrapper
    */
    get onSubscriptionChange() {
      return this._onSubscriptionChange();
    }
    /**
    * Returns the number of subscriptions.
    *
    * @readonly
    * @type {number}
    * @memberOf DispatcherWrapper
    */
    get count() {
      return this._count();
    }
    /**
    * Subscribe to the event dispatcher.
    *
    * @param {TEventHandler} fn The event handler that is called when the event is dispatched.
    * @returns {() => void} A function that unsubscribes the event handler from the event.
    *
    * @memberOf DispatcherWrapper
    */
    subscribe(fn) {
      return this._subscribe(fn);
    }
    /**
    * Subscribe to the event dispatcher.
    *
    * @param {TEventHandler} fn The event handler that is called when the event is dispatched.
    * @returns {() => void} A function that unsubscribes the event handler from the event.
    *
    * @memberOf DispatcherWrapper
    */
    sub(fn) {
      return this.subscribe(fn);
    }
    /**
    * Unsubscribe from the event dispatcher.
    *
    * @param {TEventHandler} fn The event handler that is called when the event is dispatched.
    *
    * @memberOf DispatcherWrapper
    */
    unsubscribe(fn) {
      this._unsubscribe(fn);
    }
    /**
    * Unsubscribe from the event dispatcher.
    *
    * @param {TEventHandler} fn The event handler that is called when the event is dispatched.
    *
    * @memberOf DispatcherWrapper
    */
    unsub(fn) {
      this.unsubscribe(fn);
    }
    /**
    * Subscribe once to the event with the specified name.
    *
    * @returns {() => void} A function that unsubscribes the event handler from the event.
    *
    * @memberOf DispatcherWrapper
    */
    one(fn) {
      return this._one(fn);
    }
    /**
    * Checks it the event has a subscription for the specified handler.
    *
    * @param {TEventHandler} fn The event handler that is called when the event is dispatched.
    *
    * @memberOf DispatcherWrapper
    */
    has(fn) {
      return this._has(fn);
    }
    /**
    * Clears all the subscriptions.
    *
    * @memberOf DispatcherWrapper
    */
    clear() {
      this._clear();
    }
  };
  exports2.DispatcherWrapper = DispatcherWrapper;
} });
var require_Subscription = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-core@3.0.11/node_modules/ste-core/dist/events/Subscription.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  var Subscription = class {
    /**
    * Creates an instance of Subscription.
    *
    * @param {TEventHandler} handler The handler for the subscription.
    * @param {boolean} isOnce Indicates if the handler should only be executed once.
    */
    constructor(handler, isOnce) {
      this.handler = handler;
      this.isOnce = isOnce;
      this.isExecuted = false;
    }
    /**
    * Executes the handler.
    *
    * @param {boolean} executeAsync True if the even should be executed async.
    * @param {*} scope The scope the scope of the event.
    * @param {IArguments} args The arguments for the event.
    */
    execute(executeAsync, scope, args) {
      if (!this.isOnce || !this.isExecuted) {
        this.isExecuted = true;
        var fn = this.handler;
        if (executeAsync)
          setTimeout(() => {
            fn.apply(scope, args);
          }, 1);
        else
          fn.apply(scope, args);
      }
    }
  };
  exports2.Subscription = Subscription;
} });
var require_EventManagement = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-core@3.0.11/node_modules/ste-core/dist/management/EventManagement.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  var EventManagement = class {
    /**
    * Creates an instance of EventManagement.
    * @param {() => void} unsub An unsubscribe handler.
    *
    * @memberOf EventManagement
    */
    constructor(unsub) {
      this.unsub = unsub;
      this.propagationStopped = false;
    }
    /**
    * Stops the propagation of the event.
    * Cannot be used when async dispatch is done.
    *
    * @memberOf EventManagement
    */
    stopPropagation() {
      this.propagationStopped = true;
    }
  };
  exports2.EventManagement = EventManagement;
} });
var require_DispatcherBase = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-core@3.0.11/node_modules/ste-core/dist/dispatching/DispatcherBase.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  const DispatcherWrapper_1$1 = require_DispatcherWrapper();
  const Subscription_1$1 = require_Subscription();
  const EventManagement_1$2 = require_EventManagement();
  var DispatcherBase = class {
    constructor() {
      this._subscriptions = new Array();
    }
    /**
    * Returns the number of subscriptions.
    *
    * @readonly
    * @type {number}
    * @memberOf DispatcherBase
    */
    get count() {
      return this._subscriptions.length;
    }
    /**
    * Triggered when subscriptions are changed (added or removed).
    *
    * @readonly
    * @type {ISubscribable<SubscriptionChangeEventHandler>}
    * @memberOf DispatcherBase
    */
    get onSubscriptionChange() {
      if (this._onSubscriptionChange == null)
        this._onSubscriptionChange = new SubscriptionChangeEventDispatcher();
      return this._onSubscriptionChange.asEvent();
    }
    /**
    * Subscribe to the event dispatcher.
    *
    * @param {TEventHandler} fn The event handler that is called when the event is dispatched.
    * @returns A function that unsubscribes the event handler from the event.
    *
    * @memberOf DispatcherBase
    */
    subscribe(fn) {
      if (fn) {
        this._subscriptions.push(this.createSubscription(fn, false));
        this.triggerSubscriptionChange();
      }
      return () => {
        this.unsubscribe(fn);
      };
    }
    /**
    * Subscribe to the event dispatcher.
    *
    * @param {TEventHandler} fn The event handler that is called when the event is dispatched.
    * @returns A function that unsubscribes the event handler from the event.
    *
    * @memberOf DispatcherBase
    */
    sub(fn) {
      return this.subscribe(fn);
    }
    /**
    * Subscribe once to the event with the specified name.
    *
    * @param {TEventHandler} fn The event handler that is called when the event is dispatched.
    * @returns A function that unsubscribes the event handler from the event.
    *
    * @memberOf DispatcherBase
    */
    one(fn) {
      if (fn) {
        this._subscriptions.push(this.createSubscription(fn, true));
        this.triggerSubscriptionChange();
      }
      return () => {
        this.unsubscribe(fn);
      };
    }
    /**
    * Checks it the event has a subscription for the specified handler.
    *
    * @param {TEventHandler} fn The event handler.
    *
    * @memberOf DispatcherBase
    */
    has(fn) {
      if (!fn)
        return false;
      return this._subscriptions.some((sub) => sub.handler == fn);
    }
    /**
    * Unsubscribes the handler from the dispatcher.
    *
    * @param {TEventHandler} fn The event handler.
    *
    * @memberOf DispatcherBase
    */
    unsubscribe(fn) {
      if (!fn)
        return;
      let changes = false;
      for (let i = 0; i < this._subscriptions.length; i++)
        if (this._subscriptions[i].handler == fn) {
          this._subscriptions.splice(i, 1);
          changes = true;
          break;
        }
      if (changes)
        this.triggerSubscriptionChange();
    }
    /**
    * Unsubscribes the handler from the dispatcher.
    *
    * @param {TEventHandler} fn The event handler.
    *
    * @memberOf DispatcherBase
    */
    unsub(fn) {
      this.unsubscribe(fn);
    }
    /**
    * Generic dispatch will dispatch the handlers with the given arguments.
    *
    * @protected
    * @param {boolean} executeAsync `True` if the even should be executed async.
    * @param {*} scope The scope of the event. The scope becomes the `this` for handler.
    * @param {IArguments} args The arguments for the event.
    * @returns {(IPropagationStatus | null)} The propagation status, or if an `executeAsync` is used `null`.
    *
    * @memberOf DispatcherBase
    */
    _dispatch(executeAsync, scope, args) {
      for (let sub of [...this._subscriptions]) {
        let ev = new EventManagement_1$2.EventManagement(() => this.unsub(sub.handler));
        let nargs = Array.prototype.slice.call(args);
        nargs.push(ev);
        sub.execute(executeAsync, scope, nargs);
        this.cleanup(sub);
        if (!executeAsync && ev.propagationStopped)
          return { propagationStopped: true };
      }
      if (executeAsync)
        return null;
      return { propagationStopped: false };
    }
    /**
    * Creates a subscription.
    *
    * @protected
    * @param {TEventHandler} handler The handler.
    * @param {boolean} isOnce True if the handler should run only one.
    * @returns {ISubscription<TEventHandler>} The subscription.
    *
    * @memberOf DispatcherBase
    */
    createSubscription(handler, isOnce) {
      return new Subscription_1$1.Subscription(handler, isOnce);
    }
    /**
    * Cleans up subs that ran and should run only once.
    *
    * @protected
    * @param {ISubscription<TEventHandler>} sub The subscription.
    *
    * @memberOf DispatcherBase
    */
    cleanup(sub) {
      let changes = false;
      if (sub.isOnce && sub.isExecuted) {
        let i = this._subscriptions.indexOf(sub);
        if (i > -1) {
          this._subscriptions.splice(i, 1);
          changes = true;
        }
      }
      if (changes)
        this.triggerSubscriptionChange();
    }
    /**
    * Creates an event from the dispatcher. Will return the dispatcher
    * in a wrapper. This will prevent exposure of any dispatcher methods.
    *
    * @returns {ISubscribable<TEventHandler>}
    *
    * @memberOf DispatcherBase
    */
    asEvent() {
      if (this._wrap == null)
        this._wrap = new DispatcherWrapper_1$1.DispatcherWrapper(this);
      return this._wrap;
    }
    /**
    * Clears the subscriptions.
    *
    * @memberOf DispatcherBase
    */
    clear() {
      if (this._subscriptions.length != 0) {
        this._subscriptions.splice(0, this._subscriptions.length);
        this.triggerSubscriptionChange();
      }
    }
    /**
    * Triggers the subscription change event.
    *
    * @private
    *
    * @memberOf DispatcherBase
    */
    triggerSubscriptionChange() {
      if (this._onSubscriptionChange != null)
        this._onSubscriptionChange.dispatch(this.count);
    }
  };
  exports2.DispatcherBase = DispatcherBase;
  var SubscriptionChangeEventDispatcher = class extends DispatcherBase {
    /**
    * Dispatches the event.
    *
    * @param {number} count The currrent number of subscriptions.
    *
    * @memberOf SubscriptionChangeEventDispatcher
    */
    dispatch(count) {
      this._dispatch(false, this, arguments);
    }
  };
  exports2.SubscriptionChangeEventDispatcher = SubscriptionChangeEventDispatcher;
} });
var require_DispatchError = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-core@3.0.11/node_modules/ste-core/dist/dispatching/DispatchError.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  var DispatchError = class extends Error {
    /**
    * Creates an instance of DispatchError.
    * @param {string} message The message.
    *
    * @memberOf DispatchError
    */
    constructor(message) {
      super(message);
    }
  };
  exports2.DispatchError = DispatchError;
} });
var require_EventListBase = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-core@3.0.11/node_modules/ste-core/dist/dispatching/EventListBase.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  var EventListBase = class {
    constructor() {
      this._events = {};
    }
    /**
    * Gets the dispatcher associated with the name.
    *
    * @param {string} name The name of the event.
    * @returns {TEventDispatcher} The disptacher.
    *
    * @memberOf EventListBase
    */
    get(name) {
      let event = this._events[name];
      if (event)
        return event;
      event = this.createDispatcher();
      this._events[name] = event;
      return event;
    }
    /**
    * Removes the dispatcher associated with the name.
    *
    * @param {string} name
    *
    * @memberOf EventListBase
    */
    remove(name) {
      delete this._events[name];
    }
  };
  exports2.EventListBase = EventListBase;
} });
var require_HandlingBase = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-core@3.0.11/node_modules/ste-core/dist/handling/HandlingBase.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  var HandlingBase = class {
    /**
    * Creates an instance of HandlingBase.
    * @param {TList} events The event list. Used for event management.
    *
    * @memberOf HandlingBase
    */
    constructor(events) {
      this.events = events;
    }
    /**
    * Subscribes once to the event with the specified name.
    * @param {string} name The name of the event.
    * @param {TEventHandler} fn The event handler.
    *
    * @memberOf HandlingBase
    */
    one(name, fn) {
      this.events.get(name).one(fn);
    }
    /**
    * Checks it the event has a subscription for the specified handler.
    * @param {string} name The name of the event.
    * @param {TEventHandler} fn The event handler.
    *
    * @memberOf HandlingBase
    */
    has(name, fn) {
      return this.events.get(name).has(fn);
    }
    /**
    * Subscribes to the event with the specified name.
    * @param {string} name The name of the event.
    * @param {TEventHandler} fn The event handler.
    *
    * @memberOf HandlingBase
    */
    subscribe(name, fn) {
      this.events.get(name).subscribe(fn);
    }
    /**
    * Subscribes to the event with the specified name.
    * @param {string} name The name of the event.
    * @param {TEventHandler} fn The event handler.
    *
    * @memberOf HandlingBase
    */
    sub(name, fn) {
      this.subscribe(name, fn);
    }
    /**
    * Unsubscribes from the event with the specified name.
    * @param {string} name The name of the event.
    * @param {TEventHandler} fn The event handler.
    *
    * @memberOf HandlingBase
    */
    unsubscribe(name, fn) {
      this.events.get(name).unsubscribe(fn);
    }
    /**
    * Unsubscribes from the event with the specified name.
    * @param {string} name The name of the event.
    * @param {TEventHandler} fn The event handler.
    *
    * @memberOf HandlingBase
    */
    unsub(name, fn) {
      this.unsubscribe(name, fn);
    }
  };
  exports2.HandlingBase = HandlingBase;
} });
var require_PromiseSubscription = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-core@3.0.11/node_modules/ste-core/dist/events/PromiseSubscription.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  var PromiseSubscription = class {
    /**
    * Creates an instance of PromiseSubscription.
    * @param {TEventHandler} handler The handler for the subscription.
    * @param {boolean} isOnce Indicates if the handler should only be executed once.
    *
    * @memberOf PromiseSubscription
    */
    constructor(handler, isOnce) {
      this.handler = handler;
      this.isOnce = isOnce;
      this.isExecuted = false;
    }
    /**
    * Executes the handler.
    *
    * @param {boolean} executeAsync True if the even should be executed async.
    * @param {*} scope The scope the scope of the event.
    * @param {IArguments} args The arguments for the event.
    *
    * @memberOf PromiseSubscription
    */
    async execute(executeAsync, scope, args) {
      if (!this.isOnce || !this.isExecuted) {
        this.isExecuted = true;
        var fn = this.handler;
        if (executeAsync) {
          setTimeout(() => {
            fn.apply(scope, args);
          }, 1);
          return;
        }
        await fn.apply(scope, args);
      }
    }
  };
  exports2.PromiseSubscription = PromiseSubscription;
} });
var require_PromiseDispatcherBase = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-core@3.0.11/node_modules/ste-core/dist/dispatching/PromiseDispatcherBase.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  const PromiseSubscription_1$1 = require_PromiseSubscription();
  const EventManagement_1$1 = require_EventManagement();
  const DispatcherBase_1$1 = require_DispatcherBase();
  const DispatchError_1$1 = require_DispatchError();
  var PromiseDispatcherBase = class extends DispatcherBase_1$1.DispatcherBase {
    /**
    * The normal dispatch cannot be used in this class.
    *
    * @protected
    * @param {boolean} executeAsync `True` if the even should be executed async.
    * @param {*} scope The scope of the event. The scope becomes the `this` for handler.
    * @param {IArguments} args The arguments for the event.
    * @returns {(IPropagationStatus | null)} The propagation status, or if an `executeAsync` is used `null`.
    *
    * @memberOf DispatcherBase
    */
    _dispatch(executeAsync, scope, args) {
      throw new DispatchError_1$1.DispatchError("_dispatch not supported. Use _dispatchAsPromise.");
    }
    /**
    * Crates a new subscription.
    *
    * @protected
    * @param {TEventHandler} handler The handler.
    * @param {boolean} isOnce Indicates if the handler should only run once.
    * @returns {ISubscription<TEventHandler>} The subscription.
    *
    * @memberOf PromiseDispatcherBase
    */
    createSubscription(handler, isOnce) {
      return new PromiseSubscription_1$1.PromiseSubscription(handler, isOnce);
    }
    /**
    * Generic dispatch will dispatch the handlers with the given arguments.
    *
    * @protected
    * @param {boolean} executeAsync `True` if the even should be executed async.
    * @param {*} scope The scope of the event. The scope becomes the `this` for handler.
    * @param {IArguments} args The arguments for the event.
    * @returns {(IPropagationStatus | null)} The propagation status, or if an `executeAsync` is used `null`.
    *
    * @memberOf DispatcherBase
    */
    async _dispatchAsPromise(executeAsync, scope, args) {
      for (let sub of [...this._subscriptions]) {
        let ev = new EventManagement_1$1.EventManagement(() => this.unsub(sub.handler));
        let nargs = Array.prototype.slice.call(args);
        nargs.push(ev);
        await sub.execute(executeAsync, scope, nargs);
        this.cleanup(sub);
        if (!executeAsync && ev.propagationStopped)
          return { propagationStopped: true };
      }
      if (executeAsync)
        return null;
      return { propagationStopped: false };
    }
  };
  exports2.PromiseDispatcherBase = PromiseDispatcherBase;
} });
var require_dist$1 = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-core@3.0.11/node_modules/ste-core/dist/index.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  exports2.SubscriptionChangeEventDispatcher = exports2.HandlingBase = exports2.PromiseDispatcherBase = exports2.PromiseSubscription = exports2.DispatchError = exports2.EventManagement = exports2.EventListBase = exports2.DispatcherWrapper = exports2.DispatcherBase = exports2.Subscription = void 0;
  const DispatcherBase_1 = require_DispatcherBase();
  Object.defineProperty(exports2, "DispatcherBase", {
    enumerable: true,
    get: function() {
      return DispatcherBase_1.DispatcherBase;
    }
  });
  Object.defineProperty(exports2, "SubscriptionChangeEventDispatcher", {
    enumerable: true,
    get: function() {
      return DispatcherBase_1.SubscriptionChangeEventDispatcher;
    }
  });
  const DispatchError_1 = require_DispatchError();
  Object.defineProperty(exports2, "DispatchError", {
    enumerable: true,
    get: function() {
      return DispatchError_1.DispatchError;
    }
  });
  const DispatcherWrapper_1 = require_DispatcherWrapper();
  Object.defineProperty(exports2, "DispatcherWrapper", {
    enumerable: true,
    get: function() {
      return DispatcherWrapper_1.DispatcherWrapper;
    }
  });
  const EventListBase_1 = require_EventListBase();
  Object.defineProperty(exports2, "EventListBase", {
    enumerable: true,
    get: function() {
      return EventListBase_1.EventListBase;
    }
  });
  const EventManagement_1 = require_EventManagement();
  Object.defineProperty(exports2, "EventManagement", {
    enumerable: true,
    get: function() {
      return EventManagement_1.EventManagement;
    }
  });
  const HandlingBase_1 = require_HandlingBase();
  Object.defineProperty(exports2, "HandlingBase", {
    enumerable: true,
    get: function() {
      return HandlingBase_1.HandlingBase;
    }
  });
  const PromiseDispatcherBase_1 = require_PromiseDispatcherBase();
  Object.defineProperty(exports2, "PromiseDispatcherBase", {
    enumerable: true,
    get: function() {
      return PromiseDispatcherBase_1.PromiseDispatcherBase;
    }
  });
  const PromiseSubscription_1 = require_PromiseSubscription();
  Object.defineProperty(exports2, "PromiseSubscription", {
    enumerable: true,
    get: function() {
      return PromiseSubscription_1.PromiseSubscription;
    }
  });
  const Subscription_1 = require_Subscription();
  Object.defineProperty(exports2, "Subscription", {
    enumerable: true,
    get: function() {
      return Subscription_1.Subscription;
    }
  });
} });
var require_SimpleEventDispatcher = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-simple-events@3.0.11/node_modules/ste-simple-events/dist/SimpleEventDispatcher.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  const ste_core_1$2 = require_dist$1();
  var SimpleEventDispatcher$2 = class extends ste_core_1$2.DispatcherBase {
    /**
    * Creates an instance of SimpleEventDispatcher.
    *
    * @memberOf SimpleEventDispatcher
    */
    constructor() {
      super();
    }
    /**
    * Dispatches the event.
    *
    * @param {TArgs} args The arguments object.
    * @returns {IPropagationStatus} The status of the event.
    *
    * @memberOf SimpleEventDispatcher
    */
    dispatch(args) {
      const result = this._dispatch(false, this, arguments);
      if (result == null)
        throw new ste_core_1$2.DispatchError("Got `null` back from dispatch.");
      return result;
    }
    /**
    * Dispatches the event without waiting for the result.
    *
    * @param {TArgs} args The arguments object.
    *
    * @memberOf SimpleEventDispatcher
    */
    dispatchAsync(args) {
      this._dispatch(true, this, arguments);
    }
    /**
    * Creates an event from the dispatcher. Will return the dispatcher
    * in a wrapper. This will prevent exposure of any dispatcher methods.
    *
    * @returns {ISimpleEvent<TArgs>} The event.
    *
    * @memberOf SimpleEventDispatcher
    */
    asEvent() {
      return super.asEvent();
    }
  };
  exports2.SimpleEventDispatcher = SimpleEventDispatcher$2;
} });
var require_SimpleEventList = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-simple-events@3.0.11/node_modules/ste-simple-events/dist/SimpleEventList.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  const ste_core_1$1 = require_dist$1();
  const SimpleEventDispatcher_1$2 = require_SimpleEventDispatcher();
  var SimpleEventList = class extends ste_core_1$1.EventListBase {
    /**
    * Creates a new SimpleEventList instance.
    */
    constructor() {
      super();
    }
    /**
    * Creates a new dispatcher instance.
    */
    createDispatcher() {
      return new SimpleEventDispatcher_1$2.SimpleEventDispatcher();
    }
  };
  exports2.SimpleEventList = SimpleEventList;
} });
var require_SimpleEventHandlingBase = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-simple-events@3.0.11/node_modules/ste-simple-events/dist/SimpleEventHandlingBase.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  const ste_core_1 = require_dist$1();
  const SimpleEventList_1$1 = require_SimpleEventList();
  var SimpleEventHandlingBase = class extends ste_core_1.HandlingBase {
    constructor() {
      super(new SimpleEventList_1$1.SimpleEventList());
    }
  };
  exports2.SimpleEventHandlingBase = SimpleEventHandlingBase;
} });
var require_NonUniformSimpleEventList = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-simple-events@3.0.11/node_modules/ste-simple-events/dist/NonUniformSimpleEventList.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  const SimpleEventDispatcher_1$1 = require_SimpleEventDispatcher();
  var NonUniformSimpleEventList = class {
    constructor() {
      this._events = {};
    }
    /**
    * Gets the dispatcher associated with the name.
    * @param name The name of the event.
    */
    get(name) {
      if (this._events[name])
        return this._events[name];
      const event = this.createDispatcher();
      this._events[name] = event;
      return event;
    }
    /**
    * Removes the dispatcher associated with the name.
    * @param name The name of the event.
    */
    remove(name) {
      delete this._events[name];
    }
    /**
    * Creates a new dispatcher instance.
    */
    createDispatcher() {
      return new SimpleEventDispatcher_1$1.SimpleEventDispatcher();
    }
  };
  exports2.NonUniformSimpleEventList = NonUniformSimpleEventList;
} });
var require_dist = /* @__PURE__ */ __commonJS2({ "../../node_modules/.pnpm/ste-simple-events@3.0.11/node_modules/ste-simple-events/dist/index.js": (exports2) => {
  Object.defineProperty(exports2, "__esModule", { value: true });
  exports2.NonUniformSimpleEventList = exports2.SimpleEventList = exports2.SimpleEventHandlingBase = exports2.SimpleEventDispatcher = void 0;
  const SimpleEventDispatcher_1 = require_SimpleEventDispatcher();
  Object.defineProperty(exports2, "SimpleEventDispatcher", {
    enumerable: true,
    get: function() {
      return SimpleEventDispatcher_1.SimpleEventDispatcher;
    }
  });
  const SimpleEventHandlingBase_1 = require_SimpleEventHandlingBase();
  Object.defineProperty(exports2, "SimpleEventHandlingBase", {
    enumerable: true,
    get: function() {
      return SimpleEventHandlingBase_1.SimpleEventHandlingBase;
    }
  });
  const NonUniformSimpleEventList_1 = require_NonUniformSimpleEventList();
  Object.defineProperty(exports2, "NonUniformSimpleEventList", {
    enumerable: true,
    get: function() {
      return NonUniformSimpleEventList_1.NonUniformSimpleEventList;
    }
  });
  const SimpleEventList_1 = require_SimpleEventList();
  Object.defineProperty(exports2, "SimpleEventList", {
    enumerable: true,
    get: function() {
      return SimpleEventList_1.SimpleEventList;
    }
  });
} });
var import_dist$1 = /* @__PURE__ */ __toESM2(require_dist(), 1);
var EventSystem = class {
  constructor() {
    this.onLogEvent = new import_dist$1.SimpleEventDispatcher();
    this.onFromRadio = new import_dist$1.SimpleEventDispatcher();
    this.onMeshPacket = new import_dist$1.SimpleEventDispatcher();
    this.onMyNodeInfo = new import_dist$1.SimpleEventDispatcher();
    this.onNodeInfoPacket = new import_dist$1.SimpleEventDispatcher();
    this.onChannelPacket = new import_dist$1.SimpleEventDispatcher();
    this.onConfigPacket = new import_dist$1.SimpleEventDispatcher();
    this.onModuleConfigPacket = new import_dist$1.SimpleEventDispatcher();
    this.onAtakPacket = new import_dist$1.SimpleEventDispatcher();
    this.onMessagePacket = new import_dist$1.SimpleEventDispatcher();
    this.onRemoteHardwarePacket = new import_dist$1.SimpleEventDispatcher();
    this.onPositionPacket = new import_dist$1.SimpleEventDispatcher();
    this.onUserPacket = new import_dist$1.SimpleEventDispatcher();
    this.onRoutingPacket = new import_dist$1.SimpleEventDispatcher();
    this.onDeviceMetadataPacket = new import_dist$1.SimpleEventDispatcher();
    this.onCannedMessageModulePacket = new import_dist$1.SimpleEventDispatcher();
    this.onWaypointPacket = new import_dist$1.SimpleEventDispatcher();
    this.onAudioPacket = new import_dist$1.SimpleEventDispatcher();
    this.onDetectionSensorPacket = new import_dist$1.SimpleEventDispatcher();
    this.onPingPacket = new import_dist$1.SimpleEventDispatcher();
    this.onIpTunnelPacket = new import_dist$1.SimpleEventDispatcher();
    this.onPaxcounterPacket = new import_dist$1.SimpleEventDispatcher();
    this.onSerialPacket = new import_dist$1.SimpleEventDispatcher();
    this.onStoreForwardPacket = new import_dist$1.SimpleEventDispatcher();
    this.onRangeTestPacket = new import_dist$1.SimpleEventDispatcher();
    this.onTelemetryPacket = new import_dist$1.SimpleEventDispatcher();
    this.onZpsPacket = new import_dist$1.SimpleEventDispatcher();
    this.onSimulatorPacket = new import_dist$1.SimpleEventDispatcher();
    this.onTraceRoutePacket = new import_dist$1.SimpleEventDispatcher();
    this.onNeighborInfoPacket = new import_dist$1.SimpleEventDispatcher();
    this.onAtakPluginPacket = new import_dist$1.SimpleEventDispatcher();
    this.onMapReportPacket = new import_dist$1.SimpleEventDispatcher();
    this.onPrivatePacket = new import_dist$1.SimpleEventDispatcher();
    this.onAtakForwarderPacket = new import_dist$1.SimpleEventDispatcher();
    this.onClientNotificationPacket = new import_dist$1.SimpleEventDispatcher();
    this.onDeviceStatus = new import_dist$1.SimpleEventDispatcher();
    this.onLogRecord = new import_dist$1.SimpleEventDispatcher();
    this.onMeshHeartbeat = new import_dist$1.SimpleEventDispatcher();
    this.onDeviceDebugLog = new import_dist$1.SimpleEventDispatcher();
    this.onPendingSettingsChange = new import_dist$1.SimpleEventDispatcher();
    this.onQueueStatus = new import_dist$1.SimpleEventDispatcher();
  }
};
var import_dist = /* @__PURE__ */ __toESM2(require_dist(), 1);
var Queue = class {
  constructor() {
    this.queue = [];
    this.lock = false;
    this.ackNotifier = new import_dist.SimpleEventDispatcher();
    this.errorNotifier = new import_dist.SimpleEventDispatcher();
    this.timeout = 6e4;
  }
  getState() {
    return this.queue;
  }
  clear() {
    this.queue = [];
  }
  push(item) {
    const queueItem = {
      ...item,
      sent: false,
      added: /* @__PURE__ */ new Date(),
      promise: new Promise((resolve, reject) => {
        this.ackNotifier.subscribe((id) => {
          if (item.id === id) {
            this.remove(item.id);
            resolve(id);
          }
        });
        this.errorNotifier.subscribe((e) => {
          if (item.id === e.id) {
            this.remove(item.id);
            reject(e);
          }
        });
        setTimeout(() => {
          if (this.queue.findIndex((qi) => qi.id === item.id) !== -1) {
            this.remove(item.id);
            const decoded = fromBinary2(mesh_pb_exports.ToRadioSchema, item.data);
            if (decoded.payloadVariant.case === "heartbeat" || decoded.payloadVariant.case === "wantConfigId") {
              resolve(item.id);
              return;
            }
            console.warn(`Packet ${item.id} of type ${decoded.payloadVariant.case} timed out`);
            reject({
              id: item.id,
              error: mesh_pb_exports.Routing_Error.TIMEOUT
            });
          }
        }, this.timeout);
      })
    };
    this.queue.push(queueItem);
  }
  remove(id) {
    if (this.lock) {
      setTimeout(() => this.remove(id), 100);
      return;
    }
    this.queue = this.queue.filter((item) => item.id !== id);
  }
  processAck(id) {
    this.ackNotifier.dispatch(id);
  }
  processError(e) {
    console.error(`Error received for packet ${e.id}: ${mesh_pb_exports.Routing_Error[e.error]}`);
    this.errorNotifier.dispatch(e);
  }
  wait(id) {
    const queueItem = this.queue.find((qi) => qi.id === id);
    if (!queueItem)
      throw new Error("Packet does not exist");
    return queueItem.promise;
  }
  async processQueue(outputStream) {
    if (this.lock)
      return;
    this.lock = true;
    const writer = outputStream.getWriter();
    try {
      while (this.queue.filter((p) => !p.sent).length > 0) {
        const item = this.queue.filter((p) => !p.sent)[0];
        if (item) {
          await new Promise((resolve) => setTimeout(resolve, 200));
          try {
            await writer.write(item.data);
            item.sent = true;
          } catch (error) {
            if (error?.code === "ECONNRESET" || error?.code === "ERR_INVALID_STATE") {
              writer.releaseLock();
              this.lock = false;
              throw error;
            }
            console.error(`Error sending packet ${item.id}`, error);
          }
        }
      }
    } finally {
      writer.releaseLock();
      this.lock = false;
    }
  }
};
var fromDeviceStream = () => {
  let byteBuffer = new Uint8Array([]);
  const textDecoder = new TextDecoder();
  return new TransformStream({ transform(chunk, controller) {
    byteBuffer = new Uint8Array([...byteBuffer, ...chunk]);
    let processingExhausted = false;
    while (byteBuffer.length !== 0 && !processingExhausted) {
      const framingIndex = byteBuffer.findIndex((byte) => byte === 148);
      if (byteBuffer[framingIndex + 1] === 195) {
        if (byteBuffer.subarray(0, framingIndex).length) {
          controller.enqueue({
            type: "debug",
            data: textDecoder.decode(byteBuffer.subarray(0, framingIndex))
          });
          byteBuffer = byteBuffer.subarray(framingIndex);
        }
        const msb = byteBuffer[2];
        const lsb = byteBuffer[3];
        if (msb !== void 0 && lsb !== void 0 && byteBuffer.length >= 4 + (msb << 8) + lsb) {
          const packet = byteBuffer.subarray(4, 4 + (msb << 8) + lsb);
          const malformedDetectorIndex = packet.findIndex((byte) => byte === 148);
          if (malformedDetectorIndex !== -1 && packet[malformedDetectorIndex + 1] === 195) {
            console.warn(`\u26A0\uFE0F Malformed packet found, discarding: ${byteBuffer.subarray(0, malformedDetectorIndex - 1).toString()}`);
            byteBuffer = byteBuffer.subarray(malformedDetectorIndex);
          } else {
            byteBuffer = byteBuffer.subarray(3 + (msb << 8) + lsb + 1);
            controller.enqueue({
              type: "packet",
              data: packet
            });
          }
        } else
          processingExhausted = true;
      } else
        processingExhausted = true;
    }
  } });
};
var toDeviceStream = () => {
  return new TransformStream({ transform(chunk, controller) {
    const bufLen = chunk.length;
    const header = new Uint8Array([
      148,
      195,
      bufLen >> 8 & 255,
      bufLen & 255
    ]);
    controller.enqueue(new Uint8Array([...header, ...chunk]));
  } });
};
var Xmodem = class {
  constructor(sendRaw) {
    this.sendRaw = sendRaw;
    this.rxBuffer = [];
    this.txBuffer = [];
    this.textEncoder = new TextEncoder();
    this.counter = 0;
  }
  async downloadFile(filename) {
    return await this.sendCommand(xmodem_pb_exports.XModem_Control.STX, this.textEncoder.encode(filename), 0);
  }
  async uploadFile(filename, data) {
    for (let i = 0; i < data.length; i += 128)
      this.txBuffer.push(data.slice(i, i + 128));
    return await this.sendCommand(xmodem_pb_exports.XModem_Control.SOH, this.textEncoder.encode(filename), 0);
  }
  async sendCommand(command, buffer, sequence, crc16) {
    const toRadio = create2(mesh_pb_exports.ToRadioSchema, { payloadVariant: {
      case: "xmodemPacket",
      value: {
        buffer,
        control: command,
        seq: sequence,
        crc16
      }
    } });
    return await this.sendRaw(toBinary2(mesh_pb_exports.ToRadioSchema, toRadio));
  }
  async handlePacket(packet) {
    await new Promise((resolve) => setTimeout(resolve, 100));
    switch (packet.control) {
      case xmodem_pb_exports.XModem_Control.NUL:
        break;
      case xmodem_pb_exports.XModem_Control.SOH:
        this.counter = packet.seq;
        if (this.validateCrc16(packet)) {
          this.rxBuffer[this.counter] = packet.buffer;
          return this.sendCommand(xmodem_pb_exports.XModem_Control.ACK);
        }
        return await this.sendCommand(xmodem_pb_exports.XModem_Control.NAK, void 0, packet.seq);
      case xmodem_pb_exports.XModem_Control.STX:
        break;
      case xmodem_pb_exports.XModem_Control.EOT:
        break;
      case xmodem_pb_exports.XModem_Control.ACK:
        this.counter++;
        if (this.txBuffer[this.counter - 1])
          return this.sendCommand(xmodem_pb_exports.XModem_Control.SOH, this.txBuffer[this.counter - 1], this.counter, crc16ccitt_default(this.txBuffer[this.counter - 1] ?? new Uint8Array()));
        if (this.counter === this.txBuffer.length + 1)
          return this.sendCommand(xmodem_pb_exports.XModem_Control.EOT);
        this.clear();
        break;
      case xmodem_pb_exports.XModem_Control.NAK:
        return this.sendCommand(xmodem_pb_exports.XModem_Control.SOH, this.txBuffer[this.counter], this.counter, crc16ccitt_default(this.txBuffer[this.counter - 1] ?? new Uint8Array()));
      case xmodem_pb_exports.XModem_Control.CAN:
        this.clear();
        break;
      case xmodem_pb_exports.XModem_Control.CTRLZ:
        break;
    }
    return Promise.resolve(0);
  }
  validateCrc16(packet) {
    return crc16ccitt_default(packet.buffer) === packet.crc16;
  }
  clear() {
    this.counter = 0;
    this.rxBuffer = [];
    this.txBuffer = [];
  }
};
var utils_exports = __export({
  EventSystem: () => EventSystem,
  Queue: () => Queue,
  Xmodem: () => Xmodem,
  fromDeviceStream: () => fromDeviceStream,
  toDeviceStream: () => toDeviceStream
});

// server/server.js
var { Mesh, Portnums, Telemetry, Channel: ChannelPb } = dist_exports;
var { FromRadioSchema: FromRadioSchema2, ToRadioSchema: ToRadioSchema2, MeshPacketSchema: MeshPacketSchema2, PositionSchema: PositionSchema2, UserSchema: UserSchema2, RoutingSchema: RoutingSchema2, Routing_Error: Routing_Error2 } = Mesh;
var { PortNum: PortNum2 } = Portnums;
var { TelemetrySchema: TelemetrySchema2 } = Telemetry;
var SERVER_PORT = parseInt(process.env.MESHNATTER_PORT || "3000");
var POLL_MS = 400;
var HEARTBEAT_MS = 8e3;
var MAX_POLL_ERRORS = 5;
var RECONNECT_MS = 5e3;
var httpServer = (0, import_http.createServer)((req, res) => {
  res.writeHead(200);
  res.end("Meshnatter server running");
});
var wss = new import_websocket_server.default({ server: httpServer });
var nodeHost = null;
var nodePort = 80;
var pollTimer = null;
var heartbeatTimer = null;
var active = false;
var polling = false;
var pollErrors = 0;
var reconnectTimer = null;
function broadcast(obj) {
  const msg = JSON.stringify(obj);
  wss.clients.forEach((c) => {
    if (c.readyState === 1)
      c.send(msg);
  });
}
function log(msg) {
  console.log(`[${(/* @__PURE__ */ new Date()).toTimeString().slice(0, 8)}] ${msg}`);
  broadcast({ type: "log", msg });
}
function nodeBaseUrl() {
  return `http://${nodeHost}${nodePort !== 80 ? ":" + nodePort : ""}`;
}
function numToId(num) {
  return "!" + (num >>> 0).toString(16).padStart(8, "0");
}
async function httpGet(url, timeout = 6e3) {
  const res = await fetch(url, { headers: { "Accept": "application/x-protobuf" }, signal: AbortSignal.timeout(timeout) });
  if (!res.ok)
    throw new Error(`HTTP ${res.status}`);
  return new Uint8Array(await res.arrayBuffer());
}
async function httpPut(url, body, timeout = 6e3) {
  const res = await fetch(url, { method: "PUT", headers: { "Content-Type": "application/x-protobuf" }, body, signal: AbortSignal.timeout(timeout) });
  if (!res.ok)
    throw new Error(`HTTP PUT ${res.status}`);
}
async function sendWantConfig() {
  const configId = Math.floor(Math.random() * 16777215);
  const toRadio = create(ToRadioSchema2, { payloadVariant: { case: "wantConfigId", value: configId } });
  await httpPut(`${nodeBaseUrl()}/api/v1/toradio`, toBinary(ToRadioSchema2, toRadio));
  log(`wantConfig sent (id=${configId})`);
}
async function sendHeartbeat() {
  if (!active)
    return;
  try {
    await httpPut(`${nodeBaseUrl()}/api/v1/toradio`, toBinary(ToRadioSchema2, create(ToRadioSchema2, { payloadVariant: { case: "heartbeat", value: {} } })), 4e3);
  } catch {
  }
}
function handleFromRadio(bytes) {
  if (!bytes?.length)
    return;
  let pkt;
  try {
    pkt = fromBinary(FromRadioSchema2, bytes);
  } catch {
    return;
  }
  if (!pkt?.payloadVariant?.case)
    return;
  const { case: kind, value: val } = pkt.payloadVariant;
  if (kind === "myInfo") {
    broadcast({ type: "myInfo", myNodeNum: Number(val.myNodeNum) });
  } else if (kind === "nodeInfo") {
    const n = val;
    broadcast({ type: "node", node: {
      num: Number(n.num),
      id: numToId(Number(n.num)),
      name: n.user?.longName || n.user?.shortName || numToId(Number(n.num)),
      shortName: n.user?.shortName || "???",
      hwModel: n.user?.hwModel ?? 0,
      lat: n.position?.latitudeI ? n.position.latitudeI / 1e7 : null,
      lon: n.position?.longitudeI ? n.position.longitudeI / 1e7 : null,
      alt: n.position?.altitude ?? null,
      battery: n.deviceMetrics?.batteryLevel ?? null,
      voltage: n.deviceMetrics?.voltage ? (n.deviceMetrics.voltage / 1e3).toFixed(2) : null,
      chUtil: n.deviceMetrics?.channelUtilization ?? null,
      airUtil: n.deviceMetrics?.airUtilTx ?? null,
      snr: n.snr ?? null,
      hopsAway: n.hopsAway ?? null,
      lastHeard: n.lastHeard ? new Date(Number(n.lastHeard) * 1e3).toISOString() : null
    } });
  } else if (kind === "channel") {
    const ch = val;
    if (ch.role === 0)
      return;
    broadcast({ type: "channel", channel: {
      index: ch.index,
      name: ch.settings?.name || (ch.index === 0 ? "Primary" : `Channel ${ch.index}`),
      role: ch.role === 1 ? "PRIMARY" : "SECONDARY"
    } });
  } else if (kind === "packet") {
    const mp = val;
    const from = Number(mp.from), to = Number(mp.to);
    const rxRssi = mp.rxRssi ?? null, rxSnr = mp.rxSnr ?? null;
    const hopLimit = mp.hopLimit ?? null, hopStart = mp.hopStart ?? null;
    broadcast({ type: "signal", fromNum: from, fromId: numToId(from), rxRssi, rxSnr, hopsAway: hopStart != null && hopLimit != null ? hopStart - hopLimit : null });
    broadcast({ type: "lastHeard", fromNum: from, ts: (/* @__PURE__ */ new Date()).toISOString() });
    const decoded = mp.decoded;
    if (!decoded)
      return;
    if (decoded.portnum === PortNum2.TEXT_MESSAGE_APP) {
      broadcast({ type: "message", id: Number(mp.id), fromNum: from, fromId: numToId(from), toNum: to, toId: numToId(to), channel: mp.channel ?? 0, isDM: to !== 4294967295, text: new TextDecoder().decode(decoded.payload), rxRssi, rxSnr, hopLimit, hopStart, rxTime: (/* @__PURE__ */ new Date()).toISOString() });
    } else if (decoded.portnum === PortNum2.ROUTING_APP) {
      try {
        const routing = fromBinary(RoutingSchema2, decoded.payload);
        const requestId = Number(decoded.requestId || 0);
        let ackStatus = "ack", errorCode = null;
        if (routing.variant?.case === "errorReason") {
          const errVal = routing.variant.value;
          if (errVal !== Routing_Error2.NONE && errVal !== 0) {
            ackStatus = "nack";
            errorCode = Routing_Error2[errVal] || `ERROR_${errVal}`;
          }
        }
        broadcast({ type: "ack", requestId, fromNum: from, fromId: numToId(from), ackStatus, errorCode });
      } catch {
      }
    } else if (decoded.portnum === PortNum2.POSITION_APP) {
      try {
        const pos = fromBinary(PositionSchema2, decoded.payload);
        broadcast({ type: "position", fromNum: from, fromId: numToId(from), lat: pos.latitudeI ? pos.latitudeI / 1e7 : null, lon: pos.longitudeI ? pos.longitudeI / 1e7 : null, alt: pos.altitude ?? null });
      } catch {
      }
    } else if (decoded.portnum === PortNum2.TELEMETRY_APP) {
      try {
        const tel = fromBinary(TelemetrySchema2, decoded.payload);
        const dm = tel.variant?.case === "deviceMetrics" ? tel.variant.value : null;
        broadcast({ type: "telemetry", fromNum: from, fromId: numToId(from), batteryLevel: dm?.batteryLevel ?? null, voltage: dm?.voltage ? (dm.voltage / 1e3).toFixed(2) : null, chUtil: dm?.channelUtilization ?? null, airUtil: dm?.airUtilTx ?? null, rxRssi, rxSnr });
      } catch {
      }
    } else if (decoded.portnum === PortNum2.NODEINFO_APP) {
      try {
        const user = fromBinary(UserSchema2, decoded.payload);
        broadcast({ type: "nodeUser", fromNum: from, fromId: numToId(from), longName: user.longName, shortName: user.shortName });
      } catch {
      }
    }
  } else if (kind === "configCompleteId") {
    log("Mesh ready");
    broadcast({ type: "dbReady" });
  }
}
async function poll() {
  if (!active || polling)
    return;
  polling = true;
  try {
    let i = 0;
    while (active && i < 100) {
      const b = await httpGet(`${nodeBaseUrl()}/api/v1/fromradio`);
      if (!b.length)
        break;
      handleFromRadio(b);
      i++;
    }
    pollErrors = 0;
  } catch (e) {
    if (!active)
      return;
    if (++pollErrors >= MAX_POLL_ERRORS) {
      log("Reconnecting...");
      broadcast({ type: "reconnecting" });
      stopTimers();
      reconnectTimer = setTimeout(attemptReconnect, RECONNECT_MS);
    }
  } finally {
    polling = false;
  }
}
async function attemptReconnect() {
  if (!nodeHost)
    return;
  try {
    await fetch(`${nodeBaseUrl()}/`, { signal: AbortSignal.timeout(5e3) });
    active = true;
    pollErrors = 0;
    await sendWantConfig();
    startTimers();
    broadcast({ type: "connected", ip: nodeHost });
    log("Reconnected");
  } catch {
    reconnectTimer = setTimeout(attemptReconnect, RECONNECT_MS);
  }
}
function startTimers() {
  stopTimers();
  pollTimer = setInterval(poll, POLL_MS);
  heartbeatTimer = setInterval(sendHeartbeat, HEARTBEAT_MS);
}
function stopTimers() {
  clearInterval(pollTimer);
  clearInterval(heartbeatTimer);
  clearTimeout(reconnectTimer);
  pollTimer = null;
  heartbeatTimer = null;
  reconnectTimer = null;
  polling = false;
}
async function sendText(text, destNum, channelIndex) {
  if (typeof text !== "string" || text.length === 0 || text.length > 237) {
    throw new Error("Message must be 1-237 characters (Meshtastic limit)");
  }
  const mp = create(MeshPacketSchema2, { to: destNum ?? 4294967295, channel: channelIndex ?? 0, decoded: { payload: new TextEncoder().encode(text), portnum: PortNum2.TEXT_MESSAGE_APP }, wantAck: true, hopLimit: 3 });
  await httpPut(`${nodeBaseUrl()}/api/v1/toradio`, toBinary(ToRadioSchema2, create(ToRadioSchema2, { payloadVariant: { case: "packet", value: mp } })));
  return Number(mp.id);
}
async function doConnect(ip, port) {
  port = port || 80;
  const ipOk = /^[a-zA-Z0-9._-]+$/.test(ip) && ip.length < 64;
  const portOk = Number.isInteger(port) && port > 0 && port < 65536;
  if (!ipOk || !portOk) {
    broadcast({ type: "connectError", error: "Invalid IP address or port" });
    return;
  }
  nodeHost = ip;
  nodePort = port;
  active = false;
  pollErrors = 0;
  const baseUrl = `http://${ip}${port !== 80 ? ":" + port : ""}`;
  log(`Connecting to ${baseUrl}...`);
  try {
    await fetch(`http://${ip}${port !== 80 ? ":" + port : ""}/`, { signal: AbortSignal.timeout(5e3) });
  } catch (e) {
    broadcast({ type: "connectError", error: `Cannot reach ${ip} \u2014 ${e.message}` });
    return;
  }
  await (async () => {
    try {
      let n = 0;
      while (n < 50) {
        const b = await httpGet(`http://${ip}${port !== 80 ? ":" + port : ""}/api/v1/fromradio`, 3e3);
        if (!b.length)
          break;
        n++;
      }
    } catch {
    }
  })();
  active = true;
  try {
    await sendWantConfig();
  } catch (e) {
    broadcast({ type: "connectError", error: `Init failed: ${e.message}` });
    active = false;
    return;
  }
  broadcast({ type: "connected", ip });
  startTimers();
}
function doDisconnect(silent = false) {
  active = false;
  pollErrors = 0;
  nodeHost = null;
  stopTimers();
  if (!silent)
    broadcast({ type: "disconnected" });
}
wss.on("connection", (ws) => {
  ws.send(JSON.stringify({ type: active ? "connected" : "disconnected", ip: nodeHost }));
  ws.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    if (msg.type === "connect") {
      doDisconnect(true);
      await doConnect(msg.ip, msg.port || 80);
    } else if (msg.type === "disconnect") {
      doDisconnect();
    } else if (msg.type === "sendMessage") {
      try {
        const id = await sendText(msg.text, msg.destinationNum ?? null, msg.channelIndex ?? 0);
        broadcast({ type: "messageSent", text: msg.text, destinationNum: msg.destinationNum, channelIndex: msg.channelIndex, packetId: id, ts: (/* @__PURE__ */ new Date()).toISOString() });
      } catch (e) {
        ws.send(JSON.stringify({ type: "sendError", error: e.message }));
      }
    }
  });
});
httpServer.listen(SERVER_PORT, "127.0.0.1", () => {
  log(`Server listening on port ${SERVER_PORT}`);
});
/*! Bundled license information:

@meshtastic/core/dist/mod.js:
  (*!
  * Strongly Typed Events for TypeScript - Core
  * https://github.com/KeesCBakker/StronlyTypedEvents/
  * http://keestalkstech.com
  *
  * Copyright Kees C. Bakker / KeesTalksTech
  * Released under the MIT license
  *)
*/
