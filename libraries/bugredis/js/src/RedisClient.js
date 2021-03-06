/*
 * Copyright (c) 2014 airbug Inc. All rights reserved.
 *
 * All software, both binary and source contained in this work is the exclusive property
 * of airbug Inc. Modification, decompilation, disassembly, or any other means of discovering
 * the source code of this software is prohibited. This work is protected under the United
 * States copyright law and other international copyright treaties and conventions.
 */


//-------------------------------------------------------------------------------
// Annotations
//-------------------------------------------------------------------------------

//@Export('bugredis.RedisClient')

//@Require('Class')
//@Require('EventDispatcher')
//@Require('Map')
//@Require('bugredis.RedisEvent')


//-------------------------------------------------------------------------------
// Context
//-------------------------------------------------------------------------------

require('bugpack').context("*", function(bugpack) {

    //-------------------------------------------------------------------------------
    // BugPack
    //-------------------------------------------------------------------------------

    var Class               = bugpack.require('Class');
    var EventDispatcher     = bugpack.require('EventDispatcher');
    var Map                 = bugpack.require('Map');
    var RedisEvent          = bugpack.require('bugredis.RedisEvent');


    //-------------------------------------------------------------------------------
    // Declare Class
    //-------------------------------------------------------------------------------

    /**
     * @class
     * @extends {EventDispatcher}
     */
    var RedisClient = Class.extend(EventDispatcher, {

        _name: "bugredis.RedisClient",


        //-------------------------------------------------------------------------------
        // Constructor
        //-------------------------------------------------------------------------------

        /**
         * @constructs
         * @param {*} redis
         * @param {RedisConfig} config
         */
        _constructor: function(redis, config) {

            this._super();


            //-------------------------------------------------------------------------------
            // Private Properties
            //-------------------------------------------------------------------------------


            /**
             * @private
             * @type {Map}
             */
            this.channelToRedisSubscriberMap        = new Map();

            /**
             * @private
             * @type {RedisConfig}
             */
            this.config                             = config;

            /**
             * @private
             * @type {boolean}
             */
            this.connected                          = false;

            /**
             * @private
             * @type {boolean}
             */
            this.connecting                         = false;

            /**
             * @private
             * @type {*}
             */
            this.redis                              = redis;

            /**
             * @private
             * @type {*}
             */
            this.client                             = null;
        },


        //-------------------------------------------------------------------------------
        // Getters and Setters
        //-------------------------------------------------------------------------------

        /**
         * @return {*}
         */
        getClient: function() {
            return this.client;
        },

        /**
         * @return {RedisConfig}
         */
        getConfig: function() {
            return this.config;
        },

        /**
         * @return {boolean}
         */
        getConnected: function() {
            return this.connected;
        },

        /**
         * @return {boolean}
         */
        getConnecting: function() {
            return this.connecting;
        },

        /**
         * @return {*}
         */
        getRedis: function() {
            return this.redis;
        },

        /**
         * @return {boolean}
         */
        isConnected: function() {
            return this.connected;
        },


        //-------------------------------------------------------------------------------
        // Public Methods
        //-------------------------------------------------------------------------------

        /**
         * @param {function(Throwable=)} callback
         */
        connect: function(callback) {
            var _this = this;
            var cbFired = false;
            if (!this.getConnected() && !this.getConnecting()) {
                this.connecting = true;
                this.client     = this.redis.createClient(this.config.getPort(), this.config.getHost());
                this.client.on("connect", function() {
                    _this.dispatchEvent(new RedisEvent(RedisEvent.EventTypes.CONNECT));
                });
                this.client.on("drain", function() {
                    _this.dispatchEvent(new RedisEvent(RedisEvent.EventTypes.DRAIN));
                });
                this.client.on("end", function() {
                    _this.connected = false;
                    _this.dispatchEvent(new RedisEvent(RedisEvent.EventTypes.END));
                });
                this.client.on("error", function(error) {
                    console.error(error.message);
                    console.error(error.stack);
                    if (!cbFired) {
                        cbFired = true;
                        callback(error);
                    }
                    _this.dispatchEvent(new RedisEvent(RedisEvent.EventTypes.ERROR));
                });
                this.client.on("idle", function() {
                    _this.dispatchEvent(new RedisEvent(RedisEvent.EventTypes.IDLE));
                });
                this.client.on("ready", function() {
                    _this.connecting = false;
                    _this.connected = true;
                    console.log("Connected to redis server on port ", _this.config.getPort());
                    _this.dispatchEvent(new RedisEvent(RedisEvent.EventTypes.READY));
                    if (!cbFired) {
                        cbFired = true;
                        callback();
                    }
                });
                this.client.on("message", function(channel, message) {
                    _this.dispatchEvent(new RedisEvent(RedisEvent.EventTypes.MESSAGE, {
                        channel: channel,
                        message: message
                    }));
                });
                this.client.on("subscribe", function(channel, count) {
                    _this.dispatchEvent(new RedisEvent(RedisEvent.EventTypes.SUBSCRIBE, {
                        channel: channel,
                        count: count
                    }));
                });
                this.client.on("unsubscribe", function(channel, count) {
                    _this.dispatchEvent(new RedisEvent(RedisEvent.EventTypes.UNSUBSCRIBE, {
                        channel: channel,
                        count: count
                    }));
                });
            }
        },

        /**
         *
         */
        quit: function() {
            this.client.quit();
        },


        //-------------------------------------------------------------------------------
        // Proxy Methods
        //-------------------------------------------------------------------------------

        bRPopLPush: function() {
            return this.client.brpoplpush.apply(this.client, arguments);
        },

        del: function() {
            return this.client.del.apply(this.client, arguments);
        },

        exists: function() {
            return this.client.exists.apply(this.client, arguments);
        },

        get: function() {
            return this.client.get.apply(this.client, arguments);
        },

        getRange: function() {
            return this.client.getrange.apply(this.client, arguments);
        },

        hDel: function() {
            return this.client.hdel.apply(this.client, arguments);
        },

        hExists: function() {
            return this.client.hexists.apply(this.client, arguments);
        },

        hGet: function() {
            return this.client.hget.apply(this.client, arguments);
        },

        hLen: function() {
            return this.client.hlen.apply(this.client, arguments);
        },

        hSet: function() {
            return this.client.hset.apply(this.client, arguments);
        },

        hVals: function() {
            return this.client.hvals.apply(this.client, arguments);
        },

        /**
         * @param {string} key
         * @param {function(number)} callback
         */
        incr: function(key, callback) {
            return this.client.incr.apply(this.client, arguments);
        },

        lPush: function() {
            return this.client.lpush.apply(this.client, arguments);
        },

        lRem: function() {
            return this.client.lrem.apply(this.client, arguments);
        },

        multi: function() {
            return this.client.multi.apply(this.client, arguments);
        },

        publish: function() {
            return this.client.publish.apply(this.client, arguments);
        },

        rPopLPush: function() {
            return this.client.rpoplpush.apply(this.client, arguments);
        },

        sAdd: function() {
            return this.client.sadd.apply(this.client, arguments);
        },

        sCard: function() {
            return this.client.scard.apply(this.client, arguments);
        },

        set: function() {
            return this.client.set.apply(this.client, arguments);
        },

        setNX: function() {
            return this.client.setnx.apply(this.client, arguments);
        },

        sMembers: function() {
            return this.client.smembers.apply(this.client, arguments);
        },

        sRem: function() {
            return this.client.srem.apply(this.client, arguments);
        },

        /**
         * @param {string} channel
         * @param {function(Error, string)} callback
         * @return {*}
         */
        subscribe: function(channel, callback) {
            return this.client.subscribe.apply(this.client, arguments);
        },

        unsubscribe: function() {
            return this.client.unsubscribe.apply(this.client, arguments);
        }
    });


    //-------------------------------------------------------------------------------
    // Exports
    //-------------------------------------------------------------------------------

    bugpack.export('bugredis.RedisClient', RedisClient);
});
