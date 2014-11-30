
var _ = require('lodash');
var async = require('async');
var express = require('express');
var serveStatic = require('serve-static');
var compression = require('compression');
var mongoose = require('mongoose');
var _ws = require('websocket').client;
var ws = new _ws();
var debug = true;

var log = function(s) {
    if(debug) console.log(s);
};

Date.prototype.addHours = function(h){
    this.setHours(this.getHours() + h);
    return this;
};
Date.prototype.subHours = function(h){
    this.setHours(this.getHours() - h);
    return this;
};

var ticker = [];
var trades = [];

mongoose.connect('mongodb://localhost/okcoinfutures');

var TickerSchema = mongoose.Schema({
    timestamp : { type: Date, default: Date.now },
    contractId : String,
    buy : Number,
    high : Number,
    low : Number,
    sell : Number,
    hold_amount : Number,
    last : Number,
    unitAmount : Number,
    vol : Number
});
var TickerModel = mongoose.model('Ticker', TickerSchema);

var TradeSchema = mongoose.Schema({
    timestamp : { type: Date, default: Date.now },
    price : Number,
    type : String,
    vol : Number
});
var TradeModel = mongoose.model('Trade', TradeSchema);


var socketlist = [];
var ok, io, http, httpserver;

var getTicker = function(req, res) {
    var from = req.query.from;
    var to = req.query.to;
    if(!(from && to)) {
        var d = new Date();
        to = d.getTime();
        d.setHours(d.getHours()-24);
        from = d.getTime();
    }
    var out = [];
    for(var k in ticker) {
        k = ticker[k];
        if(from <= k.timestamp && to >= k.timestamp) {
            out.push(k);
        }
    }
    res.send(ticker);
};

var getTrades = function(req, res) {
    var from = req.query.from;
    var to = req.query.to;
    if(!(from && to)) {
        var d = new Date();
        to = d.getTime();
        d.setHours(d.getHours()-24);
        from = d.getTime();
    }
    var out = [];
    for(var k in trades) {
        k = trades[k];
        if(from <= k.timestamp && to >= k.timestamp) {
            out.push(k);
        }
    }
    res.send(out);
};


var setupHTTP = function(next) {
    http = express();
    http.use(compression({ threshold: 512 }));
    http.use(serveStatic(__dirname + '/static'));
    http.get('/ticker', getTicker);
    http.get('/trades', getTrades);
    httpserver = http.listen(3000);
    console.log('http server running');
    next();
};

var clientDisconnected = function(socket) {
    for(var i=0; i< socketlist; i++) {
        if(socketlist[i] === socket) {
            socketlist.splice(i, 1);
        }
    }
};

var clientHistory = function(socket) {
    send(socket, 'trades', trades);
};

var clientHistory = function(socket) {
    send(socket, 'ticker', ticker);
};

var clientConnected = function(socket) {
    socket.on('disconnect', clientDisconnected);
    socket.on('history', clientHistory);
    socketlist.push(socket);
    send(socket, {message: 'connected'});
};

var setupOKCoinSubscription = function(next) {
    ws.on('connect', function(connection) {
        connection.on('message', function(res){
            var data = JSON.parse(res.utf8Data)[0];
            if(data.channel === 'ok_btcusd_future_ticker_this_week') {
                var ti = new TickerModel(data.data);
                ti.save(function (err) {});
                var d = new Date();
                d.addHours(15);
                data.data.timestamp = d;
                ticker.push(data.data);
                data.data.timestamp =  d.getTime();
                broadcast('ticker', data.data);
                log('wrote 1 ticker to db.');
            } else if(data.channel === 'ok_btcusd_future_trade_this_week') {
                var ao = [];
                for(var k in data.data) {
                    k = data.data[k];
                    var d = new Date().addHours(15);
                    var dp = k[2].split(':');
                    d.setHours(dp[0]);
                    d.setMinutes(dp[1]);
                    d.setSeconds(dp[2]);
                    d.subHours(15);
                    var o = {
                        timestamp: d,
                        price : parseFloat(k[0]),
                        vol : parseInt(k[1]),
                        type : k[3]
                    };
                    var ti = new TradeModel(o);
                    ti.save(function (err) {});;
                    trades.push(o);
                    o.timestamp = d.getTime();
                    ao.push(o);
                }
                broadcast('trades', ao);
                log('wrote ' + data.data.length + ' trades to db.');
            }
        });
        connection.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_this_week'}");
        connection.send("{'event':'addChannel','channel':'ok_btcusd_future_trade_this_week'}");
        next();
    });
    ok = ws.connect('wss://real.okcoin.com:10440/websocket/okcoinapi');
};

var setupWebsocketServer = function(next) {
    io = require('socket.io')(httpserver);
    io.on('connection', clientConnected);
    console.log('websocket server running');
    next();
};

var send = function(conn , msg,  obj) {
    conn.emit(msg, obj);
};

var broadcast = function(msg, obj) {
    if(!io || !obj) return;
    io.emit(msg, obj);
};

async.series(
    [
        setupHTTP,
        setupWebsocketServer,
        setupOKCoinSubscription
    ],
    function() {
        console.log('okcoin data server running');
    }
);
