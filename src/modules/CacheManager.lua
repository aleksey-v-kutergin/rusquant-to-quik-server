---------------------------------------------------------------------------------------
-- CacheManager.lua
-- Author: Aleksey Kutergin <aleksey.v.kutergin@gmail.com>
-- Company: rusquant.ru
-- Date: 11.07.2017
---------------------------------------------------------------------------------------
-- This module provides functionality for caching transaction's replays, orders, trades
-- and othe objects. Also, it provides methods to manage cached objects.
--
---------------------------------------------------------------------------------------

-- src.modules.CacheManager
local CacheManager = {};


---------------------------------------------------------------------------------------
-- Available objects for caching
--
---------------------------------------------------------------------------------------
local TRANS_REPLAY = "TRANS_REPLAY";
local ORDER = "ORDER";
local TRADE = "TRADE";
local logger;
local jsonParser;

---------------------------------------------------------------------------------------
-- Caching replays for transactions;
-- The key of the replay object in cache is trans_id ( user defined id of the transaction ).
--
---------------------------------------------------------------------------------------
local trasactionsReplayCache = {};
local transCacheSize = 0;


local function cacheTransactionReplay(transReplay)
    logger.writeToLog(this, "CACHING TRANSACTION REPLAY: \n" .. jsonParser: encode_pretty(transReplay));
    trasactionsReplayCache[transReplay.trans_id] = transReplay;
    transCacheSize = transCacheSize + 1;
end;


local function findTransactionReplay(transId)
    logger.writeToLog(this, "CHECKING FOR EXISTANCE OF TRANSACTION REPLAY IN CACHE WITHIN!");
    local cacheItem = trasactionsReplayCache[transId];
    if cacheItem ~= nil then
        trasactionsReplayCache[transId] = nil;
        transCacheSize = transCacheSize - 1;
        logger.writeToLog(this, "FIND TRANSE_REPLAY IN CACHE FOR TRANSACTION: " .. transId .."!");
    end;
    return cacheItem;
end;



---------------------------------------------------------------------------------------
-- Caching orders;
-- The key of the order object in cache is order_num ( because it is available in replay for transacrion ).
--
---------------------------------------------------------------------------------------
local ordersCache = {};
local ordersCacheSize = 0;


local function cacheOrder(order)
    logger.writeToLog(this, "Caching order: \n" .. jsonParser: encode_pretty(order));
    ordersCache[order.order_num] = order;
    ordersCacheSize = ordersCacheSize + 1;
end;


local function findOrder(orderNum)
    local cacheItem = ordersCache[orderNum];
    if cacheItem ~= nil then
        ordersCache[orderNum] = nil;
        ordersCacheSize = ordersCacheSize - 1;
    end;
    return cacheItem;
end;



---------------------------------------------------------------------------------------
-- Caching trades;
-- A single order may cause several trades. So one needs to cache a collection of trades.
-- The key of the trade's collection in cache is order_num ( id of order that triggered  ).
--
---------------------------------------------------------------------------------------
local tradesCache = {};
local tradesCacheSize = 0;


local function cacheTrade(trade)
    local trades = tradesCache[trade.order_num];
    if trades ~= nil then
        -- Add new trade for existing trade's collection
        trades[trade.trade_num] = trade;
    else
        -- Create new trades collection
        trades = {};
        trades[trade.trade_num] = trade;
        tradesCache[trade.order_num] = trades;
    end;
    tradesCacheSize = tradesCacheSize + 1;
end;


local function findTrades(orderNum)
    local trades = tradesCache[orderNum];
    if trades ~= nil then
        for k, v in pairs(trades) do
            tradesCacheSize = tradesCacheSize - 1;
        end;
        tradesCache[orderNum] = nil;
    end;
    return trades;
end;



---------------------------------------------------------------------------------------
-- Public functions to work with cache
--
---------------------------------------------------------------------------------------
function CacheManager : cache(objectType, object)
    if(TRANS_REPLAY == objectType) then
        cacheTransactionReplay(object);
    elseif ORDER == objectType then
        cacheOrder(object);
    elseif TRADE == objectType then
        cacheTrade(object);
    else
        -- DO NOTHING
    end;
end;


function CacheManager : find(objectType, key)
    if(TRANS_REPLAY == objectType) then
        return findTransactionReplay(key);
    elseif ORDER == objectType then
        return findOrder(key);
    elseif TRADE == objectType then
        return findTrades(key);
    else
        return nil;
    end;
end;


function CacheManager : resetCache()
    trasactionsReplayCache = {};
    transCacheSize = 0;
    ordersCache = {};
    ordersCacheSize = 0;
    tradesCache = {};
    tradesCacheSize = 0;
end;


function CacheManager : setLogger(externalLogger)
    logger = externalLogger;
    logger.writeToLog(this, "LOGGER WITHIN CACHE MANAGER");
end;


function CacheManager : setJsonParser(parser)
    jsonParser = parser;
    logger.writeToLog(this, "JSOM PARSE WITHIN CACHE MANAGER");
end;


-- End of CacheManager module
return CacheManager;