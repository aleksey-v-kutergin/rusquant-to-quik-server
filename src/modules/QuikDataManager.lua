---------------------------------------------------------------------------------------
-- QuikDataManager.lua
-- Author: Aleksey Kutergin <aleksey.v.kutergin@gmail.com>
-- Company: rusquant.ru
-- Date: 11.07.2017
---------------------------------------------------------------------------------------
-- This module provides functionality for ...
--
---------------------------------------------------------------------------------------

-- src.modules.CacheManager
local QuikDataManager = {};

local logger;
local jsonParser;
local cacheManager;


---------------------------------------------------------------------------------------
-- This is because one cannot hold nil for key in hash table
-- At the java side one could serialize all fields of the Transaction class, including fields without referense to data (null value)
-- In resulting JSON-object it looks like "someFiledName": null.
-- But when converting from JSON to lua table, JSON parser does not include null-field to resulting table
-- Thus, one needs some kind of etalon set of feilds. This set must not be out of date!
--
---------------------------------------------------------------------------------------
local TRANSACTION_FIELDS =
{
    "ACCOUNT",
    "BALANCE",
    "BROKERREF",
    "CLASSCODE",
    "CLIENT_CODE",
    "EXCHANGE_CODE",
    "FIRM_ID",
    "FLAGS",
    "ORDER_NUM",
    "PRICE",
    "QUANTITY",
    "RESULT_MSG",
    "SECCODE",
    "SERVER_TRANS_ID",
    "TIME",
    "TRANS_ID",
    "UID",
    "ACTION",
    "STATUS",
    "OPERATION",
    "TYPE",
    "COMMENT",
    "MODE"
}



---------------------------------------------------------------------------------------
-- Setter for external dependencies
--
---------------------------------------------------------------------------------------

function QuikDataManager : setLogger(externalLogger)
    logger = externalLogger;
    logger.writeToLog(this, "LOGGER WITHIN QUIK DATA MANAGER");
end;

function QuikDataManager : setJsonParser(parser)
    jsonParser = parser;
    logger.writeToLog(this, "JSON PASER WITHIN QUIK DATA MANAGER!");
end;


function QuikDataManager : setCacheManager(cache)
    cacheManager = cache;
    logger.writeToLog(this, "CACHE MANAGER WITHIN QUIK DATA MANAGER!");
end;





---------------------------------------------------------------------------------------
-- Section with server-side functionality for transaction processing.
--
---------------------------------------------------------------------------------------

local TRANSACTION_REPLAY_RETRY_COUNT = 10000000;

local function prepareTransactionArgs(transaction)
    local args = {};
    for key, value in pairs(transaction) do
        if type(value) == "string" then
            if key ~= "MODE" then
                args[key] = value;
            end;
        else
            args[key] = tostring(value);
        end;
    end;
    return args;
end;



local function getTransactionResult(transaction)
    local transId = transaction["TRANS_ID"];
    local replay = cacheManager.find(this, "TRANS_REPLAY", transId)

    local result = {};
    local counter = 0;
    while replay == nil do
        replay = cacheManager.find(this, "TRANS_REPLAY", transId);
        if counter > TRANSACTION_REPLAY_RETRY_COUNT then
            result["status"] = "FAILED";
            result["error"] = "EXCEEDING RETRY COUNT FOR WAITING OF OCCURANCE OF THE TRANSACTION REPLAY IN CACHE! TRANSACTION REPLAY HAS NOT OCCURED YET!";
            return result;
        end;
        counter = counter + 1;
    end

    result["status"] = "SUCCESS";
    result["trans_replay"] = replay
    return result;
end;



function QuikDataManager : sendTransaction(transaction)
    transaction["type"] = nil;
    local args = prepareTransactionArgs(transaction);
    logger.writeToLog(this, "\nCALL SEND TRANSACTION WITH ARGS: " .. jsonParser: encode_pretty(args) .. "\n");
    local error = sendTransaction(args);
    local result = {};
    if error ~= nil and error ~= '' then
        logger.writeToLog(this, "\nSEND TRANSACTION FAILS WITH ERROR: " .. error .. "\n");
        result["status"] = "FAILED";
        result["error"] = error;
        return result;
    end;

    local transactionReesult = getTransactionResult(transaction);
    if transactionReesult["status"] == "FAILED" then
        return transactionReesult;
    end;

    result["status"] = "SUCCESS";
    result["error"] = nil;
    local replay = transactionReesult["trans_replay"];
    logger.writeToLog(this, "\nREPLAY FOR TRANSACTION FROM CACHE: " .. jsonParser: encode_pretty(replay) .. "\n");

    logger.writeToLog(this, "MERGING TRANSACTION AND TRANSACTION REPLAY WITH PRIORITY TO REPLAY\n");
    local mergedReplay = {};
    local lowerCaseFieldName;
    for index, fieldName in ipairs(TRANSACTION_FIELDS) do
        lowerCaseFieldName = string.lower(fieldName);
        if replay[lowerCaseFieldName] ~= nil then
            mergedReplay[fieldName] = replay[lowerCaseFieldName];
        elseif transaction[fieldName] ~= nil then
            mergedReplay[fieldName] = transaction[fieldName];
        end;
    end;

    mergedReplay["type"] = "Transaction";
    result["transReplay"] = mergedReplay;
    logger.writeToLog(this, "\nFINAL TRANSACTION REPLAY: " .. jsonParser: encode_pretty(result) .. "\n");
    return result;
end;



-- End of CacheManager module
return QuikDataManager;