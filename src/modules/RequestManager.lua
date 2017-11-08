---------------------------------------------------------------------------------------
-- RequestManager.lua
-- Author: Aleksey Kutergin <aleksey.v.kutergin@gmail.com>
-- Company: rusquant.ru
-- Date: 13.09.2016
---------------------------------------------------------------------------------------
-- This module contains logic for request's processing
--
---------------------------------------------------------------------------------------

-- src.modules.RequestManager
local RequestManager = {};

---------------------------------------------------------------------------------------
-- Module variables
--
---------------------------------------------------------------------------------------

-- State of connection from QUIK terminal to broker server
local isTerminalConnected = false;


local logger;
local jsonParser;
local quikDataManager;

function RequestManager : setLogger(externalLogger)
    logger = externalLogger;
    logger.writeToLog(this, "LOGGER WITHIN REQUEST MANAGER");
end;

function RequestManager : setJsonParser(parser)
    jsonParser = parser;
    logger.writeToLog(this, "JSON PASER WITHIN REQUEST MANAGER!");
end;

function RequestManager : setDataManager(manager)
    quikDataManager = manager;
    quikDataManager.setJsonParser(this, jsonParser);
end;

---------------------------------------------------------------------------------------
-- Incoming request validation
--
---------------------------------------------------------------------------------------
local function validateRequest(request)

    local targetCount = 0;
    for key, value in pairs(request) do
        if ( key == "type" or key == "subject" or key == "id" or key == "time" or key == "body" ) and value ~= nil then
            targetCount = targetCount + 1;
        end;
    end;

    if targetCount == 5 then return true end;
    return false;

end;


---------------------------------------------------------------------------------------
-- Constructs blank for response (Constructs part, common for all ).
--
---------------------------------------------------------------------------------------
local function getCommonResponsePart(request)

    local response = {};
    response["requestId"]                           =   request["id"];
    response["sendingTimeOfRequestAtClient"]        =   request["time"];
    response["timeOfReceiptOfRequestAtServer"]      =   os.time();
    response["sendingTimeOfResponseAtServer"]       =   os.time();
    response["timeOfReceiptOfResponseAtClient"]     =   os.time();
    response["type"]                                =   request["type"];
    response["subject"]                             =   request["subject"];
    response["status"]                              =   "EMPTY_RESPONSE";
    response["error"]                               =   "NO_ERROR";
    response["body"]                                =   {};
    return response;

end;



---------------------------------------------------------------------------------------
-- Constructs response for ECHO request.
--
---------------------------------------------------------------------------------------
local function getECHOResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request.body;

    local responseBody = {};
    responseBody["type"] = "EchoResponseBody";

    if reuqestBody.echoMessage ~= nil then
        response["status"] = "SUCCESS";

        local echo = {};
        echo["type"] = "Echo";
        echo["echoAnswer"] = "@ECHO: " .. reuqestBody.echoMessage;
        responseBody["echo"] = echo;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. ECHO REQUEST MUST CONTAIN NOT NULL echoMessage PARAMETER!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;



---------------------------------------------------------------------------------------
-- Constructs response for connection state of the terminal to QUIK-server request.
--
---------------------------------------------------------------------------------------
local function getConnectionStateResponse(request)

    local response = getCommonResponsePart(request);
    response["status"] = "SUCCESS";

    local responseBody = {};
    responseBody["type"] = "ConnectionSateResponseBody";

    local connectionState = {};
    connectionState["type"] = "ConnectionState";
    connectionState["isConnected"] = isConnected();
    responseBody["connectionState"] = connectionState;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;



---------------------------------------------------------------------------------------
-- Constructs response for request of the info about terminal.
--
---------------------------------------------------------------------------------------
local function getInfoParameterResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request.body;

    local responseBody = {};
    responseBody["type"] = "InfoParameterResponseBody";

    if reuqestBody.infoParameterName ~= nil then
        response["status"] = "SUCCESS";

        local info = {};
        info["type"] = "InfoParameter";
        info["parameterName"] = reuqestBody.infoParameterName;

        local value = getInfoParam(reuqestBody.infoParameterName);
        if value == "" then
            info["parameterValue"] = "NA";
        else
            info["parameterValue"] = value;
        end;
        responseBody["infoParameter"] = info;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. INFO PARAMETER REQUEST MUST CONTAIN NOT NULL NAME OF THE PARAMETER!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Constructs response for send transaction request.
--
---------------------------------------------------------------------------------------
local function getTransactionResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request.body;

    local responseBody = {};
    responseBody["type"] = "TransactionResponseBody";

    local transaction = reuqestBody.transaction;
    if transaction ~= nil then

        local result = quikDataManager.sendTransaction(this, transaction);
        if result.status ~= "FAILED" then
            response["status"] = "SUCCESS";
            responseBody["transactionReplay"] = result.transReplay;
        else
            response["status"] = "FAILED";
            response["error"] = result.error;
        end;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. TRANSACTION CANNOT BE NULL!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Constructs response for get order request.
--
---------------------------------------------------------------------------------------
local function getOrderResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request.body;

    local responseBody = {};
    responseBody["type"] = "OrderResponseBody";

    if reuqestBody.orderNumber ~= nil then
        local result = quikDataManager.getOrder(this, reuqestBody.orderNumber, true);
        if result.status ~= "FAILED" then
            response["status"] = "SUCCESS";
            responseBody["order"] = result.order;
        else
            response["status"] = "FAILED";
            response["error"] = result.error;
        end;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. ORDER NUMBER CANNOT BE NULL!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Constructs response for get trades request.
--
---------------------------------------------------------------------------------------
local function getTradesResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request.body;

    local responseBody = {};
    responseBody["type"] = "TradesResponseBody";

    if reuqestBody.orderNumber ~= nil then
        local result = quikDataManager.getTrades(this, reuqestBody.orderNumber);
        if result.status ~= "FAILED" then
            response["status"] = "SUCCESS";
            responseBody["tradesDataFrame"] = result.tradesDataFrame;
        else
            response["status"] = "FAILED";
            response["error"] = result.error;
        end;

    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. ORDER NUMBER CANNOT BE NULL!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Constructs response for request of the info about quik table.
--
---------------------------------------------------------------------------------------
local function getTableInfoResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request.body;

    local responseBody = {};
    responseBody["type"] = "QuikTableInfoResponseBody";

    if reuqestBody.tableType ~= nil then
        response["status"] = "SUCCESS";

        local tableInfo = {};
        tableInfo["type"] = "QuikTableInfo";
        tableInfo["tableType"] = reuqestBody.tableType;
        tableInfo["rowsCount"] =  getNumberOf(reuqestBody.tableType);

        responseBody["tableInfo"] = tableInfo;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. INFO PARAMETER REQUEST MUST CONTAIN NOT NULL NAME OF THE PARAMETER!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Constructs response for request for item of quik table.
--
---------------------------------------------------------------------------------------
local function getTableItemResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request.body;

    local responseBody = {};
    responseBody["type"] = "QuikTableItemResponseBody";
    if reuqestBody.tableType ~= nil and reuqestBody.itemIndex ~= nil then
        local result = quikDataManager.getTableItem(this, reuqestBody.tableType, reuqestBody.itemIndex);
        if result.status ~= "FAILED" then
            response["status"] = "SUCCESS";
            responseBody["item"] = result.item;
        else
            response["status"] = "FAILED";
            response["error"] = result.error;
        end;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. INFO PARAMETER REQUEST MUST CONTAIN NOT NULL NAME OF THE PARAMETER!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Constructs response for request for item of quik table.
--
---------------------------------------------------------------------------------------
local function getTableItemsResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request.body;

    local responseBody = {};
    responseBody["type"] = "QuikTableItemsResponseBody";

    if reuqestBody.tableType ~= nil then
        local result = quikDataManager.getTableItems(this, reuqestBody.tableType);
        if result.status ~= "FAILED" then
            response["status"] = "SUCCESS";
            responseBody["items"] = result.tableItems;
        else
            response["status"] = "FAILED";
            response["error"] = result.error;
        end;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. INFO PARAMETER REQUEST MUST CONTAIN NOT NULL NAME OF THE PARAMETER!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Constructs response for request for parameter of current trading table.
--
---------------------------------------------------------------------------------------
local function getTradingParameterResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request.body;

    local responseBody = {};
    responseBody["type"] = "TradingParameterResponseBody";

    local isValid = reuqestBody.classCode ~= nil;
    isValid = isValid and reuqestBody.securityCode ~= nil;
    isValid = isValid and reuqestBody.parameter ~= nil;

    if isValid == true then
        local result = quikDataManager.getTradingParameter(this, reuqestBody.classCode,
                                                                 reuqestBody.securityCode,
                                                                 reuqestBody.parameter,
                                                                 reuqestBody.version);
        if result.status ~= "FAILED" then
            response["status"] = "SUCCESS";
            responseBody["tradingParameter"] = result.tradingParameter;
        else
            response["status"] = "FAILED";
            response["error"] = result.error;
        end;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Constructs response for tarde date request.
--
---------------------------------------------------------------------------------------
local function getTradeDateResponse(request)

    local response = getCommonResponsePart(request);
    local responseBody = {};
    responseBody["type"] = "TradeDateResponseBody";

    local tradeDate = getTradeDate();
    if tradeDate ~= nil then
        response["status"] = "SUCCESS";
        tradeDate["type"] = "TradeDate";
        responseBody["tradeDate"] = tradeDate;
    else
        response["status"] = "FAILED";
        response["error"] = "CALL OF getTradeDate() RETURN NIL VALUE!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Constructs response for tarde date request.
--
---------------------------------------------------------------------------------------
local function getSecurityInfoResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request.body;

    local responseBody = {};
    responseBody["type"] = "SecurityInfoResponseBody";

    local isValid = reuqestBody.classCode ~= nil;
    isValid = isValid and reuqestBody.securityCode ~= nil;

    if isValid == true then
        local result = quikDataManager.getSecurityInfo(this, reuqestBody.classCode, reuqestBody.securityCode);
        if result.status ~= "FAILED" then
            response["status"] = "SUCCESS";
            responseBody["security"] = result.security;
        else
            response["status"] = "FAILED";
            response["error"] = result.error;
        end;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;



local function getClassInfoResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request.body;

    local responseBody = {};
    responseBody["type"] = "SecurityClassInfoResponseBody";

    if reuqestBody.classCode ~= nil then
        local result = quikDataManager.getClassInfo(this, reuqestBody.classCode);
        if result.status ~= "FAILED" then
            response["status"] = "SUCCESS";
            responseBody["securityClass"] = result.securityClass;
        else
            response["status"] = "FAILED";
            response["error"] = result.error;
        end;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Constructs response for max count of lots in order request.
--
---------------------------------------------------------------------------------------
local function getMaxLotCountResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request.body;

    local responseBody = {};
    responseBody["type"] = "MaxCountOfLotsResponseBody";

    local isValid = reuqestBody.classCode ~= nil;
    isValid = isValid and reuqestBody.securityCode ~= nil;
    isValid = isValid and reuqestBody.clientCode ~= nil;
    isValid = isValid and reuqestBody.account ~= nil;
    isValid = isValid and reuqestBody.price ~= nil;
    isValid = isValid and reuqestBody.isBuy ~= nil;
    isValid = isValid and reuqestBody.isMarket ~= nil;

    if isValid == true then
        local result = quikDataManager.getMaxCountOfLotsInOrder(this,
                                                                reuqestBody.classCode,
                                                                reuqestBody.securityCode,
                                                                reuqestBody.clientCode,
                                                                reuqestBody.account,
                                                                reuqestBody.price,
                                                                reuqestBody.isBuy,
                                                                reuqestBody.isMarket);
        if result.status ~= "FAILED" then
            response["status"] = "SUCCESS";
            responseBody["countOfLots"] = result.countOfLots;
        else
            response["status"] = "FAILED";
            response["error"] = result.error;
        end;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS!";
    end;

    response["body"] = responseBody;
    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;

---------------------------------------------------------------------------------------
-- Process GET request from pipe's client
--
---------------------------------------------------------------------------------------
local function processGET(request)

    local subject = request.subject;
    if subject == "ECHO" then
        return getECHOResponse(request);
    elseif subject == "CONNECTION_SATE" then
        return getConnectionStateResponse(request);
    elseif subject == "INFO_PARAMETER" then
        return getInfoParameterResponse(request);
    elseif subject == "ORDER" then
        return getOrderResponse(request);
    elseif subject == "TRADE" then
        return getTradesResponse(request);
    elseif subject == "TABLE_INFO" then
        return getTableInfoResponse(request);
    elseif subject == "TABLE_ITEM" then
        return getTableItemResponse(request);
    elseif subject == "TABLE_ITEMS" then
        return getTableItemsResponse(request);
    elseif subject == "TRADING_PARAMETER" then
        return getTradingParameterResponse(request);
    elseif subject == "TRADE_DATE" then
        return getTradeDateResponse(request);
    elseif subject == "SECURITY_INFO" then
        return getSecurityInfoResponse(request);
    elseif subject == "MAX_LOT_COUNT" then
        return getMaxLotCountResponse(request);
    elseif subject == "CLASS_INFO" then
        return getClassInfoResponse(request);
    else
        --logger.log("UNKNOWN SUBJECT OF REQUEST" .. jsonParser: encode_pretty(request));
    end;

end;



---------------------------------------------------------------------------------------
-- Process POST request from pipe's client
--
---------------------------------------------------------------------------------------
local function processPOST(request)
    local subject = request.subject;
    if subject == "TRANSACTION" then
        return getTransactionResponse(request);
    else
        --logger.log("UNKNOWN SUBJECT OF REQUEST" .. jsonParser: encode_pretty(request));
    end;
end;



---------------------------------------------------------------------------------------
-- Process the request from pipe's client
--
---------------------------------------------------------------------------------------
function RequestManager : processRequest(rawJSONRequest)
    local request = jsonParser: decode(rawJSONRequest);
    local response;
    local rawJSONResponse;

    if request ~= nill and validateRequest(request) then

        local type = request.type;
        if type == "GET" then
            response = processGET(request);
        elseif type == "POST" then
            response = processPOST(request);
        else
            --logger.log("UNKNOWN TYPE OF REQUEST" .. jsonParser: encode_pretty(request));
        end;

    end;

    if response ~= nil then
        rawJSONResponse = jsonParser: encode(response);
    end;
    return rawJSONResponse;

end;



---------------------------------------------------------------------------------------
-- Process the request from pipe's client
--
---------------------------------------------------------------------------------------


function RequestManager : setQUIKToBrokerConnectionFlag(isConnected)
    isTerminalConnected = isConnected;
end;


-- End of RequestManager module
return RequestManager;