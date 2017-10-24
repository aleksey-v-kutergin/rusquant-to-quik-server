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
    local reuqestBody = request["body"];

    if reuqestBody["echoMessage"] ~= nil then
        response["status"] = "SUCCESS";

        local responseBody = {};
        responseBody["type"] = "EchoResponseBody";

        local echo = {};
        echo["type"] = "Echo";
        echo["echoAnswer"] = "@ECHO: " .. reuqestBody["echoMessage"];

        responseBody["echo"] = echo;
        response["body"] = responseBody;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. ECHO REQUEST MUST CONTAIN NOT NULL echoMessage PARAMETER!";
    end;

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
    local reuqestBody = request["body"];

    if reuqestBody["infoParameterName"] ~= nil then
        response["status"] = "SUCCESS";

        local responseBody = {};
        responseBody["type"] = "InfoParameterResponseBody";

        local info = {};
        info["type"] = "InfoParameter";
        info["parameterName"] = reuqestBody["infoParameterName"];

        local value = getInfoParam(reuqestBody["infoParameterName"]);
        if value == "" then
            info["parameterValue"] = "NA";
        else
            info["parameterValue"] = value;
        end;

        responseBody["infoParameter"] = info;
        response["body"] = responseBody;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. INFO PARAMETER REQUEST MUST CONTAIN NOT NULL NAME OF THE PARAMETER!";
    end;

    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Constructs response for send transaction request.
--
---------------------------------------------------------------------------------------
local function getTransactionResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request["body"];

    local transaction = reuqestBody["transaction"];
    if transaction ~= nil then

        local responseBody = {};
        responseBody["type"] = "TransactionResponseBody";

        local result = quikDataManager.sendTransaction(this, transaction);
        if result["status"] ~= "FAILED" then
            response["status"] = "SUCCESS";
            responseBody["transactionReplay"] = result["transReplay"];
        else
            response["status"] = "FAILED";
            response["error"] = result["error"];
        end;
        response["body"] = responseBody;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. TRANSACTION CANNOT BE NULL!";
    end;

    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Constructs response for get order request.
--
---------------------------------------------------------------------------------------
local function getOrderResponse(request)

    local response = getCommonResponsePart(request);
    local reuqestBody = request["body"];

    if reuqestBody["orderNumber"] ~= nil then
        response["status"] = "SUCCESS";

        local responseBody = {};
        responseBody["type"] = "OrderResponseBody";

        local result = quikDataManager.getOrder(this, reuqestBody["orderNumber"], false);
        if result["status"] ~= "FAILED" then
            response["status"] = "SUCCESS";
            responseBody["order"] = result["order"];
        else
            response["status"] = "FAILED";
            response["error"] = result["error"];
        end;
        response["body"] = responseBody;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. ORDER NUMBER CANNOT BE NULL!";
    end;

    response["sendingTimeOfResponseAtServer"] = os.time();
    return response;

end;


---------------------------------------------------------------------------------------
-- Process GET request from pipe's client
--
---------------------------------------------------------------------------------------
local function processGET(request)

    local subject = request["subject"];
    if subject == "ECHO" then
        return getECHOResponse(request);
    elseif subject == "CONNECTION_SATE" then
        return getConnectionStateResponse(request);
    elseif subject == "INFO_PARAMETER" then
        return getInfoParameterResponse(request);
    elseif subject == "ORDER" then
        return getOrderResponse(request);
    else
        --logger.log("UNKNOWN SUBJECT OF REQUEST" .. jsonParser: encode_pretty(request));
    end;

end;



---------------------------------------------------------------------------------------
-- Process POST request from pipe's client
--
---------------------------------------------------------------------------------------
local function processPOST(request)
    local subject = request["subject"];
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

        local type = request["type"];
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