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

local jsonParser = assert( require "modules.JSON" );


---------------------------------------------------------------------------------------
-- Module variables
--
---------------------------------------------------------------------------------------

-- State of connection from QUIK terminal to broker server
local isTerminalConnected = false;


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
    response["sendingTimeOfRequestAtClient"]         =   request["time"];
    response["timeOfReceiptOfRequestAtServer"]       =   os.time();
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
-- Process GET request from pipe's client
--
---------------------------------------------------------------------------------------
local function processGET(request)

    local subject = request["subject"];
    if subject == "ECHO" then
        return getECHOResponse(request);
    else
        --logger.log("UNKNOWN SUBJECT OF REQUEST" .. jsonParser: encode_pretty(request));
    end;

end;



---------------------------------------------------------------------------------------
-- Process POST request from pipe's client
--
---------------------------------------------------------------------------------------
local function processPOST(request)
    return nil;
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