local jsonParser = assert( require "modules.JSON" );

-- local val = jsonParser: decode('{ "what": "books", "count": 3 }');
local request = jsonParser: decode('{"id":2,"time":1475240545207,"type":"GET","subject":"ECHO","body":{"echoMessage":"RUSQUANT TEST MESSAGE: 1"}}');


--local rawJson = jsonParser: encode_pretty(val);

--print(rawJson)

-- request processing:

--


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
    response["sendingTimeOfResponseAtClient"]       =   os.time();
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
        responseBody["echoMessage"] = "@ECHO: " .. reuqestBody["echoMessage"];
        response["body"] = responseBody;
    else
        response["status"] = "FAILED";
        response["error"] = "INVALID REQUEST PARAMETERS. ECHO REQUEST MUST CONTAIN NOT NULL echoMessage PARAMETER!";
    end;

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



function processRequest(request)

    if validateRequest(request) then

        local type = request["type"];
        if type == "GET" then
            return processGET(request);
        elseif type == "POST" then
            return processPOST(request);
        else
            --logger.log("UNKNOWN TYPE OF REQUEST" .. jsonParser: encode_pretty(request));
            return nil;
        end;

    end;

end;


local response = processRequest(request);
local rawJson = jsonParser: encode_pretty(response);
print(rawJson);