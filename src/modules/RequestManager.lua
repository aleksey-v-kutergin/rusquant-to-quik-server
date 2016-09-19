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
-- Process the request from pipe's client
--
---------------------------------------------------------------------------------------
function RequestManager : processRequest(request)

    return "@ECHO: " .. request;

end;


---------------------------------------------------------------------------------------
-- Process the request from pipe's client
--
---------------------------------------------------------------------------------------


-- End of RequestManager module
return RequestManager;