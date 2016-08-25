------------------------------------------------------------------------------
-- RusquantToQuikServer.lua
-- Top level project module
-- Author: Aleksey Kutergin <aleksey.v.kutergin@gmail.com>
-- Company: rusquant.ru
-- Date: 09.08.2016
------------------------------------------------------------------------------
-- This module is a king of wrapper for all remaining code
-- It glues all the pieces into single program
------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-- REQUIRE section
--
---------------------------------------------------------------------------------------

local serverModule = assert( require "modules.ServerModule" );

---------------------------------------------------------------------------------------
-- Test code
---------------------------------------------------------------------------------------

--serverModule.init();
--serverModule.connect();
--serverModule.stop();


---------------------------------------------------------------------------------------
-- QUIK Terminal calls this function before main.
-- Therefore, it is logical to perform all init operations here
---------------------------------------------------------------------------------------
function OnInit()
    serverModule.init();
end;



---------------------------------------------------------------------------------------
-- Entry point to qlua script execution process under QUIK terminal
-- QUIK executes main() in separate thread
--
---------------------------------------------------------------------------------------
function main()
    serverModule.run();
    serverModule.stop();
end;


---------------------------------------------------------------------------------------
-- Callback for Stop sript execution event
--
---------------------------------------------------------------------------------------
function OnStop()
    serverModule.stop();
end;
