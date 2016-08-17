---------------------------------------------------------------------------------------
-- ServerModule.lua
-- Author: Aleksey Kutergin <aleksey.v.kutergin@gmail.com>
-- Company: rusquant.ru
-- Date: 10.08.2016
---------------------------------------------------------------------------------------
-- This module contains all pure server logic like connect, disconnect, sendMessage,
-- receiveMessage and so on without QUIK-specific code
---------------------------------------------------------------------------------------

-- @module src.modules.ServerModule

local ServerModule = {}; -- public interface



---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
-- Definitions for local variables of the module
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------

-- Important!
-- A word of advice: \\ , \p and \t are going to get interpreted as escape characters. Either switch to forward slashes or double-backslash to "escape the escapes": "\\\\.\\pipe\\test"
local PIPE_NAME = "\\\\.\\pipe\\RusquantToQuikPipe";

---------------------------------------------------------------------------------------
-- Pipe open modes
---------------------------------------------------------------------------------------


-- Specifies pipe to be bi-directional. Both server and client processes can read from and write to pipe
local PIPE_ACCESS_DUPLEX = 0x00000003;

-- This flag forbids multiple instances of the pipe. If you try to create multiple instances of the pipe with this flag,
-- creation of first instance succeeds, but creation of next instance fails with ERROR_ACCES_DENIDED
local FILE_FLAG_FIRST_PIPE_INSTANCE = 0x00080000;

-- This is very inportant flag!!!
-- It healps to avoid deadlock while client and server read from  or write to the same pipe simultaneously
-- This flag enebles overlapped mode for pipe:
-- 1. Fundtion performing read\write and connect operations, which may take a significant time to be completed, can return immediately
-- 2. Thread that started operation to perform other operations while time-consuming operation exectutes in background:
--    2.1. In overlapped mode thread can handle simultaneous IO operations on multiple instances of pipe
--    2.2. In overlapped mode thread can perform simulteneous read\write operations on the same pipe handle
-- 3. If the overlapped mode is not enabled, functions performing read, write and connect operations on the pipe handle do not return until the operation is finished
local FILE_FLAG_OVERLAPPED = 0x40000000;

-- Data is written to the pipe as steram of messages.
-- Iportant!!!
-- Pipe treats all bites, written during each write operation, as single message unit.
-- This results in:
-- The GetLastError() function reaturns ERROR_MORE_DATA when the messages not read completely.
local PIPE_TYPE_MESSAGE = 0x00000004;

-- Data is read from the pipe as stream of messages.
local PIPE_READMODE_MESSAGE = 0x00000002;

-- Enables blocking mode.
-- When pipe handle is pecified in ReadFile, WriteFile or ConnectNamedPipe functions, the operations are not completed until there is data to read, all data is written, or a client is connected
local PIPE_WAIT = 0x00000000;

-- Enables unblocking mode.
-- In this mode ReadFile, WriteFile and CinnectNamedPipe always return immediately
local PIPE_NOWAIT = 0x00000001;

-- Connections from remote clients are automatically rejected
local PIPE_REJECT_REMOTE_CLIENTS = 0x00000008;


---------------------------------------------------------------------------------------
-- Other CreateNamedPipe params
---------------------------------------------------------------------------------------

-- The max number of instances that can be created for this pipe
local MAX_NUM_OF_INSTANCES = 1;

-- The number of bytes to reserve for output buffer
local OUT_BUFFER_SIZE = 4 * 1024;

-- The number of bytes to reserve for input buffer
local IN_BUFFER_SIZE = 4 * 1024;

-- The default time-out in milliseconds. Zero means time-out in 50 ms.
local DEFAULT_TIME_OUT = 0;

-- The pointer to the structure, that specify security descripto for new named pipe and determine whether child process can inherite from return handle
-- nil means defaults
local SCURITTY_ATTRIBUTES; -- = nil


---------------------------------------------------------------------------------------
-- Pipe ERRORS and ERROR CODES
---------------------------------------------------------------------------------------

local INVALID_HANDLE_VALUE;
local ERROR_PIPE_CONNECTED = 535;
local STATUS_PENDING = 259;
local ERROR_IO_PENDING = 997;
local ERROR_NO_DATA = 232;
local ERROR_PIPE_LISTENING = 536;

---------------------------------------------------------------------------------------
-- Utility variables
---------------------------------------------------------------------------------------

-- Lua to C interface
local ffiLib = assert( require "ffi" );

-- Access to WinAPI
local dllLibs = {};

-- Storing descriptor for pipe
local pipeHandle;

-- OVERLAPPED structure
local lpOverlapped;

local isServerInitSuccess = false;
local isServerConnected = false;
local isServerStoped = false;


---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
-- Definitions for all private functions of the module
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------
-- Create Windows Named Pipe with given name
-- @return:
-- true - if success
-- false - if fail
---------------------------------------------------------------------------------------
local function createPipe()
    local pipeOpenMode = PIPE_ACCESS_DUPLEX + FILE_FLAG_OVERLAPPED + FILE_FLAG_FIRST_PIPE_INSTANCE;
    local pipeMode = PIPE_TYPE_MESSAGE + PIPE_READMODE_MESSAGE + PIPE_REJECT_REMOTE_CLIENTS;

    pipeHandle = assert( ffiLib.C.CreateNamedPipeA(PIPE_NAME, pipeOpenMode, pipeMode, MAX_NUM_OF_INSTANCES, OUT_BUFFER_SIZE, IN_BUFFER_SIZE, DEFAULT_TIME_OUT, SCURITTY_ATTRIBUTES) );
    if pipeHandle == INVALID_HANDLE_VALUE then
        -- logger.log();
        ffiLib.C.MessageBoxA(nil, "PIPE " + PIPE_NAME + "CREATION FAILED WITH ERROR: " + ffiLib.C.GetLastError(), "ERROR", 0);
        return false;
    else
        return true;
    end;
end;


---------------------------------------------------------------------------------------
-- Closes pipe handle
-- @return:
-- 0 - If the function fails
-- non-zero value - If the function succeeds
---------------------------------------------------------------------------------------
local function closeHandle()
    local result = ffiLib.C.CloseHandle(pipeHandle);
    if result == 0 then
        ffiLib.C.MessageBoxA(nil, "PIPE " + PIPE_NAME + "CLOSING OF HANDLE FAILED WITH ERROR: " + ffiLib.C.GetLastError(), "ERROR", 0);
        -- logger.log();
        return false;
    else
        return true;
    end;
end;


---------------------------------------------------------------------------------------
-- Waits FOREVER for incomming client connection
-- So the server can be stoped only by triggering of OnStop() event in QUIK or aborting  the script execution
--
---------------------------------------------------------------------------------------
local function waitForClientConnection()
    -- local countter = 0;

    while true do
          if lpOverlapped.Internal ~= STATUS_PENDING or isServerStoped == true then
                break;
          end;

          -- print "Waiting for client's connection";
          -- countter = countter + 1;
    end;
end;



---------------------------------------------------------------------------------------
-- Check the Client to Server connection after the status of asynchroniouse connect operation changed from STATUS_PENDING
-- According to Microsoft docs (See last sentense of the last paragraf of the Remarks section), the good connction between Server and Client process
-- only exits if the second call of ConnectNamedPipe() for
-- @return:
-- true - if connection is OK
-- false - else
---------------------------------------------------------------------------------------
local function checkConnection()
    -- According to docs, a new instance of OVERLAPPED structure has to be used for each asynchroniouse call
    local overaptStruct = ffiLib.new("OVERLAPPED");

    local immediateConnectResult = ffiLib.C.ConnectNamedPipe(pipeHandle, overaptStruct);
    local lastError = ffiLib.C.GetLastError();

    if immediateConnectResult == 0 and lastError == ERROR_PIPE_CONNECTED then
        return true;
    else
        return false;
    end;
end;



---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
-- Definitions for all public functions of the module
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------



---------------------------------------------------------------------------------------
-- Init all server internals
-- __cdecl - it is just calling convention for C-language
-- __cdecl mneas, that args of function are pushed into the stack in reverse order and call stack is cleaned by caller
---------------------------------------------------------------------------------------
function ServerModule : init()
    dllLibs.__cdecl = assert(ffiLib.load("kernel32"));

    ffiLib.cdef[[
                    typedef unsigned long ULONG_PTR;
                    typedef unsigned long DWORD;
                    typedef DWORD * LPDWORD;
                    typedef void * PVOID;
                    typedef PVOID HANDLE;
                    typedef struct
                    {
                        ULONG_PTR Internal;
                        ULONG_PTR InternalHigh;
                        union
                        {
                            struct
                            {
                                  DWORD Offset;
                                  DWORD OffsetHigh;
                            };
                            PVOID Pointer;
                        };
                        HANDLE hEvent;
                    } OVERLAPPED;
                    int CreateNamedPipeA(const char *name, int openMode, int pipeMode, int maxInstances, int outBufferSize, int inBufferSize, int defTimeout, void *security);
                    int ConnectNamedPipe(HANDLE, OVERLAPPED*);
                    bool GetOverlappedResult(HANDLE hFile, OVERLAPPED * lpOverlapped, LPDWORD lpNumberOfBytesTransferred, bool bWait);
                    int FlushFileBuffers(HANDLE hFile);
                    int DisconnectNamedPipe(HANDLE);
                    int CloseHandle(HANDLE hObject);
                    int GetLastError();
                    int MessageBoxA(void *w, const char *txt, const char *cap, int type);
    ]];

    INVALID_HANDLE_VALUE = assert( ffiLib.cast("void*", -1) );
    lpOverlapped = ffiLib.new("OVERLAPPED");
    isServerInitSuccess = createPipe();
end;



---------------------------------------------------------------------------------------
-- Waits for client to connect
-- @return:
-- true - if success
-- false - if fail
---------------------------------------------------------------------------------------
function ServerModule : connect()

        print "Within connect";
        local immediateConnectResult = assert( ffiLib.C.ConnectNamedPipe(pipeHandle, lpOverlapped) );
        if immediateConnectResult ~= 0 then
            print "Within immediateConnectResult equals to zero";
            -- This means that instance of pipe is ready for new client connection:
            -- 1. First call of ConnectNamedPipe() for this instance of pipe
            -- 2. Or pipe descriptor has been cleaned by call of DisconnectNamedPipe()
            waitForClientConnection();
            --isServerConnected = checkConnection();

        else
            print "Within immediateConnectResult not equals to zero";
            -- This means that we have troubles with pipe's descriptor and we need to analize GetLastError() result.
            local lastError = ffiLib.C.GetLastError();
            print(lastError);
            waitForClientConnection();
            if lastError == ERROR_NO_DATA or lastError == ERROR_PIPE_LISTENING then

            elseif lastError == ERROR_PIPE_CONNECTED then
                -- Nothing to to the connection is good
            end;

        end;

end;



---------------------------------------------------------------------------------------
-- Breaks the connection with pipe
-- @return:
-- true - if success
-- false - if fail
---------------------------------------------------------------------------------------
function ServerModule : disconnet()
    -- 1. Send warn to client
    -- 2. Wait for a response on how the message is received.

    -- 3. Flush all data buffers
    ffiLib.C.FlushFileBuffers(pipeHandle);

    -- 4. Clear pipe's handle by call of DisconnectNamedPipe()
    ffiLib.C.DisconnectNamedPipe(pipeHandle);
end;


---------------------------------------------------------------------------------------
-- Executes server main loop
--
---------------------------------------------------------------------------------------
function ServerModule : run()

    if isServerInitSuccess == true then

        -- 1. Wait for client's connection
        ServerModule.connect();


    end;

    --while isServerStoped ~= true do

    --end; -- End of server main loop

end;


---------------------------------------------------------------------------------------
-- Shutdowns the server
--
---------------------------------------------------------------------------------------
function ServerModule : stop()
    -- 1. Disconnect client
    -- 2. Close handle
    isServerStoped = true;
    closeHandle();
end;



---------------------------------------------------------------------------------------
-- Access methods
--
---------------------------------------------------------------------------------------

function ServerModule : isServerInitSuccess()
    return isServerInitSuccess;
end;


function ServerModule : isSeverConnected()
    return isServerConnected;
end

-- End of the module
return ServerModule;