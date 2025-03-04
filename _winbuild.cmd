@echo off

setlocal enableextensions enableDelayedExpansion

SET ERLANG_OTP_VERSION=24.1.7
SET PATH=C:\Erlang\erl-%ERLANG_OTP_VERSION%\bin;%PATH%

call escript rebar get-deps compile

endlocal
