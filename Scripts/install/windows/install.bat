@echo off
@REM
@REM Copyright (c) Microsoft. All rights reserved.
@REM Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
@REM

setlocal
set allArgs=%*
set installBatchDir=%~dp0
set psInstall=%installBatchDir%ps\install.ps1
set PSModulePath=%PSModulePath%;%installBatchDir%ps\Modules
:loop
if /I "%~1"=="-h" goto :helpMsg
if /I "%~1"=="/h" goto :helpMsg
if /I "%~1"=="-help" goto :helpMsg
if /I "%~1"=="/help" goto :helpMsg
if "%~1"=="-?" goto :helpMsg
if "%~1"=="/?" goto :helpMsg
shift

if not "%~1"=="" goto loop

powershell -NoProfile -NoLogo -Command "Get-ChildItem '%installBatchDir%' -Include *.psm1,*.ps1 -recurse -ErrorAction Stop | unblock-file -ErrorAction Stop"
if ERRORLEVEL 1 goto errorUnblock

powershell -NoProfile -NoLogo -ExecutionPolicy RemoteSigned %psInstall% %allArgs%
if ERRORLEVEL 1 echo Installer reported an error!!
goto finish


:errorUnblock
@echo. 
@echo. Error encountered during UNBLOCK-FILE operartion
goto finish

:helpMsg
@echo.
@echo. CNTK Binary Install Script
@echo.
@echo More help can be found at: 
@echo     https://github.com/Microsoft/CNTK/wiki/Setup-Windows-Binary-Script
@echo.
@echo An overview of the available command line options can be found at:
@echo     https://github.com/Microsoft/CNTK/wiki/Setup-Windows-Binary-Script-Options
@echo.

:finish
endlocal
