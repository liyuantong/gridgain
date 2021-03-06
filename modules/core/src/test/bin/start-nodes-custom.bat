::
:: Copyright 2019 GridGain Systems, Inc. and Contributors.
:: 
:: Licensed under the GridGain Community Edition License (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
:: 
::     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
:: 
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::
set SCRIPT_DIR=%~dp0

if %SCRIPT_DIR:~-1,1% == \ set SCRIPT_DIR=%SCRIPT_DIR:~0,-1%

:: -np option is mandatory, if it is not provided then we will wait for a user input.
call "%SCRIPT_DIR%\..\..\..\..\..\bin\ignite.bat" -v -np modules\core\src\test\config\spring-start-nodes-attr.xml
