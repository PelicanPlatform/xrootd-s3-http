/***************************************************************
 *
 * Copyright (C) 2024, Pelican Project, Morgridge Institute for Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ***************************************************************/

#pragma once

#include <string>

class XrdOucStream;
class XrdSysError;

namespace XrdHTTPServer {

enum LogMask {
	Debug = 0x01,
	Info = 0x02,
	Warning = 0x04,
	Error = 0x08,
	All = 0xff
};

// Given a bitset based on LogMask, return a human-readable string of the set
// logging levels.
std::string LogMaskToString(int mask);

// Given an xrootd configuration object that matched on httpserver.trace, parse
// the remainder of the line and configure the logger appropriately.
bool ConfigLog(XrdOucStream &conf, XrdSysError &log);

} // namespace XrdHTTPServer
