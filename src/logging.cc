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

#include "logging.hh"

#include <XrdOuc/XrdOucStream.hh>
#include <XrdSys/XrdSysError.hh>

#include <sstream>

using namespace XrdHTTPServer;

std::string
XrdHTTPServer::LogMaskToString(int mask) {
    if (mask == LogMask::All) {return "all";}
        
    bool has_entry = false;
    std::stringstream ss;
    if (mask & LogMask::Debug) {
        ss << "debug"; 
        has_entry = true;
    }           
    if (mask & LogMask::Info) {
        ss << (has_entry ? ", " : "") << "info";
        has_entry = true;
    }       
    if (mask & LogMask::Warning) {
        ss << (has_entry ? ", " : "") << "warning";
        has_entry = true;
    }   
    if (mask & LogMask::Error) {
        ss << (has_entry ? ", " : "") << "error";
        has_entry = true;
    }   
    return ss.str();
}

bool
XrdHTTPServer::ConfigLog(XrdOucStream &conf, XrdSysError &log) {
        std::string map_filename;
        log.setMsgMask(0);
        char *val = nullptr;
        if (!(val = conf.GetToken())) {
            log.Emsg("Config", "httpserver.trace requires an argument.  Usage: httpserver.trace [all|error|warning|info|debug|none]");
            return false;
        }
        do {
            if (!strcmp(val, "all")) {log.setMsgMask(log.getMsgMask() | LogMask::All);}
            else if (!strcmp(val, "error")) {log.setMsgMask(log.getMsgMask() | LogMask::Error);}
            else if (!strcmp(val, "warning")) {log.setMsgMask(log.getMsgMask() | LogMask::Warning);}
            else if (!strcmp(val, "info")) {log.setMsgMask(log.getMsgMask() | LogMask::Info);}
            else if (!strcmp(val, "debug")) {log.setMsgMask(log.getMsgMask() | LogMask::Debug);}
            else if (!strcmp(val, "none")) {log.setMsgMask(0);}
            else {log.Emsg("Config", "scitokens.trace encountered an unknown directive:", val); return false;}
        } while ((val = conf.GetToken()));
        log.Emsg("Config", "Logging levels enabled -", XrdHTTPServer::LogMaskToString(log.getMsgMask()).c_str());
    return true;
}
