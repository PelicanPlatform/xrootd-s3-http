/***************************************************************
 *
 * Copyright (C) 2025, Pelican Project, Morgridge Institute for Research
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

#include "DeadlockAcc.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSys/XrdSysError.hh>
#include <XrdVersion.hh>

#include <dlfcn.h>
#include <cstring>

DeadlockAcc::DeadlockAcc(XrdAccAuthorize *auth,
						 std::unique_ptr<XrdSysError> log,
						 const char *configName)
	: m_auth(auth), m_log(std::move(log)) {
	// Initialize the deadlock detector
	auto &detector = DeadlockDetector::GetInstance();
	if (!detector.Initialize(m_log.get(), configName)) {
		m_log->Emsg("DeadlockAcc",
					"Failed to initialize deadlock detector, continuing "
					"without deadlock detection");
	}
}

DeadlockAcc::~DeadlockAcc() { delete m_auth; }

XrdAccPrivs DeadlockAcc::Access(const XrdSecEntity *Entity, const char *path,
								const Access_Operation oper,
								XrdAccPrivCaps *caps) {
	DeadlockMonitor monitor("Access");
	return m_auth->Access(Entity, path, oper, caps);
}

int DeadlockAcc::Audit(const int accok, const XrdSecEntity *Entity,
					   const char *path, const Access_Operation oper,
					   XrdOucEnv *Env) {
	DeadlockMonitor monitor("Audit");
	return m_auth->Audit(accok, Entity, path, oper, Env);
}

int DeadlockAcc::Test(const XrdSecEntity *Entity, const char *path,
					  const Access_Operation oper, XrdOucEnv *Env) {
	DeadlockMonitor monitor("Test");
	return m_auth->Test(Entity, path, oper, Env);
}

extern "C" {

XrdVERSIONINFO(XrdAccAuthorizeObject, DeadlockAcc);

XrdAccAuthorize *XrdAccAuthorizeObject(XrdSysLogger *logger,
									   const char *config_fn,
									   const char *parms) {
	XrdSysError eDest(logger, "deadlock_acc_");

	// The parms should contain the path to the wrapped authorization library
	if (!parms || !*parms) {
		eDest.Emsg("Initialize",
				   "DeadlockAcc requires a wrapped authorization plugin. "
				   "Usage: acc.authlib libXrdAccDeadlock.so <wrapped_auth_lib>");
		return nullptr;
	}

	// Load the wrapped authorization plugin
	XrdAccAuthorize *wrapped_auth = nullptr;
	
	// Try to get the wrapped plugin using XrdAccAuthorizeOject
	// We need to use XrdSysPlugin to load the wrapped library
	std::string wrapped_lib(parms);
	
	// Extract just the library name from parms (first token)
	size_t space_pos = wrapped_lib.find(' ');
	if (space_pos != std::string::npos) {
		wrapped_lib = wrapped_lib.substr(0, space_pos);
	}
	
	// For now, we'll use a simple approach: try to dlopen the wrapped plugin
	// and get its XrdAccAuthorizeObject function
	void *lib_handle = dlopen(wrapped_lib.c_str(), RTLD_NOW | RTLD_LOCAL);
	if (!lib_handle) {
		eDest.Emsg("Initialize", "Failed to load wrapped auth plugin:",
				   wrapped_lib.c_str(), dlerror());
		return nullptr;
	}
	
	// Get the plugin entry point
	typedef XrdAccAuthorize *(*AuthObjFunc)(XrdSysLogger *, const char *,
											const char *);
	AuthObjFunc authObj = (AuthObjFunc)dlsym(lib_handle, "XrdAccAuthorizeObject");
	if (!authObj) {
		eDest.Emsg("Initialize",
				   "Failed to find XrdAccAuthorizeObject in wrapped plugin:",
				   wrapped_lib.c_str());
		dlclose(lib_handle);
		return nullptr;
	}
	
	// Call the wrapped plugin's initialization
	// Pass remaining parameters (if any) after the library name
	const char *wrapped_parms = nullptr;
	if (space_pos != std::string::npos && space_pos + 1 < strlen(parms)) {
		wrapped_parms = parms + space_pos + 1;
	}
	
	wrapped_auth = authObj(logger, config_fn, wrapped_parms);
	if (!wrapped_auth) {
		eDest.Emsg("Initialize", "Wrapped authorization plugin failed to initialize");
		dlclose(lib_handle);
		return nullptr;
	}
	
	// Create our wrapper with the wrapped plugin
	std::unique_ptr<XrdSysError> log(new XrdSysError(logger, "deadlock_acc_"));
	return new DeadlockAcc(wrapped_auth, std::move(log), config_fn);
}
}
