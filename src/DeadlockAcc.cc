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

	// We need to load the wrapped authorization plugin
	// For now, we'll return nullptr if no wrapped plugin is configured
	// In practice, this would parse configuration to determine which auth
	// plugin to wrap
	eDest.Emsg("Initialize",
			   "DeadlockAcc requires a wrapped authorization plugin to be "
			   "configured");
	return nullptr;
}
}
