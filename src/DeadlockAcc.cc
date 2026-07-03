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

#include <exception>

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
								const Access_Operation oper, XrdOucEnv *Env) {
	DeadlockMonitor monitor("Access");
	return m_auth->Access(Entity, path, oper, Env);
}

int DeadlockAcc::Audit(const int accok, const XrdSecEntity *Entity,
					   const char *path, const Access_Operation oper,
					   XrdOucEnv *Env) {
	DeadlockMonitor monitor("Audit");
	return m_auth->Audit(accok, Entity, path, oper, Env);
}

int DeadlockAcc::Test(const XrdAccPrivs priv, const Access_Operation oper) {
	DeadlockMonitor monitor("Test");
	return m_auth->Test(priv, oper);
}

extern "C" {

XrdVERSIONINFO(XrdAccAuthorizeObjAdd, DeadlockAcc);

// XRootD stacks authorization plugins by calling XrdAccAuthorizeObjAdd for each
// library named on the `acc.authlib` directive, passing the previously
// constructed authorization object as `accP`.  This wrapper adds deadlock
// detection around that existing object (the same stacking pattern used by the
// SciTokens authorization plugin).
XrdAccAuthorize *XrdAccAuthorizeObjAdd(XrdSysLogger *logger,
									   const char *config_fn, const char *parms,
									   XrdOucEnv *envP, XrdAccAuthorize *accP) {
	XrdSysError eDest(logger, "deadlock_acc_");

	if (!accP) {
		eDest.Emsg("Initialize",
				   "DeadlockAcc must wrap an existing authorization object; "
				   "name the wrapped plugin earlier on the acc.authlib "
				   "directive");
		return nullptr;
	}

	std::unique_ptr<XrdSysError> log(new XrdSysError(logger, "deadlock_acc_"));
	try {
		return new DeadlockAcc(accP, std::move(log), config_fn);
	} catch (std::exception &e) {
		eDest.Emsg("Initialize",
				   "Encountered a runtime failure when initializing the "
				   "deadlock detection authorization plugin:",
				   e.what());
		return nullptr;
	}
}
}
