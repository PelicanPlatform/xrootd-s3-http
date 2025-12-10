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

#ifndef __DEADLOCK_ACC_HH_
#define __DEADLOCK_ACC_HH_

#include "DeadlockDetector.hh"

#include <XrdAcc/XrdAccAuthorize.hh>

#include <memory>

// Forward declarations
class XrdSysError;
class XrdSecEntity;

/**
 * Authorization wrapper that adds deadlock detection to all authorization
 * operations.
 *
 * Wraps another XrdAccAuthorize implementation and creates a DeadlockMonitor
 * for each operation to detect if it blocks for too long.
 */
class DeadlockAcc : public XrdAccAuthorize {
  public:
	DeadlockAcc(XrdAccAuthorize *auth, std::unique_ptr<XrdSysError> log,
				const char *configName);

	virtual ~DeadlockAcc();

	virtual XrdAccPrivs Access(const XrdSecEntity *Entity, const char *path,
							   const Access_Operation oper,
							   XrdAccPrivCaps *caps = 0) override;

	virtual int Audit(const int accok, const XrdSecEntity *Entity,
					  const char *path, const Access_Operation oper,
					  XrdOucEnv *Env = 0) override;

	virtual int Test(const XrdSecEntity *Entity, const char *path,
					 const Access_Operation oper, XrdOucEnv *Env = 0) override;

  private:
	XrdAccAuthorize *m_auth;
	std::unique_ptr<XrdSysError> m_log;
};

#endif // __DEADLOCK_ACC_HH_
