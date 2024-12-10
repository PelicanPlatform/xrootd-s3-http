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

#include <memory>
#include <unordered_map>

typedef void CURL;

class XrdSysError;

class HTTPRequest;
class HandlerQueue;

class CurlWorker {
  public:
	CurlWorker(std::shared_ptr<HandlerQueue> queue, XrdSysError &logger)
		: m_queue(queue), m_logger(logger) {}

	CurlWorker(const CurlWorker &) = delete;

	void Run();
	static void RunStatic(CurlWorker *myself);
	static unsigned GetPollThreads() { return m_workers; }

  private:
	std::shared_ptr<HandlerQueue> m_queue;
	std::shared_ptr<HandlerQueue>
		m_unpause_queue; // Queue for notifications that a handle can be
						 // unpaused.
	std::unordered_map<CURL *, HTTPRequest *> m_op_map;
	XrdSysError &m_logger;

	const static unsigned m_workers{5};
	const static unsigned m_max_ops{20};
	const static unsigned m_marker_period{5};
};
