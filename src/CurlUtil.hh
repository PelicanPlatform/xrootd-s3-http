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

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <stack>
#include <unordered_map>

// Forward dec'ls
typedef void CURL;
struct curl_slist;

class HTTPRequest;

// Returns a newly-created curl handle (no internal caching)
CURL *GetHandle(bool verbose);

/**
 * HandlerQueue is a deque of curl operations that need
 * to be performed.  The object is thread safe and can
 * be waited on via poll().
 *
 * The fact that it's poll'able is necessary because the
 * multi-curl driver thread is based on polling FD's
 */
class HandlerQueue {
  public:
	HandlerQueue();

	void Produce(HTTPRequest *handler);

	HTTPRequest *Consume();
	HTTPRequest *TryConsume();

	int PollFD() const { return m_read_fd; }

	CURL *GetHandle();
	void RecycleHandle(CURL *);

  private:
	std::deque<HTTPRequest *> m_ops;
	thread_local static std::stack<CURL *> m_handles;
	std::condition_variable m_cv;
	std::mutex m_mutex;
	const static unsigned m_max_pending_ops{20};
	int m_read_fd{-1};
	int m_write_fd{-1};
};
