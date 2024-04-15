/***************************************************************
 *
 * Copyright (C) 2024, HTCondor Team, UW-Madison
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

#ifndef CHECK_PRINTF_FORMAT
#ifdef __GNUC__
#define CHECK_PRINTF_FORMAT(a, b) __attribute__((__format__(__printf__, a, b)))
#else
#define CHECK_PRINTF_FORMAT(a, b)
#endif
#endif

void trim(std::string &str);
std::string substring(const std::string &str, size_t left,
					  size_t right = std::string::npos);
void toLower(std::string &str);

int formatstr(std::string &s, const char *format, ...)
	CHECK_PRINTF_FORMAT(2, 3);
int formatstr_cat(std::string &s, const char *format, ...)
	CHECK_PRINTF_FORMAT(2, 3);
