#pragma once

#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOss/XrdOss.hh"


class HTTPDirectory : public XrdOssDF {
public:
    HTTPDirectory(XrdSysError &log) :
        m_log(log)
    {
    }

    virtual ~HTTPDirectory() {}

    virtual int
    Opendir(const char *path,
            XrdOucEnv &env) override
    {
        return -ENOSYS;
    }

    int Readdir(char *buff, int blen)
    {
        return -ENOSYS;
    }

    int StatRet(struct stat *statStruct)
    {
        return -ENOSYS;
    }

    int Close(long long *retsz=0)
    {
        return -ENOSYS;
    }


private:
    XrdSysError m_log;

};
