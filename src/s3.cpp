
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSec/XrdSecEntityAttr.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdVersion.hh"
#include "S3FileSystem.hh"
#include "S3File.hh"

#include <curl/curl.h>

#include <memory>
#include <mutex>
#include <sstream>
#include <vector>

S3FileSystem* g_s3_oss = nullptr;

XrdVERSIONINFO(XrdOssGetFileSystem, S3);

S3File::S3File(XrdSysError &log, S3FileSystem *oss) :
    m_log(log),
    m_nextoff(0),
    m_oss(oss)
{}

int S3File::Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env)
{
    if (!strcmp(path, "/aws/us-east-1/bucket/hello_world")) {
        m_log.Emsg("Open", "Opened our magic hello-world file");
        return 0;
    }

    return -ENOENT;
}


ssize_t
S3File::Read(void *buffer, off_t offset, size_t size)
{
    std::stringstream ss;
    ss << "Reading S3 at " << offset << "@" << size;
    m_log.Emsg("Read", ss.str().c_str());

    if (offset != 0) {return -EIO;}

    const auto len = strlen("hello world");

    if (size < len) {return -EIO;}

    memcpy(buffer, "hello world", len);
    return len;
}


int
S3File::Fstat(struct stat *buff)
{
    const auto len = strlen("hello world");

    buff->st_mode = 0600 | S_IFREG;
    buff->st_nlink = 1;
    buff->st_uid = 1;
    buff->st_gid = 1;
    buff->st_size = len;
    buff->st_mtime = 0;
    buff->st_atime = 0;
    buff->st_ctime = 0;
    buff->st_dev = 0;
    buff->st_ino = 0;

    return 0;
}


ssize_t
S3File::Write(const void *buffer, off_t offset, size_t size)
{
    m_log.Emsg("Write", "S3 file does not yet support write");
    return -ENOENT;
}



int S3File::Close(long long *retsz) 
{
    m_log.Emsg("Close", "Closed our S3 file");

    return 0;
}

extern "C" {

/*
    This function is called when we are wrapping something.
*/
XrdOss *XrdOssAddStorageSystem2(XrdOss       *curr_oss,
                                XrdSysLogger *Logger,
                                const char   *config_fn,
                                const char   *parms,
                                XrdOucEnv    *envP)
{
    XrdSysError log(Logger, "s3_");

    log.Emsg("Initialize", "S3 filesystem cannot be stacked with other filesystems");
    return nullptr;
}

/* 
    This function is called when it is the top level file system and we are not
    wrapping anything
*/
XrdOss *XrdOssGetStorageSystem2(XrdOss       *native_oss,
                                XrdSysLogger *Logger,
                                const char   *config_fn,
                                const char   *parms,
                                XrdOucEnv    *envP)
{
    XrdSysError log(Logger, "s3_");

    envP->Export("XRDXROOTD_NOPOSC", "1");

    try {
        g_s3_oss = new S3FileSystem(Logger, config_fn, envP);
        return g_s3_oss;
    } catch (std::runtime_error &re) {
        log.Emsg("Initialize", "Encountered a runtime failure", re.what());
        return nullptr;
    }
}


XrdOss *XrdOssGetStorageSystem(XrdOss       *native_oss,
                               XrdSysLogger *Logger,
                               const char   *config_fn,
                               const char   *parms)
{
    return XrdOssGetStorageSystem2(native_oss, Logger, config_fn, parms, nullptr);
}


}

XrdVERSIONINFO(XrdOssGetStorageSystem,  s3);
XrdVERSIONINFO(XrdOssGetStorageSystem2, s3);
XrdVERSIONINFO(XrdOssAddStorageSystem2, s3);
