#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdVersion.hh"
#include "S3FileSystem.hh"
#include "S3Directory.hh"
#include "S3File.hh"

#include <memory>
#include <vector>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


S3FileSystem::S3FileSystem(XrdSysLogger *lp, const char *configfn, XrdOucEnv *envP) :
    m_env(envP),
    m_log(lp, "s3_")
{
    m_log.Say("------ Initializing the S3 filesystem plugin.");
    if (!Config(lp, configfn)) {
        throw std::runtime_error("Failed to configure S3 filesystem plugin.");
    }
}


S3FileSystem::~S3FileSystem() {
}


bool
S3FileSystem::Config(XrdSysLogger *lp, const char *configfn)
{
    XrdOucEnv myEnv;
    XrdOucStream Config(&m_log, getenv("XRDINSTANCE"), &myEnv, "=====> ");

    int cfgFD = open(configfn, O_RDONLY, 0);
    if (cfgFD < 0) {
        m_log.Emsg("Config", errno, "open config file", configfn);
        return false;
    }
    Config.Attach(cfgFD);
    const char *val;
    while ((val = Config.GetMyFirstWord())) {
        if (!strcmp("s3.test", val)) {
            val = Config.GetWord();
            if (!val || !val[0]) {
                m_log.Emsg("Config", "s3.test must specify a value");
                Config.Close();
                return false;
            }
            m_log.Emsg("Config", "Value of s3.test", val);
        }
    }

    int retc = Config.LastError();
    if (retc) {
        m_log.Emsg("Config", -retc, "read config file", configfn);
        Config.Close();
        return false;
    }
    Config.Close();

    return true;
}


// Object Allocation Functions
//
XrdOssDF *
S3FileSystem::newDir(const char *user)
{
    return new S3Directory(m_log);
}


XrdOssDF *
S3FileSystem::newFile(const char *user)
{
    return new S3File(m_log, this);
}


int
S3FileSystem::Stat(const char *path, struct stat *buff,
                    int opts, XrdOucEnv *env)
{
    m_log.Emsg("Stat", "Stat'ing path", path);

    if (strcmp(path, "/aws/us-east-1/bucket/hello_world")) {
        m_log.Emsg("Stat", "Pretending file isn't found");
        return -ENOENT;
    }

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
