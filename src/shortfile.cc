#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>

#include <string>
#include "shortfile.hh"


ssize_t
full_read( int fd, void * ptr, size_t nbytes ) {
    ssize_t nleft, nread;

    nleft = nbytes;
    while (nleft > 0) {
        REISSUE_READ:
        nread = read(fd, ptr, nleft);
        if (nread < 0) {
            /* error happened, ignore if EINTR, otherwise inform the caller */
            if (errno == EINTR) {
                goto REISSUE_READ;
            }

            /* The caller has no idea how much was actually read in this
                scenario and the file offset is undefined */
            return -1;
        } else if (nread == 0) {
            /* We've reached the end of file marker, so stop looping. */
            break;
        }

        nleft -= nread;
        ptr = ((char *)ptr) + nread;
    }

    /* return how much was actually read, which could include 0 in an
        EOF situation */
    return (nbytes - nleft);
}



bool
readShortFile( const std::string & fileName, std::string & contents ) {
    int fd = open( fileName.c_str(), O_RDONLY, 0600 );

    if( fd < 0 ) {
        // dprintf( D_ALWAYS, "Failed to open file '%s' for reading: '%s' (%d).\n", fileName.c_str(), strerror( errno ), errno );
        return false;
    }

    struct stat statbuf;
    int rv = fstat( fd, & statbuf );
    if( rv < 0 ) {
        // dprintf( D_ALWAYS, "Failed to stat file '%s': %s (%d).\n", fileName.c_str(), strerror( errno ), errno );
        return false;
    }
    unsigned long fileSize = statbuf.st_size;

    char * rawBuffer = (char *)malloc( fileSize + 1 );
    assert( rawBuffer != NULL );
    unsigned long totalRead = full_read( fd, rawBuffer, fileSize );
    close( fd );
    if( totalRead != fileSize ) {
        // dprintf( D_ALWAYS, "Failed to completely read file '%s'; needed %lu but got %lu.\n", fileName.c_str(), fileSize, totalRead );
        free( rawBuffer );
        return false;
    }
    contents.assign( rawBuffer, fileSize );
    free( rawBuffer );

    return true;
}


#if defined(LATER)
bool
writeShortFile( const std::string & fileName, const std::string & contents ) {
    int fd = safe_open_wrapper_follow( fileName.c_str(), O_WRONLY | O_CREAT | O_TRUNC | _O_BINARY, 0600 );

    if( fd < 0 ) {
        dprintf( D_ALWAYS, "Failed to open file '%s' for writing: '%s' (%d).\n", fileName.c_str(), strerror( errno ), errno );
        return false;
    }

    unsigned long written = full_write( fd, contents.c_str(), contents.length() );
    close( fd );
    if( written != contents.length() ) {
        dprintf( D_ALWAYS, "Failed to completely write file '%s'; wanted to write %lu but only put %lu.\n",
                 fileName.c_str(), (unsigned long)contents.length(), written );
        return false;
    }

    return true;
}


bool
appendShortFile( const std::string & fileName, const std::string & contents ) {
    int fd = safe_open_wrapper_follow( fileName.c_str(), O_WRONLY | O_APPEND | _O_BINARY, 0600 );

    if( fd < 0 ) {
        dprintf( D_ALWAYS, "Failed to open file '%s' for writing: '%s' (%d).\n", fileName.c_str(), strerror( errno ), errno );
        return false;
    }

    unsigned long written = full_write( fd, contents.c_str(), contents.length() );
    close( fd );
    if( written != contents.length() ) {
        dprintf( D_ALWAYS, "Failed to completely append to file '%s'; wanted to append %lu but only put %lu.\n",
                 fileName.c_str(), (unsigned long)contents.length(), written );
        return false;
    }

    return true;
}
#endif /* defined(LATER) */
