/* cgo wrappers.
 * Copyright Red Hat
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Red Hat nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RED HAT AND CONTRIBUTORS ''AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL RED HAT OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package nbdkit

/*
#cgo pkg-config: nbdkit

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define NBDKIT_API_VERSION 2
#include <nbdkit-plugin.h>
#include "wrappers.h"

// Save the original PID when loaded so we can find out if we
// forked and issue a warning.  See the "IMPORTANT NOTE" section
// of nbdkit-golang-plugin(3) man page.
static pid_t original_pid;

extern void implLoad (void);
void
wrapper_load (void)
{
  original_pid = getpid ();
  implLoad ();
}

extern void implUnload (void);
void
wrapper_unload (void)
{
  implUnload ();
}

extern void implDumpPlugin (void);
void
wrapper_dump_plugin (void)
{
  implDumpPlugin ();
}

extern int implConfig (const char *key, const char *value);
int
wrapper_config (const char *key, const char *value)
{
  return implConfig (key, value);
}

extern int implConfigComplete (void);
int
wrapper_config_complete (void)
{
  return implConfigComplete ();
}

extern int implGetReady (void);
int
wrapper_get_ready (void)
{
  return implGetReady ();
}

// As the name suggests, this is not a wrapper around a golang
// function.  There is no need for it since golang plugins must
// not fork.
int
nonwrapper_after_fork (void)
{
  if (original_pid > 0 && getpid () != original_pid) {
    nbdkit_error ("detected fork in golang plugin!  The plugin will "
                  "probably fail or hang in unexpected ways.  Please read "
                  "\"IMPORTANT NOTE\" in nbdkit-golang-plugin(3) man page");
    //return -1;
  }
  return 0;
}

extern int implPreConnect (int readonly);
int
wrapper_preconnect (int readonly)
{
  return implPreConnect (readonly);
}

extern void *implOpen (int readonly);
void *
wrapper_open (int readonly)
{
  return implOpen (readonly);
}

extern void implClose (void *handle);
void
wrapper_close (void *handle)
{
  return implClose (handle);
}

extern int64_t implGetSize (void *handle);
int64_t
wrapper_get_size (void *handle)
{
  return implGetSize (handle);
}

extern int implCanWrite (void *handle);
int
wrapper_can_write (void *handle)
{
  return implCanWrite (handle);
}

extern int implCanFlush (void *handle);
int
wrapper_can_flush (void *handle)
{
  return implCanFlush (handle);
}

extern int implIsRotational (void *handle);
int
wrapper_is_rotational (void *handle)
{
  return implIsRotational (handle);
}

extern int implCanTrim (void *handle);
int
wrapper_can_trim (void *handle)
{
  return implCanTrim (handle);
}

extern int implCanZero (void *handle);
int
wrapper_can_zero (void *handle)
{
  return implCanZero (handle);
}

extern int implCanMultiConn (void *handle);
int
wrapper_can_multi_conn (void *handle)
{
  return implCanMultiConn (handle);
}

extern int implPRead (void *handle, void *buf,
                      uint32_t count, uint64_t offset, uint32_t flags);
int
wrapper_pread (void *handle, void *buf,
               uint32_t count, uint64_t offset, uint32_t flags)
{
  return implPRead (handle, buf, count, offset, flags);
}

extern int implPWrite (void *handle, const void *buf,
                       uint32_t count, uint64_t offset, uint32_t flags);
int
wrapper_pwrite (void *handle, const void *buf,
                uint32_t count, uint64_t offset, uint32_t flags)
{
  return implPWrite (handle, buf, count, offset, flags);
}

extern int implFlush (void *handle, uint32_t flags);
int
wrapper_flush (void *handle, uint32_t flags)
{
  return implFlush (handle, flags);
}

extern int implTrim (void *handle,
                     uint32_t count, uint64_t offset, uint32_t flags);
int
wrapper_trim (void *handle,
              uint32_t count, uint64_t offset, uint32_t flags)
{
  return implTrim (handle, count, offset, flags);
}

extern int implZero (void *handle,
                     uint32_t count, uint64_t offset, uint32_t flags);
int
wrapper_zero (void *handle,
              uint32_t count, uint64_t offset, uint32_t flags)
{
  return implZero (handle, count, offset, flags);
}
*/
import "C"
