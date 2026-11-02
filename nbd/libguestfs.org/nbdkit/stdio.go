package nbdkit

/*
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

// nbdkit closes fds 0 and 1 and reopens /dev/null on them, which only works
// if no other thread allocates a descriptor in between. Go runtime threads
// can, so record what stdio was at load to name an intruder at exit.
static struct stat stdio_at_load[2];
static int stdio_at_load_ok[2];

static int
stdio_same_file (int fd, const struct stat *want)
{
  struct stat st;

  if (fstat (fd, &st) != 0)
    return 0;
  return st.st_dev == want->st_dev && st.st_ino == want->st_ino;
}

// Writes the open descriptor table to buf and returns 1 when fd 0 or 1 is
// neither what it was at load nor /dev/null; otherwise returns 0.
static int
stdio_report (char *buf, size_t len)
{
  struct stat devnull;
  int have_devnull = stat ("/dev/null", &devnull) == 0;
  int foreign = 0;
  size_t off;
  int fd;

  for (fd = 0; fd < 2; fd++) {
    if (fcntl (fd, F_GETFD) == -1)
      continue;
    if (stdio_at_load_ok[fd] && stdio_same_file (fd, &stdio_at_load[fd]))
      continue;
    if (have_devnull && stdio_same_file (fd, &devnull))
      continue;
    foreign = 1;
  }
  if (!foreign)
    return 0;

  off = snprintf (buf, len, "nbdkit golang plugin: stdin/stdout were taken "
                  "by another descriptor; open descriptors:");
  for (fd = 0; fd < 64 && off < len; fd++) {
    char link[32], target[256];
    ssize_t n;
    int w;

    snprintf (link, sizeof link, "/proc/self/fd/%d", fd);
    n = readlink (link, target, sizeof target - 1);
    if (n < 0)
      continue;
    target[n] = '\0';
    w = snprintf (buf + off, len - off, " %d=%s", fd, target);
    if (w < 0)
      break;
    off += (size_t) w;
  }
  return 1;
}

static void
stdio_report_at_exit (void)
{
  char buf[4096];

  if (stdio_report (buf, sizeof buf))
    fprintf (stderr, "%s\n", buf);
}

__attribute__((constructor))
static void
stdio_record_at_load (void)
{
  int fd;

  for (fd = 0; fd < 2; fd++)
    stdio_at_load_ok[fd] = fstat (fd, &stdio_at_load[fd]) == 0;
  atexit (stdio_report_at_exit);
}

static int
stdio_report_go (char *buf, size_t len)
{
  return stdio_report (buf, len);
}
*/
import "C"

import "unsafe"

// stdioReport returns the descriptor table printed at exit when fd 0 or 1 no
// longer holds its load-time file or /dev/null, and whether that is the case.
func stdioReport() (string, bool) {
	buf := make([]byte, 4096)
	if C.stdio_report_go((*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))) == 0 {
		return "", false
	}
	return C.GoString((*C.char)(unsafe.Pointer(&buf[0]))), true
}
