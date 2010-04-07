#define _LARGEFILE64_SOURCE
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <assert.h>

#define PORT (10000)
#define MIN(a, b)	((a) < (b) ? (a) : (b))
#define ALIGN(a, b)	(((a) + ((b) - 1)) & (~((b) - 1)))

static int direct;
int do_recvfile_splice(int fromfd, int tofd, loff_t *offset, size_t count);
int do_recvfile_rw(int fromfd, int tofd, loff_t *offset, size_t count);
double wtime();

struct functab {
  char *name;
  int (*func)(int, int, loff_t *, size_t);
} recvfiles[] = {
  { "splice",	do_recvfile_splice },
  { "rw",	do_recvfile_rw },
  { 0, 0 }
};

void
usage()
{
  printf("usage:\n");
  printf(" sender:   sendfile OPTIONS remotehost filename\n");
  printf(" receiver: sendfile OPTIONS -sink filename\n");
  printf("OPTIONS:\n");
  printf(" -direct           specify to use direct I/O\n");
  printf(" -port <port>      specify the port number\n");
  printf(" -recv [splice|rw] specify recvfile method (receiver only)\n");
  printf(" -once             specify to run in one-shot (receiver only)\n");
  printf(" -help             display this message\n");
}

inline void
perror_exit(const char *s, int status)
{
  perror(s);
  exit(status);
}

int active_open(const char *host, const int port)
{
  struct sockaddr_in sa;
  struct hostent *he;
  int fd;
  int cc;
  int one = 1;

  he = gethostbyname(host);
  if (he == NULL) {
    perror("gethostbyname");
    return -1;
  }

  memset(&sa, 0, sizeof(sa));
  sa.sin_family = he->h_addrtype;
  memcpy(&sa.sin_addr.s_addr, he->h_addr_list[0], 4);
  sa.sin_port = htons(port);

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    perror("socket");
    return -1;
  }

  cc = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&one, sizeof(one));
  if (cc < 0) {
    perror("WARN: setsockopt(SO_REUSEADDR)");
    /* Ignore setsockopt error. */
  }

  cc = connect(fd, (struct sockaddr *)&sa, sizeof(sa));
  if (cc < 0) {
    perror("connect");
    return -1;
  }

  return fd;
}

int passive_open(const int port)
{
  struct sockaddr_in sa;
  socklen_t salen;
  int fd;
  int cc;
  int one = 1;

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    perror("socket");
    return -1;
  }

  cc = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&one, sizeof(one));
  if (cc < 0) {
    perror("WARN: setsockopt(SO_REUSEADDR)");
    /* Ignore setsockopt error. */
  }

  memset(&sa, 0, sizeof(sa));
  sa.sin_family = AF_INET;
  sa.sin_addr.s_addr = htonl(INADDR_ANY);
  sa.sin_port = htons(port);
  salen = sizeof(sa);

  cc = bind(fd, (struct sockaddr *)&sa, salen);
  if (cc < 0) {
    perror("bind");
    return -1;
  }

  cc = listen(fd, 0);
  if (cc < 0) {
    perror("listen");
    return -1;
  }

  return fd;
}

long
do_sendfile(int fromfd, int tofd, loff_t *offset, size_t count)
{
  long c, n, total = 0;
  loff_t off = 0;

  if (offset == NULL)
    offset = &off;

  c = count;
  while (c > 0) {
    n = sendfile(tofd, fromfd, offset, c);
    if (n < 0) {
      perror("sendfile");
      return -1;
    }
    c -= n;
    total += n;
  }

  return total;
}

/* splice version. */

int
do_recvfile_splice(int fromfd, int tofd, loff_t *offset, size_t count)
{
  int pipefd[2];
  long rc, wc, nread, nwrite, total;
  loff_t off = 0;
  int cc;

  if (offset == NULL)
    offset = &off;

  cc = pipe(pipefd);
  if (cc < 0) {
    perror("pipe");
    return -1;
  }

  rc = count;
  while (rc > 0) {
    /* {len} is bound upto 16384, because the process blocks forever
     * when setting large number to it. */
    nread = splice(fromfd, NULL, pipefd[1], NULL, MIN(rc, 16384),
		   SPLICE_F_MOVE);
    if (nread < 0) {
      perror("splice(socket to pipe)");
      return -1;
    }

    wc = nread;
    while (wc > 0) {
      nwrite = splice(pipefd[0], NULL, tofd, offset, wc, SPLICE_F_MOVE);
      if (nwrite < 0) {
	perror("splice(pipe to file)");
	return -1;
      }
      wc -= nwrite;
    }
    rc -= nread;
    total += nread;
  }

  return total;
}

/* read/write version. */

int
do_recvfile_rw(int fromfd, int tofd, loff_t *offset, size_t count)
{
  long rc, nread, nwrite, total = 0;
  char *rcvbuf;
  int cc;
  int bufsiz = 4096 * 20;

  if (direct) {
    fprintf(stderr, "Not implement direct IO mode.");
    exit(1);

    cc = posix_memalign((void **)&rcvbuf, getpagesize(), bufsiz);
    if (cc < 0) {
      perror("posix_memalign");
      return -1;
    }
  } else {
    rcvbuf = malloc(bufsiz);
    if (rcvbuf == NULL) {
      perror("malloc");
      return -1;
    }
  }

  rc = count;
  while (rc > 0) {
    nread = read(fromfd, rcvbuf, bufsiz);
    if (nread < 0) {
      perror("read");
      return -1;
    }

    nwrite = write(tofd, rcvbuf, nread);
    if (nwrite < 0) {
      perror("write");
      return -1;
    }
    assert(nread == nwrite);

    rc -= nread;
    total += nread;
  }

  return total;
}

int
main(int argc, char **argv)
{
  struct stat s;
  int fromfd, tofd;
  ssize_t fsiz;
  int cc;
  double start, end;
  int sink, port = PORT;
  int once = 0;
  int (*do_recvfile)(int, int, loff_t *, size_t) = do_recvfile_splice;
  int flag;

  argc--;
  argv++;
  while (argc > 0) {
    if (strncmp(argv[0], "-direct", 7) == 0) {
      direct = 1;
      argc--;
      argv++;
    } else if (strncmp(argv[0], "-sink", 5) == 0) {
      sink = 1;
      argc--;
      argv++;
    } else if (strncmp(argv[0], "-once", 5) == 0) {
      once = 1;
      argc--;
      argv++;
    } else if (strncmp(argv[0], "-recv", 5) == 0) {
      int i;
      for (i = 0; recvfiles[i].name != 0; i++) {
	if (strncmp(argv[1], recvfiles[i].name,
		    strlen(recvfiles[i].name)) == 0) {
	  do_recvfile = recvfiles[i].func;
	  break;
	}
      }
      argc -= 2;
      argv += 2;
    } else if (strncmp(argv[0], "-port", 5) == 0) {
      char *p;
      port = strtoul(argv[1], &p, 10);
      if (p == argv[1])
	port = PORT;

      argc -= 2;
      argv += 2;
    } else if (strncmp(argv[0], "-help", 5) == 0 ||
	       strncmp(argv[0], "-h", 2) == 0) {
      usage();
      exit(0);
    } else
      break;
  }

  if (sink) {
    /* RECEIVER SIDE */
    struct sockaddr_in sa;
    socklen_t salen;
    int lfd;

    if (argc != 1) {
      usage();
      exit(1);
    }

    cc = stat(argv[0], &s);
    if (cc == 0) {
      cc = unlink(argv[0]);
      if (cc < 0)
	perror_exit("unlink", 1);
    }

    flag = O_RDWR | O_CREAT;
    if (direct)
      flag |= O_DIRECT;
    tofd = open(argv[0], flag, 0666);
    if (tofd < 0)
      perror_exit("open", 1);

    lfd = passive_open(port);
    if (cc < 0)
      exit(1);

    do {
      salen = sizeof(sa);
      fromfd = accept(lfd, (struct sockaddr *)&sa, &salen);

      cc = read(fromfd, &fsiz, sizeof(fsiz));
      if (cc < 0)
	perror_exit("read", 1);

      start = wtime();
      cc = do_recvfile(fromfd, tofd, NULL, fsiz);
      if (cc < 0)
	exit(1);
      end = wtime();

      cc = fstat(tofd, &s);
      if (cc < 0)
	perror_exit("fstat", 1);

      printf("%s (%ld bytes)\t%g MB/sec\n",
	     argv[0], fsiz, ((double)fsiz / (end - start) / 1.0e6));

      close(fromfd);
    } while (!once);

    close(tofd);
  } else {
    /* SENDER SIDE */
    long nsend;

    if (argc != 2) {
      usage();
      exit(1);
    }

    tofd = active_open(argv[0], port);
    if (tofd < 0)
      exit(1);

    flag = O_RDONLY;
    if (direct)
      flag |= O_DIRECT;
    fromfd = open(argv[1], flag);
    if (fromfd < 0)
      perror_exit("open", 1);

    cc = fstat(fromfd, &s);
    if (cc < 0)
      perror_exit("fstat", 1);

    fsiz = s.st_size;
    cc = write(tofd, &fsiz, sizeof(fsiz));
    if (cc < 0)
      perror_exit("write(fsiz)", 1);

    start = wtime();
    nsend = do_sendfile(fromfd, tofd, NULL, fsiz);
    if (nsend < 0)
      exit(1);
    end = wtime();

    printf("%s (%ld bytes)\t%g MB/sec\n",
	   argv[1], nsend, ((double)nsend / (end - start) / 1.0e6));

    close(fromfd);
    close(tofd);
  }

  return 0;
}

double
wtime()
{
  struct timeval tv;
  int cc;
  static struct timeval tv0;

  if (tv0.tv_sec == 0) {
    cc = gettimeofday(&tv, 0);
    if (cc != 0)
      perror_exit("gettimeofday", 1);
    tv0 = tv;
    return 0.0;
  }

  cc = gettimeofday(&tv, 0);
  if (cc != 0)
    perror_exit("gettimeofday", 1);

  return (((double)(tv.tv_sec - tv0.tv_sec))
	  + ((double)(tv.tv_usec - tv0.tv_usec)) * 1e-6);
}
