/* tcpa2a.c  */
/* This program is a benchmark of an All-to-all communication of TCP,
 *  however we use MPI library for setup operations.
 */

/*#define DEBUG*/

#include "mpi.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <assert.h>

#define PORT 10000

#define KB 1024
#define MB 1024*1024

int MAX_SEND_SIZE = 10*MB;
#define NNN(len) ((len) < 10 * 1024 ? 1000 : ((MAX_SEND_SIZE / len) * 10))
//#define NNN(len) ((MAX_SEND_SIZE / len) * 10)

int sockbufsize = 2*MB;
//int sockbufsize = 64*KB;

int lengths[] = {
    //64, 256, 512,
    1*KB, 2*KB, 4*KB, 8*KB, 
    16*KB, 32*KB, 64*KB, 128*KB, 
    256*KB, 512*KB,
    1*MB, 2*MB, 
    4*MB, 8*MB, 10*MB,
    0
};
# undef KB
# undef MB

unsigned char *sendbuf;
unsigned char *recvbuf;
struct request *recv_req;

struct host_info {
    int rank;
    unsigned long addr;
    int sock;
};

struct host_info *hosts;

struct request {
    int fd;
    int rank;
    int count;
    int len;
};

int comm_rank, comm_size;

/* statistics */
int scnt, rcnt;
int nr_select, nr_read, nr_write;


void
perror_exit(const char *s, int status)
{
    perror(s);
    exit(status);
}

inline void *
emalloc(const size_t size)
{
    void *p;

    p = malloc(size);
    if (p == NULL)
	perror_exit("malloc", 1);

    return p;
}


/* MPI related utitity functions */
void
exchange_host_info()
{
    int namelen;
    char procname[MPI_MAX_PROCESSOR_NAME];
    struct hostent *addr;
    unsigned long addr_tmp;
    struct host_info mine;

    hosts = (struct host_info *)emalloc(sizeof(struct host_info) * comm_size);

    MPI_Get_processor_name(procname, &namelen);
#if 1 /* s/iSCSI/10GbE/ */
    strncpy(procname, "10GbE", 5);
#endif
    addr = gethostbyname(procname);
    bcopy(addr->h_addr, (char *)&addr_tmp, addr->h_length);
    mine.rank = comm_rank;
    mine.addr = addr_tmp & 0xffffffff;
    mine.sock  = 0;
    MPI_Allgather(&mine, sizeof(struct host_info), MPI_BYTE, hosts, 
		  sizeof(struct host_info), MPI_BYTE, MPI_COMM_WORLD);
}

int
get_peer_rank(unsigned long addr)
{
    int i;
    for (i = 0; i < comm_size; i++) {
	if (hosts[i].addr == addr) {
	    return i;
	}
    }
    return -1;
}

int
get_peer_rank_by_sock(int fd)
{
    struct sockaddr_in sa;
    socklen_t salen = sizeof(sa);
    int peer;
    int cc;

    cc = getpeername(fd, (struct sockaddr *)&sa, &salen);
    if (cc < 0) {
	perror("getpeername");
	return -1;
    }

    peer = get_peer_rank(sa.sin_addr.s_addr);
    if (peer < 0) {
	fprintf(stderr, "can't get peer rank: %8x\n", sa.sin_addr.s_addr);
	return -1;
    }

    return peer;
}


/* socket utility functions */
int
set_sock_blocking(int fd, int block)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (block) {
      /* Switch the socket to blocking */
      flags &= ~O_NONBLOCK;
    } else {
      /* Switch the socket to non-blocking */
      flags |= O_NONBLOCK;
    }
    flags = fcntl(fd, F_SETFL, flags);
    if (flags < 0)
	perror_exit("fcntl", 1);

    return flags;
}

/* set socket options before listen(2) or connect(2) calls */
void
set_sock_opt(int fd)
{
    int cc;
    int one = 1;

    cc = setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sockbufsize,
		    sizeof(sockbufsize));
    if (cc < 0)
	perror("WARN: setsockopt(SO_SNDBUF)");

    cc = setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sockbufsize,
		    sizeof(sockbufsize));
    if (cc < 0)
	perror("WARN: setsockopt(SO_RCVBUF)");

    cc = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    if (cc < 0 )
	perror("WARN: setsockopt(TCP_NODELAY)");
}

/* request queue management functions */
int
passive_open(const int port)
{
    struct sockaddr_in sa;
    socklen_t salen;
    int fd, cc;
    int one = 1;

    fd = socket(PF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
	perror("socket");
	return -1;
    }

    cc = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&one, sizeof(one));
    if (cc < 0) {
	perror("WARN: setsockopt(SO_REUSEADDR)");
	/* Ignore this error. */
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

    set_sock_opt(fd);
    set_sock_blocking(fd, 1);

    cc = listen(fd, 5);
    if (cc < 0) {
	perror("listen");
	return -1;
    }

    return fd;
}

int
active_open(const int peer, const int port)
{
    struct sockaddr_in sa;
    int fd;
    int cc;

    fd = socket(PF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
	perror("socket");
	return -1;
    }

    memset((char *)&sa, 0, sizeof(sa));
    sa.sin_family = PF_INET;
    sa.sin_addr.s_addr = hosts[peer].addr;
    sa.sin_port = htons(port);

    set_sock_opt(fd);

    cc = connect(fd, (struct sockaddr *)&sa, sizeof(sa));
    if (cc < 0) {
	perror("connect");
	return -1;
    }

    hosts[peer].sock = fd;

    return 0;
}

int
send_handler(struct request *req)
{
    int cc;
    int base = req->rank * req->len;

    set_sock_blocking(req->fd, 0);
    while (req->count < req->len) {
	cc = write(req->fd, &sendbuf[base + req->count],
		   req->len - req->count);
	nr_write++;

	if (cc < 0) {
	    if (errno == EAGAIN || errno == EWOULDBLOCK)
		return 0;
	    else
		perror_exit("write", 1);
	} else if (cc == 0) {
	    fprintf(stderr, "send: connection closed\n");
	    exit(1);
	}
	req->count += cc;
	scnt += cc;
    }

    return 0;
}

int
recv_handler(struct request *req)
{
    int cc;
    int base = req->rank * req->len;

    set_sock_blocking(req->fd, 0);
    while (req->count < req->len) {
	cc = read(req->fd, &recvbuf[base + req->count],
		  req->len - req->count);
	nr_read++;

	if (cc < 0) {
	    if (errno == EAGAIN || errno == EWOULDBLOCK)
		return 0;
	    else
		perror_exit("read", 1);
	} else if (cc == 0) {
	    fprintf(stderr, "recv: connection closed\n");
	    exit(1);
	}
	req->count += cc;
	rcnt += cc;
    }

    return 0;
}

#define GET_RANK(n) ((n) < 0 ? comm_size - abs((n)) : (n) % comm_size)
#ifndef MAX
#define MAX(m, n) ((m) < (n) ? (n) : (m))
#endif
void
alltoall(int len)
{
    int cc;
    int i, n;
    int m;
    fd_set wfds, rfds;
    struct request send_req;

    send_req.rank = comm_rank;
    send_req.len  = len;

    for (i = 0; i < comm_size; i++) {
	if (i != comm_rank) {
	    recv_req[i].count = 0;
	} else {
	    recv_req[i].count = len;
	}
    }

    for (n = 0; n < comm_size - 1; n++) {
	/* send phase */
	send_req.rank = GET_RANK(send_req.rank + 1);
	send_req.fd = hosts[send_req.rank].sock;
	send_req.count = 0;
	send_handler(&send_req);

	assert(send_req.rank != comm_rank);

	for (;;) {
	    int nsel = 0;
	    m = -1;
	    FD_ZERO(&wfds);
	    FD_ZERO(&rfds);
	    if (send_req.count != send_req.len) {
		FD_SET(send_req.fd, &wfds);
		m = MAX(m, send_req.fd);
		nsel++;
	    } else {
		if (n + 1 != comm_size - 1)
		    break;	/* We have another pending send requests. */
	    }

	    for (i = 0; i < comm_size; i++) {
		if (recv_req[i].count == recv_req[i].len)
		    continue;
		FD_SET(recv_req[i].fd, &rfds);
		m = MAX(m, recv_req[i].fd);
		nsel++;
	    }

	    if (nsel == 0)
		break;
	    cc = select(m + 1, &rfds, &wfds, NULL, NULL);
	    nr_select++;
	    if (cc < 0)
		perror_exit("select", 1);

	    for (i = 0; i < comm_size; i++) {
		if (recv_req[i].count == recv_req[i].len)
		    continue;
		if (FD_ISSET(recv_req[i].fd, &rfds))
		    recv_handler(&recv_req[i]);
	    }
	    if (FD_ISSET(send_req.fd, &wfds))
		send_handler(&send_req);
	}
    }
}

void
runtest(size_t len, int iter)
{
    int i;
    scnt = rcnt = 0;
    nr_read = nr_write = nr_select = 0;

    for (i = 0; i < iter; i++)
	alltoall(len);

#ifdef DEBUG
    printf("[%d] phase end len: %d iter: %d s: %d r: %d\n",
	   comm_rank, len, iter, scnt, rcnt);
    printf("[%d] read: %d write: %d select: %d\n", comm_rank, nr_read, nr_write, nr_select);
#endif
}

/* establish all to all connections. */
void
do_dance(const int port)
{
    int i, cc;
    int fd;

    if (comm_rank != comm_size - 1) {
	fd = passive_open(port);
	if (fd < 0) {
	    fprintf(stderr, "[%d] Failed passive open.\n", comm_rank);
	    exit(1);
	}
    }
    MPI_Barrier(MPI_COMM_WORLD);

    for (i = 0; i < comm_rank; i++) {
	if (active_open(i, port) < 0) {
	    fprintf(stderr, "[%d] Failed active open.\n", comm_rank);
	    exit(1);
	}
    }

    for (i = 0; i < comm_size - comm_rank - 1;) {
	fd_set afds;

	FD_ZERO(&afds);
	FD_SET(fd, &afds);
	cc = select(fd + 1, &afds, NULL, NULL, NULL);
	if (cc == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
	    continue;

	if (FD_ISSET(fd, &afds)) {
	    struct sockaddr_in sa;
	    socklen_t salen = sizeof(sa);
	    int sock;
	    int peer;

	    sock = accept(fd, (struct sockaddr *)&sa, &salen);
	    if (sock < 0)
		perror_exit("accept", 1);

	    peer = get_peer_rank_by_sock(sock);
	    if (peer < 0) {
		fprintf(stderr, "Failed get peer rank.\n");
		exit(1);
	    }
	    hosts[peer].sock = sock;
	}
	i++;
    }
}

int
main(int argc, char **argv)
{
    int datasize;
    int port = PORT;
    int i;

    /* initialize MPI library */
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &comm_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

    datasize = MAX_SEND_SIZE * comm_size;
    sendbuf = emalloc(datasize);
    for (i = 0; i < (datasize / sizeof(int)); i++) {
	((int *)sendbuf)[i] = rand();
    }

    recvbuf = emalloc(datasize);
    memset(recvbuf, 0, datasize);

    recv_req = emalloc(sizeof(struct request) * comm_size);

    exchange_host_info();

    do_dance(port);

    if (comm_rank == 0) {
	printf("# TCP all-to-all\n");
	printf("# len\tMB/s\n");
    }

    /* main loop */
    for (i = 0; lengths[i] != 0; i++) {
	double time0 = 0.0, time1 = 0.0;
	int len = lengths[i];
	int iter;
	int n;

	iter = NNN(len);

	for (n = 0; n < comm_size; n++) {
	    recv_req[n].rank = n;
	    recv_req[n].fd = hosts[n].sock;
	    recv_req[n].len  = len;
	}

	MPI_Barrier(MPI_COMM_WORLD);
	if (comm_rank == 0) /* start time */
	    time0 = MPI_Wtime();

	runtest(len, iter);

	MPI_Barrier(MPI_COMM_WORLD);
	if (comm_rank == 0) { /* end time */
	    int links;
	    time1 = MPI_Wtime();

	    links = comm_size * (comm_size - 1);
	    printf("%d\t%g\n", len,
		   (((double)len * iter * links) / (time1 - time0) / 1.0e6));
            fflush(stdout);
	}
    }

    /* close connections */
    for (i = 0; i < comm_size; i++)
	close(hosts[i].sock);

    free(recv_req);
    free(sendbuf);
    free(recvbuf);

    /* finalize MPI library */
    MPI_Finalize();

    return 0;
}

