all: sendfile Makefile
sendfile: sendfile.o
sendfile.o: sendfile.c
	$(CC) -Wall -g -c -o $@ $<

.PHONY: clean
clean:
	rm -f *~ sendfile sendfile.o

