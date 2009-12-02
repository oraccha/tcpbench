sendfile: sendfile.o
sendfile.o: sendfile.c

.PHONY: clean
clean:
	rm -f *~ sendfile sendfile.o

