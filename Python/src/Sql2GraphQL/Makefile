# $Header: /home/johnl/flnb/code/sql/RCS/Makefile,v 2.1 2009/11/08 02:53:39 johnl Exp $
# Companion source code for "flex & bison", published by O'Reilly
# Media, ISBN 978-0-596-15597-1
# Copyright (c) 2009, Taughannock Networks. All rights reserved.
# See the README file for license conditions and contact info.

CC = cc -g
LEX = flex
YACC = bison
CFLAGS = -DYYDEBUG=1

PROGRAMS9 = glrmysql

all:	${PROGRAMS9}


glrmysql:	glrmysql.tab.o glrmysql.o
	${CC} -o $@ glrmysql.tab.o glrmysql.o

glrmysql.tab.c glrmysql.tab.h:	glrmysql.y
	${YACC} -v glrmysql.y

glrmysql.c:	glrmysql.l
	${LEX} -o $@ $<

glrmysql.o:	glrmysql.c glrmysql.tab.h

clean:
	rm -f glrmysql glrmysql.tab.o glrmysql.o glrmysql.tab.c glrmysql.tab.h \
	glrmysql.c glrmysql.output

.SUFFIXES:	.l .y .c

