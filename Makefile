# Makefile for Postgres ODBC-based API 

run:
	@./bin/pgapi

build:
	@gcc \
	-I/usr/include \
	-o ./bin/pgapi main.c pgapi/pgapi.c \
	-L/usr/lib/x86_64-linux-gnu -lodbc \
	-DODBC64 \

all: build run