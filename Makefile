# Copyright 2020 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
#

EXE_SERVER = bin/kvgo-fs-mount
APP_HOME = /opt/lynkdb/kvgo-fs-mount
APP_USER = kvgo

all:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ${EXE_SERVER} cmd/server/main.go

install:
	mkdir -p ${APP_HOME}/bin
	mkdir -p ${APP_HOME}/etc
	mkdir -p ${APP_HOME}/var/log
	cp -rp misc ${APP_HOME}/ 
	install -m 755 ${EXE_SERVER} ${APP_HOME}/${EXE_SERVER}
	# id -u ${APP_USER} &>/dev/null || useradd -d ${APP_HOME} -s /sbin/nologin ${APP_USER}
	id -u ${APP_USER} || useradd -d ${APP_HOME} -s /sbin/nologin ${APP_USER}
	chown -R ${APP_USER}:${APP_USER} ${APP_HOME}
	install -m 600 misc/systemd/systemd.service /lib/systemd/system/kvgo-fs-mount.service
	systemctl daemon-reload

clean:
	rm -f ${EXE_SERVER}

