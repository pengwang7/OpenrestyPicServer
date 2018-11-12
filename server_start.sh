#!/bin/bash

config_path=`pwd`
nginx_path="/usr/local/openresty/nginx/sbin/nginx"

${nginx_path} -p ${config_path} -c ${config_path}/conf/nginx.conf &
