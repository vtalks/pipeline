#!/usr/bin/env sh
source environment.sh
supervisord -c /etc/supervisord.conf
python3 main.py