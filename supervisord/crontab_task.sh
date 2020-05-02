#!/bin/bash
cron_path='/var/spool/cron/root'
task_path='/root/src/Linux-Stock/supervisord/crontab_task'
cat ${task_path} > ${cron_path}

