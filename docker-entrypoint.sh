#!/bin/bash
python3 /home/docker/code/app/manage.py collectstatic --noinput  # Collect static files

supervisord -n