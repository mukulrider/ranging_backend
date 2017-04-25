"""
WSGI config for pricing_backend project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.10/howto/deployment/wsgi/
"""

import os
import environ

from django.core.wsgi import get_wsgi_application

ROOT_DIR = environ.Path(__file__) - 2

env = environ.Env()
env_file = str(ROOT_DIR.path('.env'))
print('Loading : {}'.format(env_file))
env.read_env(env_file)
print('The .env file has been loaded. See base.py for more information')

os.environ.setdefault("DJANGO_SETTINGS_MODULE", env('SETTINGS_MODULE'))

application = get_wsgi_application()
