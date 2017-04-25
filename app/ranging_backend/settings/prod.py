from .base import *

DATABASES = {
    'default': {
        'ENGINE': env('DB_ENGINE'),
        'NAME': env('DATABASE_NAME'),
        'USER': env('DATABASE_USER'),
        'PASSWORD': env('DATABASE_PASSWORD'),
        'HOST': env('DATABASE_HOST'),
        'PORT': env('DATABASE_PORT'),
        'OPTIONS': {
            'driver': 'FreeTDS',
            'host_is_server': True,
            'extra_params': "TDS_VERSION=8.0"
        }        
    }
}