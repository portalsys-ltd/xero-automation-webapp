# config.py

import os

from kombu import Connection
import ssl

basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    """Base configuration."""
    SECRET_KEY = os.environ.get('SECRET_KEY', 'you-will-never-guess')

    database_url = os.environ.get('DATABASE_URL', 'sqlite:///' + os.path.join(basedir, 'app.db'))
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    SQLALCHEMY_DATABASE_URI = database_url

    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Celery Configuration
    CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
    CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')

    # Add SSL options for rediss:// URLs
    BROKER_USE_SSL = {
        'ssl_cert_reqs': ssl.CERT_NONE  # Use ssl.CERT_NONE for self-signed certificates or Heroku Redis
    }

    RESULT_BACKEND_USE_SSL = {
        'ssl_cert_reqs': ssl.CERT_NONE  # Same as above
    }


    # Default ENV set to 'development', change it if needed in production
    ENV = os.environ.get('FLASK_ENV', 'production')
    DEBUG = False

class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True
    CLIENT_ID = os.environ.get('DEV_CLIENT_ID', 'E65D6D2CD7B6438C8FC7BBE21764826A')  # Default for dev
    CLIENT_SECRET = os.environ.get('DEV_CLIENT_SECRET', 'mMxOFA2nRbHZDdgK_QDEa8z_Nhx-Ym2UE-o_rOavjQhykzPG')  # Default for dev
    ENV = 'development'

class TestingConfig(Config):
    """Testing configuration."""
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'
    CELERY_ALWAYS_EAGER = True  # Executes tasks synchronously for testing
    ENV = 'development'
    DEBUG = True

class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False
    ENV = 'production'
    CLIENT_ID = os.environ.get('PROD_CLIENT_ID', 'E65D6D2CD7B6438C8FC7BBE21764826A')  # Fetch from environment
    CLIENT_SECRET = os.environ.get('PROD_CLIENT_SECRET', 'mMxOFA2nRbHZDdgK_QDEa8z_Nhx-Ym2UE-o_rOavjQhykzPG')  # Fetch from environment
    # Add more production-specific configurations here, like secure cookies, HTTPS, etc.

config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}
