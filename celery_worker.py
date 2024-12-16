# celery_worker.py

from app import create_app, make_celery
import os

# Print environment variables for debugging
print("BROKER_URL:", os.environ.get("BROKER_URL"))
print("RESULT_BACKEND:", os.environ.get("RESULT_BACKEND"))


config_name = os.getenv('FLASK_CONFIG') or 'default'
app = create_app(config_name)
celery = make_celery(app)  # Initialize Celery with the app


# No additional configuration needed here since Celery is already set up in the factory
# code to strat worke,  celery -A celery_worker.celery worker --loglevel=info,  ~ % redis-server

if __name__ == '__main__':
    celery.start()


