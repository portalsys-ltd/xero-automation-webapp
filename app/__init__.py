# app/__init__.py

from flask import Flask
from config import config
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from celery import Celery
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_login import LoginManager  # Import LoginManager
from app.admin_views import UserAdmin  # Import UserAdmin
import os
from werkzeug.security import generate_password_hash
from flask import flash
from flask_session import Session


# Initialize extensions
db = SQLAlchemy()
migrate = Migrate()
celery = Celery(__name__)
login_manager = LoginManager()  # Initialize Flask-Login

def make_celery(app):
    """Create and configure Celery."""
    celery = Celery(
        app.import_name,
        broker=app.config['BROKER_URL'],  # Updated key
        backend=app.config['CELERY_RESULT_BACKEND'],  # Updated key

    )
    celery.conf.update(app.config)

    

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)
        
    celery.Task = ContextTask
    celery.autodiscover_tasks(['app.celery_tasks'])  # Ensure tasks are discovered
    return celery



def create_app(config_name=None):
    """Application factory pattern to create a Flask app instance."""
    if config_name is None:
        config_name = os.getenv('FLASK_CONFIG') or 'default'

    app = Flask(__name__)
    app.config.from_object(config[config_name])

    # Debugging - print the configuration to verify
    print(f"Loaded configuration: {config_name}")
    print(f"BROKER_URL: {app.config.get('BROKER_URL')}")
    print(f"CELERY_RESULT_BACKEND: {app.config.get('CELERY_RESULT_BACKEND')}")

    # Allow OAuth2 loop to run over http for local testing
    if app.config["ENV"] != "production":
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    # Initialize extensions with the app
    db.init_app(app)
    migrate.init_app(app, db)
    login_manager.init_app(app)  # Attach LoginManager to the app
    login_manager.login_view = 'auth.user_login'  # Redirect to login page if not authenticated
    celery = make_celery(app)


    #with app.app_context():
            #db.create_all()  # Ensure all models are created if not already

    # Import models after initializing db to avoid circular import issues
    from app.models import User

    # Initialize Flask-Admin
    admin = Admin(app, name='Admin Panel', template_mode='bootstrap3')

    # Register the custom UserAdmin view with Flask-Admin
    admin.add_view(UserAdmin(User, db.session))



    # Initialize Flask-Session
    app.config['SESSION_TYPE'] = 'filesystem'  # Configure session storage
    app.config['SECRET_KEY'] = 'your_secret_key_here'  # Ensure you have a strong secret key
    Session(app)  # Initialize session with app

    # Load user function for Flask-Login
    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))  # Flask-Login needs to load the user from the user ID


    # Register blueprints for routes
    from app.routes.main import main_bp
    from app.routes.auth import auth_bp
    from app.xero import xero_bp  # Import the xero blueprint
    from app.routes.logs import logs_bp  # Import the logs blueprint
    from app.routes.recharging import recharging_bp
    from app.routes.auto_workflows import auto_workflows_bp
    from app.routes.scheduled_tasks import scheduled_tasks_bp

    app.register_blueprint(recharging_bp)
    app.register_blueprint(auto_workflows_bp)
    app.register_blueprint(logs_bp, url_prefix='/logs')
    app.register_blueprint(main_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(xero_bp)  # Xero API routes
    app.register_blueprint(scheduled_tasks_bp)  


    return app

# Add user loader function for Flask-Login
@login_manager.user_loader
def load_user(user_id):
    from app.models import User
    user_name = User.query.get(int(user_id))
    return user_name