# run.py

from dotenv import load_dotenv
load_dotenv()

import os
from app import create_app, db
from app.models import User
from flask_migrate import Migrate

app = create_app(os.getenv('FLASK_CONFIG') or 'default')
migrate = Migrate(app, db)

# Shell context for flask shell
@app.shell_context_processor
def make_shell_context():
    return {'db': db, 'User': User}

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
