web: python manage.py collectstatic --noinput && python manage.py migrate --run-syncdb && gunicorn project.wsgi --bind 0.0.0.0:$PORT
