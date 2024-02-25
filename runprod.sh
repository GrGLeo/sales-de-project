until nc -z database 5432; do
    echo "Waiting for PostgreSQL to be ready..."
    sleep 1
done

# Proceed with script actions that require the database
echo "PostgreSQL is ready. Proceeding with script actions..."

python run.py >> /var/log/cron.log