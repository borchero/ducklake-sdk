SNAPSHOT=$1
if [[ -z "$SNAPSHOT" ]]; then
  echo "Usage: $0 <version>"
  exit 1
fi

set -euo pipefail
cd "$(dirname "$(realpath $0)")/$SNAPSHOT"
ENV=ducklake-${SNAPSHOT/./-}

# Remove old snapshots and reset databases
mkdir -p catalogs
rm -f catalogs/*.sql

# Take snapshot of SQLite metadata
echo "Taking snapshot of SQLite metadata..."
pixi run -e $ENV duckdb \
  < <(cat ../init/sqlite.sql run.sql)
pixi run -e $ENV sqlite3 metadata.sqlite .dump > catalogs/sqlite.sql
rm -f metadata.sqlite

# Take snapshot of Postgres database
echo "Taking snapshot of Postgres metadata..."
docker container rm -f snapshot-postgres >/dev/null 2>&1 || true
docker run \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  --name snapshot-postgres \
  --detach \
  postgres:latest \
  >/dev/null 2>&1
sleep 5
pixi run -e $ENV duckdb \
  < <(cat ../init/postgres.sql run.sql)
docker exec snapshot-postgres \
  pg_dump -U postgres postgres \
  > catalogs/postgres.sql
docker container rm -f snapshot-postgres >/dev/null 2>&1

# Take snapshot of MySQL database
echo "Taking snapshot of MySQL metadata..."
docker container rm -f snapshot-mysql >/dev/null 2>&1 || true >/dev/null
docker run \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=snapshot \
  -p 3307:3306 \
  --name snapshot-mysql \
  --detach \
  mysql:latest \
  >/dev/null 2>&1
sleep 10
pixi run -e $ENV duckdb \
  < <(cat ../init/mysql.sql run.sql)
docker exec snapshot-mysql \
  mysqldump -u root -proot snapshot --single-transaction --set-gtid-purged=OFF \
  > catalogs/mysql.sql
docker container rm -f snapshot-mysql >/dev/null 2>&1

# Stopping databases
rm -f data_files*/
echo "Done!"
