mysql --user=$2 --password=$3 $1 < scripts/delete.sql
mysql --user=$2 --password=$3 $1 < scripts/make.sql