FROM mysql:8.0

# Use a path outside the VOLUME declared by the base image (/var/lib/mysql),
# otherwise Docker discards RUN writes to that path at build time.
RUN mkdir -p /mysqldata && chown mysql:mysql /mysqldata

RUN mysqld --initialize-insecure --datadir=/mysqldata --user=mysql && \
    mysqld --datadir=/mysqldata --user=mysql \
        --skip-log-bin \
        --skip-performance-schema \
        --skip-networking & \
    until mysqladmin -u root ping --silent 2>/dev/null; do sleep 0.1; done && \
    mysql -u root -p"" -e "ALTER USER 'root'@'localhost' IDENTIFIED BY 'sqltgen_root';" && \
    mysql -u root -psqltgen_root -e "CREATE USER 'root'@'%' IDENTIFIED BY 'sqltgen_root';" && \
    mysql -u root -psqltgen_root -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;" && \
    mysql -u root -psqltgen_root -e "CREATE USER 'sqltgen'@'%' IDENTIFIED BY 'sqltgen';" && \
    mysql -u root -psqltgen_root -e "CREATE DATABASE sqltgen;" && \
    mysql -u root -psqltgen_root -e "GRANT ALL PRIVILEGES ON *.* TO 'sqltgen'@'%' WITH GRANT OPTION;" && \
    mysql -u root -psqltgen_root -e "FLUSH PRIVILEGES;" && \
    mysqladmin -u root -psqltgen_root shutdown && \
    wait
