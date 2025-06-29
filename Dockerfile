FROM openjdk:17-jdk-slim

WORKDIR /app

COPY target/agro-monitor-prod.jar app.jar

COPY data/raw /app/data/raw

RUN apt-get update && apt-get install -y curl unzip \
 && curl -L https://github.com/duckdb/duckdb/releases/download/v0.10.2/duckdb_cli-linux-amd64.zip -o duckdb.zip \
 && unzip duckdb.zip && mv duckdb /usr/local/bin/duckdb && rm duckdb.zip \
 && apt-get clean && rm -rf /var/lib/apt/lists/* \

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]