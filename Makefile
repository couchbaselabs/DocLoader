.PHONY: all rest_server

all:
	mvn clean compile package

rest_server: all
	java -cp ./target/magmadocloader/magmadocloader.jar RestServer.RestApplication --server.port=8080 --server.name="sirius_java_rest_loader"
