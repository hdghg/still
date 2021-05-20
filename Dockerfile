FROM openjdk:8-jre
COPY lib/*.jar /lib/
COPY target/*.jar /lib/
ENTRYPOINT ["java", "-cp", "lib/*", "com.github.atokar.App"]
