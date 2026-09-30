FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update && \
    apt-get install -y \
        curl \
        libxrender1 \
        libjpeg-turbo8 \
        fontconfig \
        libxtst6 \
        xfonts-75dpi \
        xfonts-base && \
    rm -rf /var/lib/apt/lists/*

COPY cb-search-service-0.0.1-SNAPSHOT.jar /opt/

CMD ["/bin/bash", "-c", "java -XX:+PrintFlagsFinal $JAVA_OPTIONS -XX:+UnlockExperimentalVMOptions -jar /opt/cb-search-service-0.0.1-SNAPSHOT.jar"]
