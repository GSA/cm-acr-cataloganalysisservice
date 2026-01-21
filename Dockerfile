ARG image_version="20260104"

FROM 752281881774.dkr.ecr.us-east-1.amazonaws.com/odp_ubuntu24_openjdk17:20260118 AS builder

COPY src src
COPY pom.xml pom.xml

RUN mvn -B -DskipTests clean install verify


# --- copy jar file from previous stage
ARG image_version
FROM 752281881774.dkr.ecr.us-east-1.amazonaws.com/odp_ubuntu24_openjdk17:20260118

RUN mkdir -p ./external-libs/datadogjar/
ADD --chown=gsa-user:gsa-user 'https://dtdg.co/latest-java-tracer' ./external-libs/datadogjar/dd-java-agent.jar
RUN chmod 755 ./external-libs/datadogjar/dd-java-agent.jar

COPY --from=builder /home/gsa-user/app/target/cm-acr-cataloganalysisservice-0.0.1-SNAPSHOT.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
