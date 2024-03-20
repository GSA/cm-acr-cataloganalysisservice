FROM 752281881774.dkr.ecr.us-east-1.amazonaws.com/odp_openjdk17:20230924
RUN mkdir -p ./src
COPY ./src ./src
COPY ./pom.xml ./
RUN mkdir -p ./external-libs/datadogjar/
ADD --chown=gsa-user:gsa-user 'https://dtdg.co/latest-java-tracer' ./external-libs/datadogjar/dd-java-agent.jar
RUN chmod 755 ./external-libs/datadogjar/dd-java-agent.jar
RUN mvn -DskipTests clean install verify
RUN find $M2_HOME/ -iname '*.jar'
RUN rm -rf /home/gsa-user/.m2/repository

# --- copy jar file from previous stage
RUN cp ./target/*.jar app.jar
# ENV JAVA_TOOL_OPTIONS "-XX:MaxRAMPercentage=80"

ENTRYPOINT ["java", "-jar", "app.jar"]
