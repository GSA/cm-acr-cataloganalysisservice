FROM 752281881774.dkr.ecr.us-east-1.amazonaws.com/odp_openjdk17:20230924
RUN mkdir -p ./src
COPY ./src ./src
COPY ./pom.xml ./
RUN mvn -DskipTests clean install verify
RUN find $M2_HOME/ -iname '*.jar'
RUN rm -rf /home/gsa-user/.m2/repository


# --- copy jar file from previous stage
RUN cp ./target/*.jar app.jar
# ENV JAVA_TOOL_OPTIONS "-XX:MaxRAMPercentage=80"
ENTRYPOINT ["java", "-javaagent:./src/datadogjar/dd-java-agent.jar" , "-XX:FlightRecorderOptions=stackdepth=256", "-Ddd.service=cataloganalysisservice", "-Ddd.version=1.0", "-Ddd.profiling.enabled=true", "-jar", "app.jar"]

