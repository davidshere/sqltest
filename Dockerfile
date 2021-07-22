FROM openjdk:16-alpine3.13

RUN apk add wget

RUN wget https://github.com/sbt/sbt/releases/download/v1.5.5/sbt-1.5.5.tgz

COPY build.sbt .
COPY src/ src
CMD ["sbt", "run"]