# temp container to build using gradle
FROM gradle:alpine AS TEMP_BUILD_IMAGE
ENV APP_HOME=/usr/app/
WORKDIR $APP_HOME
COPY settings.gradle.kts $APP_HOME
COPY gradle $APP_HOME/gradle
COPY app $APP_HOME/app
USER root
RUN ls
RUN gradle installDist || return 0
COPY . .
# actual container
FROM bellsoft/liberica-openjdk-alpine
ENV APP_HOME=/usr/app/
WORKDIR $APP_HOME
COPY --from=TEMP_BUILD_IMAGE $APP_HOME/app/build/install/app .
ENTRYPOINT bin/app
