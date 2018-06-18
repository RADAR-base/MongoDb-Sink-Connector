# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM openjdk:8-alpine as builder

RUN mkdir /code
WORKDIR /code

ENV GRADLE_OPTS -Dorg.gradle.daemon=false -Dorg.gradle.project.profile=docker

COPY ./gradle/wrapper /code/gradle/wrapper
COPY ./gradlew /code/
RUN ./gradlew --version

COPY ./gradle/profile.docker.gradle /code/gradle/
COPY ./build.gradle ./gradle.properties ./settings.gradle /code/

RUN ./gradlew downloadDependencies copyDependencies

COPY ./src/ /code/src

RUN ./gradlew jar

FROM confluentinc/cp-kafka-connect-base:4.1.0

MAINTAINER Nivethika M <nivethika@thehyve.nl> , Joris B <joris@thehyve.nl> , Yatharth R <yatharth.ranjan@kcl.ac.uk>

LABEL description="Kafka MongoDB Sink Connector"

ENV CONNECT_PLUGIN_PATH /usr/share/java/kafka-connect/plugins

COPY --from=builder /code/build/third-party/*.jar ${CONNECT_PLUGIN_PATH}/kafka-connect-mongodb-sink/
COPY --from=builder /code/build/libs/*.jar ${CONNECT_PLUGIN_PATH}/kafka-connect-mongodb-sink/

COPY ./src/main/docker/kafka-wait /usr/bin/kafka-wait

# Load modified launcher
COPY ./src/main/docker/launch /etc/confluent/docker/launch
