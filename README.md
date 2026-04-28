# dissco-core-digital-specimen-processor

The digital specimen processor can receive data from two different sources:

- Through the API as a request to register a digital specimen
- Through a RabbitMQ queue to register a digital specimen

## Preparation

The digital specimen processor receives objects as a batch.
The first step in the processing is to make sure there are no conflicts inside the batch.
We only want unique objects (based on physicalSpecimenId), so all duplicates will be pushed back to
the queue.

## Evaluation with existing objects

The next step is to get the existing specimen information from the database.
We will then evaluate if the received objects are new, updated or equal to the existing object.
This evaluation is based on the physicalSpecimenId and the full information.

- If the objects was not found in the database, it is seen as new.
- If the object was found but is an exact match, it is seen as equal.
- If the object was found but differs, it is seen as updated.

This will result in three lists of specimens: new, equal and updated.

## New digital specimen

For new digital specimens, we create a new PID and FDO Profile, and transfer the object to a record.
Changing the object to a record means we will add a version (`1`), a timestamp and calculate a
MidsLevel.
The logic behind the calculation of the MidsLevel can be
found [here] (https://github.com/DiSSCo/openDS/blob/master/mids-calculation/intro.md).
After the transfer to a record, we bulk insert the records into the Postgres database.
Next we bulk index the records into Elasticsearch.
If this indexation has been successful, we will publish a CreateUpdateDelete event for each new
specimen.
This message will contain the full newly created object.
The last step is to notify any requested automated annotation services.
If everything is successful, we return the created objects.
These are used as response object if the application is running in the `web` profile (see below).

### Exception handling

There are several moments that the creation of a new specimen could fail.
The creation of the PID and FDO Profile might fail, which will mean we won't be able to process the
specimen.
We will then publish the specimen to the dead letter queue.
If the indexing in Elasticsearch fails, we will roll back the database insert and the handle
creation.
If the publication of the CreateUpdateDelete event fails, we will roll back the database insert, the
handle creation as well as the indexing.

## Updated digital specimen

For updated digital specimen, we need to check whether we also need to update the FDO Profile.
If we need to update the FDO Profile, we will update the FDO Profile and increment the profile's
version.
Next we will recalculate the MidsLevel, as the changed information might have modified the level,
and increment the version of the digital specimen.
We will update the information in the database, overriding the previous information.
After successful database insert, we bulk index the digital specimen, overwriting the old data.
If everything is successful we will publish a CreateDeleteUpdate event to a RabbitMQ Exchange.
This event will hold both the new digital specimen and a JsonPatch with the changes.
We will return the updated records which will be used as response object when the application is
running the `web` profile.

### Exception handling

There are several point where errors could occur.
If the indexing to Elasticsearch fails, we will roll back to the previous version.
This means that we will reinsert the old version of the FDO Profile, and reinsert the old digital
specimen to the database.
If the publishing of the CreateUpdateDelete event fails, we will roll back the FDO Profile, digital
specimen and the indexing.

### Update only Digital Specimen

When only the Digital Specimen is updated, for example when a Virtual Collection Relationship is
added we set the `isDataFromSourceSystem` attribute to `false`.
This indicates that it should ignore any changes in the `digitalMediaEvents` as the DigitalMedia are
not included in the message.
It will keep the existing EntityRelationships to any digital media (if the ERs are in the provided
digitalSpecimen) and will only check the specimen info for equality.
When this property is `true` and the media are not attached it will remove all relationships as it
assumes the media have been removed.
If the Digital Specimen has been updated we will increment the version and update the `lastModified`
timestamp.

## Equal digital specimen

When the stored digital specimen and the received digital specimen are equal, we will only update
the `last_checked` timestamp.
We will do a batch update on the particular field with the current timestamp to indicate the object
were checked and equal at this moment.
We will not return the equal objects as we did not change the data.

## Run locally

To run the system locally, it can be run from an IDEA.
Clone the code and fill in the application properties (see below).
The application needs to store data in a Postgres database and an Elasticsearch instance.
It needs a connection to an RabbitMQ cluster as it publishes messages to one or more queues.

### Domain Object generation

DiSSCo uses JSON schemas to generate domain objects (e.g. Digital Specimens, Digital Media, etc)
based on the openDS specification. These files are stored in the
`/target/generated-sources/jsonschema2pojo directory`, and must be generated before running locally.
The following steps indicate how to generate these objects.

### Importing Up To-Date JSON Schemas

The JSON schemas are stored in `/resources/json-schemas`. The source of truth for JSON schemas is
the [DiSSCO Schemas Site](https://schemas.dissco.tech/schemas/fdo-type/). If the JSON schema has
changed, the changes can be downloaded using the maven runner script.

1. **Update the pom.xml**: The exec-maven-plugin in the pom indicated which version of the schema to
   download. If the version has changed, update the pom.
2. **Run the exec plugin**: Before the plugin can be run, the code must be compiled. Run the
   following in the terminal (or via the IDE interface):

```
mvn compile 
mvn exec:java
```

### Building POJOs

DiSSCo uses the [JsonSchema2Pojo](https://github.com/joelittlejohn/jsonschema2pojo) plugin to
generate domain objects based on our JSON Schemas. Once the JSON schemas have been updated, you can
run the following from the terminal (or via the IDE interface):

```
mvn clean
mvn jsonschema2pojo:generate
```

## Run as Container

The application can also be run as container.
It will require the environmental values described below.
The container can be built with the Dockerfile, which can be found in the root of the project.

## Profiles

There are four profiles with which the application can be run:

### Web (Synchronous)

`spring.profiles.active=web`  
This listens to an API which has two endpoint:

- `POST /`
  This endpoint can be used to post a digital specimen event to the processing service in the old
  model.
  The endpoint is deprecated and will be replaced with the implementation at `/new` at a later
  moment.
  The only current user is the OpenRefine pilot.
  Calls to this endpoint are formatted to the new model and send to the `/new` endpoint.
- `POST /new`
  This endpoint uses the new model and will replace the endpoint at `/`
  It can be used to add a new digital specimen to DiSSCo.
  Calls to the endpoint will trigger the processing (described above).
  The result of the processing will be returned to the user.
  It can only be called with a single digital specimen and does not support batches.

If an exception occurs during processing, the message will be published to the RabbitMQ Dead Letter
queue.
We can than later evaluate why the exception was thrown and if needed, retry the object.

### Specimen RabbitMQ (Asynchronous)

`spring.profiles.active=specimen-rabbitmq`
This will make the application listen to a specified queue and process the digital specimen events
from the queue.
We collect the objects in batches of between 300-500 (depending on the length of the queue).
If any exception occurs we publish the event to a Dead Letter Queue where we can evaluate the
failure and if needed retry the messages.

### Media RabbitMQ (Asynchronous)

`spring.profiles.active=media-rabbitmq`
This will make the application listen to a specified queue and process the digital media events
from the queue.
We collect the objects in batches of between 300-500 (depending on the length of the queue).
If any exception occurs we publish the event to a Dead Letter Queue where we can evaluate the
failure and if needed retry the messages.

### Entity Relationship RabbitMQ (Asynchronous)

`spring.profiles.active=er-rabbitmq`
This will make the application listen to a specified queue and process the digital media tombstone
events
from the queue. This queue unlinks media and specimen entity relationships/
We collect the objects in batches of between 300-500 (depending on the length of the queue).
If any exception occurs we publish the event to a Dead Letter Queue where we can evaluate the
failure and if needed retry the messages.

## Environmental variables

The following backend specific properties can be configured:

```
# Database properties
spring.datasource.url=# The JDBC url to the PostgreSQL database to connect with
spring.datasource.username=# The login username to use for connecting with the database
spring.datasource.password=# The login password to use for connecting with the database

#Elasticsearch properties
elasticsearch.hostname=# The hostname of the Elasticsearch cluster
elasticsearch.port=# The port of the Elasticsearch cluster
elasticsearch.index-name=# The name of the index for Elasticsearch

# RabbitMQ properties (only necessary when the rabbitMQ profile is active)
spring.rabbitmq.password=# The password to use for connecting with RabbitMQ
spring.rabbitmq.username=# The username to use for connecting with RabbitMQ
spring.rabbitmq.host=# The hostname of the RabbitMQ cluster

# Oauth2 properties
spring.security.oauth2.resourceserver.jwt.issuer-uri=# The endpoint of the jwt issuer
spring.security.oauth2.client.registration.dissco.client-secret=
spring.security.oauth2.client.registration.dissco.client-id=
spring.security.oauth2.client.registration.dissco.authorization-grant-type=
spring.security.oauth2.client.provider.dissco.token-uri=
spring.security.oauth2.resourceserver.jwt.issuer-uri=
spring.security.oauth2.authorizationserver.endpoint.jwk-set-uri=

# Other
pid.endpoint = PID Manager api endpoint
```