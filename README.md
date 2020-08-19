# IDS Open Data Broker Manager

This is the respository for the manage component of the IDS Open Data Broker.
The managers job is to accept IDS messages from he IDS Open Data Connector and orchestrate the processing the the provided metadata.
The metadata passed to a virtuoso triplestore for querying and (meta)data storage. Further it is passed to the [piveau](https://www.piveau.de/) backend for further processing and so it can be displayed via the brokers frontend.

The main repository of the IDS Open Data Connector is: 

ids-open-data-broker

### Accepted IDS messages
#### Infrastructure messages
* ConnectorAvailableMessage
* ConnectorUnavailableMessage
* ConnectorUpdateMessage
* DescriptionRequestMessage
* ResourceUnavailableMessage
* ResourceAvailableMessage
* ResourceUpdateMessage
#### Data messages
* QueryMessage


## Requirements
* docker
* docker-compose 3.5 or higher
* maven
* DAPS certificates

## Building the Component
* run ``mvn clean package``
* run ``sudo docker build -t odb-manager``