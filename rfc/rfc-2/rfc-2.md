# RFC-2: Metadata Connector Design

## Design Prerequisites

The Unified-Catalog receives multiple sources as input, stores associated metadata, 
and outputs this information through a RESTful api to support the Unified-Catalog UI.

When first designing the Unified-Catalog, one of the major decisions we had to make 
was whether we would store the metadata we collected or extract it on request. 
Our service needs to support high throughput and low latency reads, and if we delegate 
this responsibility to metadata sources, we need all sources to support high throughput 
and low latency reads, which introduces complexity and risk.
For example, a Vertica query that gets a table schema often takes a few seconds to process, 
making it unsuitable for visualization. Similarly, our Hive metastore manages 
all of Hive's metadata, making it risky to require high throughput read requests. 

Because the Unified-Catalog supports so many different sources of metadata, 
we decided to store the metadata in the Unified-Catalog architecture itself. 

In addition, while most use cases require new metadata, they do not need to see 
the metadata change in real time, making periodic crawling possible.

We also separate the request service layer from the data collection layer so that 
each layer runs in a separate process, as shown in Figure 1 below:
![rfc-2-01.png](rfc-2-01.png)

This isolates the two layers, which reduces collateral impact. 
For example, a data collection crawl job may use a large amount of system resources, 
which may affect the SLA of the api at the request service layer. 

In addition, the data collection layer is less sensitive to outages than the 
Unified-Catalog's request service layer, ensuring that outdated metadata will still 
be served if the data collection layer goes down, thus minimizing the impact on users.

## Plug-in mode
We need to support many kinds of data sources, we not only need to provide a modular connector design, 
we also need to enable the connector to be integrated in the system as a plug-in, 
which will better enable developers to extend more data sources without recompiling the whole Unified-Catalog project.

## Event-based versus scheduled collection
Our next challenge was to determine the most effective and efficient way to collect metadata 
from several different and completely disparate data sources.

We started by creating crawlers that regularly collect information from various data sources 
and microservices that generate metadata information about the dataset.

We needed to collect metadata information frequently in a scalable way without blocking 
other crawler tasks. To do this, we deployed the crawlers to different machines and 
needed to coordinate efficiently among the crawlers in a distributed manner. 
We considered configuring [Quartz](https://github.com/quartz-scheduler/quartz) 
in cluster mode for distributed scheduling (supported by MySQL).

In addition, to accommodate future cloud deployment models, 
we need to deploy the crawler to different hosts and multiple data centers in Docker containers.

## Metadata Type System
We use Substrait’s type system to define our metadata schema’s type, 
Substrait’s type system can be referred here https://substrait.io/types/type_system/.
In this way, different engines can read and write the same data based on the same schema and field type. 
Instead of each engine having to build a separate external table, 
multiple engines can be seamlessly connected in the future.

## Multi-version
We need to support multi-version for metadata, Each time we find an existing change in the metadata,
We save all history version data by adding a new version metadata record.
