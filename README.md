# presto-logging-plugin

[![Build status](https://badge.buildkite.com/31ad8b950d1f5a2e45d2704e5a591b2cf478bedc6def64ef94.svg)](https://buildkite.com/shopify/presto-logging-plugin)

[Shipit Pipeline](https://shipit.shopify.io/shopify/presto-logging-plugin/packagecloud)

A Presto plugin which logs completed queries with execution stats to Kafka or Google PubSub.

## Releasing a new version
- Bump the version of the package in pom.xml
- Tag the commit bumping the version with a tag of the form `v<VERSION>` like `v2.6`
- Push the commit and the tag, the ShipIt pipeline will build the JAR and push it to PackageCloud
