#### Setup

Please run OpenSearch:
```
docker-compose up -d
```

Run tests:
```
mvn clean test
```

See failing test:
```
[ERROR]   DateRangeAggregationClientTest.testDateRangeAggregation:50->sendAggregateRequest:70 » JsonParsing Property name 'key' is not in the 'type#name' format. Make sure the request has 'typed_keys' set.
```