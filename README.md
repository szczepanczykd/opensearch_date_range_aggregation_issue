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

# Update 2.3.0

This issue was fixed in the version 2.3.0

Now the test is passing :)
