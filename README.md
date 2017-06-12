# LEAF = Event-Activated Functions

Build:
```
./mvnw clean install
```

Run RabbitMQ:
```
rabbitmq-server
```

Run the Function Controller:
```
java -jar function-controller/target/function-controller-1.0.0.BUILD-SNAPSHOT.jar &
```

Register a Runner:
```
./scripts/runner-create myrunner file:function-runner/target/function-runner-1.0.0.BUILD-SNAPSHOT.jar
```

## Request Reply HTTP Example

Register a Function:
```
./scripts/function-create uppercase "f->f.map(s->s.toString().toUpperCase())"
```

Bind the Function to a Destination and Specify its Runner:
```
./scripts/function-bind uppercase myrunner words
```

Publish an Event:
```
./scripts/publish-request words "Hello World"
HELLO WORLD
```

## Async Pipeline Example

```
./scripts/function-create time "f->Flux.just(\"the time is: \"+new java.util.Date())"
./scripts/function-create log "f->f.log()"

./scripts/function-bind time myrunner pings notifications
./scripts/function-bind log myrunner notifications

./scripts/trigger-create pings "0/10 * * * * ?"
```

