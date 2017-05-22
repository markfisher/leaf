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

Register a Function:
```
./scripts/function-create uppercase "f->f.map(s->s.toString().toUpperCase())"
```

Create a Binding:
```
./scripts/binding-create words uppercase myrunner
```

Publish an Event:
```
./scripts/publish-request words "Hello World"
HELLO WORLD
```
