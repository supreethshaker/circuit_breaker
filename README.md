# 3-Phase Circuit Breaker in Proxy Server

## Description
A 3-phase circuit breaker is a control mechanism for disparater services communicating with each other.
For this project, we take an example of a proxy server which forwards requests to a web-server. The proxy server will have a control mechanism, i.e. the circuit breaker to monitor if a service is up and running. The Circuit Breaker can be in 3 states (CLOSED, OPEN, or HALF-OPEN), and changes depending on successful or unsuccessful calls to the web-server.

+ If the Circuit breaker is in a CLOSED state, it will forward requests to the remote service.
+ If requests made to the remote service timeout or are dropped, a failure has occured. When consecutive failures reach a limit (default 3), the Circuit Breaker moves to an OPEN state. 
+ If the Circuit breaker is in the OPEN state and is asked to make a request to the remote service, it will typically not forward requests to the remote service (hence, protecting the callee from making request to a supposedly down remote service).
+ If the Circuit breaker is in the OPEN state and is asked to make a request to the remote service, and the time since it changed to the OPEN state has exceeded the recovery time (default 30 seconds), the Circuit Breaker will move to the HALF-OPEN state and immediately try the request again.
+ If the Circuit breaker is in the HALF-OPEN state and is making the request to the remote service, the Circuit Breaker will move to a CLOSED state upon successful response, and move to a OPEN state upon unsuccessful response.

## Installation
### Activate the virtual environment
### Install required packages

## Running the services
> For this project, you will need to run
+ The web application (`app.py`)
+ The proxy server (`proxy.py`)

### Web application
The web application is run on Flask. For the demonstration of the project, the Flask service exposes very rudimentary APIs that simply return a JSON response.
+ `python3 app.py`

### Proxy server
+ `python3 proxy.py`
The proxy server is implemented using the `Twisted` library. The `proxy` service acts as a client making requests on behalf of the user to the forward server. 

#### Proxy vs Reverse Server
+ A proxy server forwards requests to a different server.
+ A reverse proxy is like the above, only, it sits inside a network of machine, to forward the requests to servers within the network.


### Circuit Breaker
A custom circuit breaker is implemented on top of the `Proxy` service. Network requests to the forward server are monitored by the Circuit Breaker, and consecutive failures trigger the circuit breaker.
Once triggered, the circuit breaker protects the client by not allowing the proxy server to continue making requests to the supposedly down server, until recovery timeout has passed.
See above description for explanation of circuit breaker states.