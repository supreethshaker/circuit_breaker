import sys
from twisted.python import log
from twisted.web import http, proxy, client
from twisted.internet import endpoints, reactor
from CircuitBreaker import CircuitBreaker
import time

PROXY_URL = "http://localhost:5000/test-path"
CONNECTION_STR = "tcp:8080:interface=127.0.0.1"
circuitBreaker = CircuitBreaker(PROXY_URL)

"""
Proxy class usage
Successful response
        ProxyRequest::requestReceived
        ProxyRequest::process
        ProxyClient::connectionMade
        ProxyClient::handleStatus
        ProxyClient::dataReceived
        ProxyClient::handleHeader
        ProxyClient::handleEndHeaders
        ProxyClient::rawDataReceived
        ProxyClient::handleResponsePart
        ProxyClient::handleResponseEnd
        ProxyClient::connectionLost
        ProxyClient::handleResponseEnd
        ProxyClientFactory::clientConnectionLost
        Proxy::connectionLost
    Response on proxy server failing
        ProxyRequest::requestReceived
        ProxyRequest::process
        ProxyClientFactory::clientConnectionFailed
        Proxy::connectionLost
"""

"""Proxy client intercepts the request and handles response"""
class ProxyClient(proxy.ProxyClient):
    """
    This class proxies the request to the PROXY_URL
    """
    def handleHeader(self, key, value):
        log.msg(f"ProxyClient::handleHeader")
        proxy.ProxyClient.handleHeader(self, key, value)

    def handleResponsePart(self, buffer):
        # change response part here
        log.msg(f"ProxyClient::handleResponsePart")
        # buffer = circuitBreaker.doHttpGetRequest()
        proxy.ProxyClient.handleResponsePart(self, buffer)

    def handleStatus(self, version, code, message):
        log.msg(f"ProxyClient::handleStatus")
        return super().handleStatus(version, code, message)

    def connectionLost(self, reason):
        log.msg(f"ProxyClient::connectionLost")
        return super().connectionLost(reason)

    def connectionMade(self):
        log.msg(f"ProxyClient::connectionMade")
        return super().connectionMade()

    def handleResponseEnd(self):
        log.msg(f"ProxyClient::handleResponseEnd")
        return super().handleResponseEnd()

    def handleEndHeaders(self):
        log.msg(f"ProxyClient::handleEndHeaders")
        return super().handleEndHeaders()

    def rawDataReceived(self, data):
        return super().rawDataReceived(data)

    def dataReceived(self, data):
        log.msg(f"ProxyClient::dataReceived")
        circuitBreaker.handleSuccessfulConnection()
        return super().dataReceived(data)

class ProxyClientFactory(proxy.ProxyClientFactory):
    protocol = ProxyClient

    def clientConnectionFailed(self, connector, reason):
        circuitBreaker.handleFailedConnection()        
        return super().clientConnectionFailed(connector, reason)

    def clientConnectionLost(self, connector, reason):
        log.msg(f"ProxyClientFactory::clientConnectionLost")
        return super().clientConnectionLost(connector, reason)

""" Handle the request that is to be made by the proxy """
class ProxyRequest(proxy.ProxyRequest):
    protocols = {
        http: ProxyClientFactory,
        bytes('http', "utf8"): ProxyClientFactory
    }

    def __init__(self, channel, queued=http._QUEUED_SENTINEL, reactor=reactor):
        super().__init__(channel, queued=queued, reactor=reactor)

    def requestReceived(self, command, path, version):
        path = bytes(PROXY_URL, "utf8")
        return super().requestReceived(
            command, 
            path, 
            version
        )

    def process(self):
        log.msg(f"ProxyRequest::process")
        if(circuitBreaker.state == "OPEN"):
            currentTime = time.time()

            if(currentTime - circuitBreaker.failedTime > circuitBreaker.recovery_timeout):
                circuitBreaker.state = "HALF_OPEN"
                proxy.ProxyRequest.process(self)
            else:
                self.setResponseCode(502)
                self.write(b"<html><body>Service Down</body></html>")
                self.finish()
                return None
        elif (circuitBreaker.state == "CLOSED"):
            proxy.ProxyRequest.process(self)
            

    def connectionLost(self, reason):
        log.msg(f"ProxyRequest::ConnectionLost")
        return super().connectionLost(reason)

""" The proxy itself which forwards the request"""     
class Proxy(proxy.Proxy):
    requestFactory = ProxyRequest

    def timeoutConnection(self):
        log.msg(f"Proxy::timeoutConnection")
        return super().timeoutConnection()

    def connectionLost(self, reason):
        log.msg(f"Proxy::connectionLost")
        return super().connectionLost(reason)

""""""
class ProxyFactory(http.HTTPFactory):
    protocol = Proxy

if __name__ == '__main__': 

    def shutdown(reason, reactor, stopping=[]):
        """Stop the reactor"""
        if stopping: 
            return
        stopping.append(True)
        if reason:
            log.msg(reason.value)
        reactor.callWhenRunning(reactor.stop)

    log.startLogging(sys.stdout)

    endpoint = endpoints.serverFromString(reactor, CONNECTION_STR)
    proxy_server = endpoint.listen(ProxyFactory())
    proxy_server.addErrback(shutdown, reactor)
    reactor.run()