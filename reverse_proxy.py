from twisted.internet import reactor
from twisted.web import proxy, server, http

class ReverseProxy():
    def __init__(self):
        self.port = 8080
        self.proxy_url = 'localhost'
        self.proxy_port = 5000
        self.proxy_path = ''.encode("utf-8")
        self.reactor = reactor
        self.site = server.Site(
            proxy.ReverseProxyResource(
                self.proxy_url, 
                self.proxy_port, 
                self.proxy_path
            )
        )

    def dry_run(self):
        print("Dry run")
        
    def run(self):
        print("Running Reverse Proxy service")
        self.reactor.listenTCP(
            self.port, 
            self.site,
        )
        self.reactor.run()

if __name__ == "__main__":
    proxy = ReverseProxy()
    proxy.run()