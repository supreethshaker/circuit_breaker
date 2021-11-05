import requests
import time
from twisted.python import log
import sys

#We assume clientside timeout typically is 30 seconds.
#Can specify via timeout
class CircuitBreaker:
    def __init__(self, url=None, failure_threshold=3, recovery_timeout=30):
        self.failure_threshold=failure_threshold
        self.recovery_timeout=recovery_timeout
        self.state = "CLOSED"
        self.failure_count = 0
        self.url = url
    
    def incrementFailureCount(self):
        self.failure_count += 1

    def handleSuccessfulConnection(self):
        log.msg(f"CircuitBreaker::handleSuccessfulConnection")
        self.state = "CLOSED"
        self.failure_count = 0

    def attach(self, url):
        log.msg(f"CircuitBreaker::attach")
        self.url = url

    def doHttpGetRequest(self, timeout=30):
        log.msg(f"CircuitBreaker::doHttpGetRequest")
        if(self.url == None):
            log.msg(f"CircuitBreaker::doHttpGetRequest::No URL")
            print("Please attach a url for this CircuitBreaker object.")
            return None

        if(self.state == "CLOSED" or self.state == "HALF_OPEN"):
            log.msg(f"CircuitBreaker::doHttpGetRequest::state==CLOSED||state==HALF_OPEN")
            print("Trying to execute the http GET request...")
            try:
                response = requests.get(self.url, timeout=timeout)
                print("Successful execution. Returning response.")
                self.handleSuccessfulConnection()
                return response
            except requests.exceptions.ReadTimeout as e1:
                print("Timeout Exception Occured: " + str(e1))
                self.handleFailedConnection()
                return None
            except requests.exceptions.ConnectionError as e2:
                print("Connection Error Occured: " + str(e2))
                self.handleFailedConnection()
                return None
            except Exception as e3:
                print("Other exception occured: " + str(e3))
                self.handleFailedConnection()
                return None
        elif(self.state == "OPEN"):
            log.msg(f"CircuitBreaker::doHttpGetRequest::state==OPEN")
            currentTime = time.time()

            if(currentTime - self.failedTime > self.recovery_timeout):
                print("Recovery timeout has passed. Going to half open state and retrying API...")
                self.state = "HALF_OPEN"

                response = self.doHttpGetRequest()
                if(response):
                    print("Execution in half open state succeeded. Returning response.")
                else:
                    print("Execution in half open state failed. Returning None response.")
                return response
            else:
                print("Circuit Breaker is in open state. Will not try connecting to API.")
                return None


    def handleFailedConnection(self):
        log.msg(f"CircuitBreaker::handleFailedConnection")
        self.failedTime = time.time()

        if(self.state == "CLOSED"):
            self.failure_count += 1

            if(self.failure_count == self.failure_threshold):
                self.state = "OPEN"
        elif (self.state == "OPEN"):
            pass
        elif (self.state == "HALF_OPEN"):
            self.state = "OPEN"
        
    
    """""
    If state is open we wait for a response from the server
    We make infinite requests to the server at an increasing interval of time
    When we get more than 3 good responses we turn the state to closed 
    """""
    def poll_if_server_alive(self):
        if self.state != "OPEN": 
            return 

        back_off = 1
        good_requests = 0
        while True:
            resp = requests.get(self.url, timeout=30)
            if not resp: 
                back_off += 1
                time.sleep(2 * back_off)
                continue
            good_requests += 1
            if good_requests > 3:
                self.state = "CLOSED"
                break
            time.sleep(1)
        

# Test function. 
def main():
    log.startLogging(sys.stdout)
    circuitBreaker = CircuitBreaker("http://127.0.0.1:5000/studentInfo")
    while(1):
        response = circuitBreaker.doHttpGetRequest()
        time.sleep(10)

if __name__ == "__main__":
    main()
