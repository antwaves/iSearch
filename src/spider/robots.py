import urllib
import urllib.robotparser

class robotsTxt:
    def __init__(self, parser, crawl_delay, request_rate):
        self.parser = parser
        self.crawl_delay = crawl_delay
        self.request_rate = request_rate


def check_robots(link: str, robot_dict: dict):
    r_parser = urllib.robotparser.RobotFileParser()
    
    parseResult = urllib.parse.urlparse(link)
    if parseResult.netloc.startswith("www."):
        domainName = parseResult.netloc.split("www.")[1]
    else:
        domainName = parseResult.netloc
    domainUrl = parseResult.scheme + "://" + domainName

    #check if this has already been grabbed
    if domainUrl in robot_dict.keys():
        return robot_dict[domainUrl]
    robotsUrl = parseResult.scheme + "://" + parseResult.netloc + "/robots.txt"

    #grab the robots.txt
    try:
        r_parser.set_url(robotsUrl)
        r_parser.read()
        print(f"Grabbed robots.txt for {link}")
    except Exception as e:
        print(f"Robots.txt failed to be grabbed at {link} with error {e}")
        return None

    if r_parser:    
        #get rate-limits to not get banned by websites
        crawl_delay = r_parser.crawl_delay("iSearch")
        request_rate = r_parser.request_rate("iSearch")

        if request_rate:
            request_rate = request_rate.seconds / request_rate.requests

        robot_dict[domainUrl] = robotsTxt(r_parser, crawl_delay, request_rate)
        return robot_dict[domainUrl]
    else:
        robot_dict[domainUrl] = None
        return None
