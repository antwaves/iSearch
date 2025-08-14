import urllib
import httpx
import asyncio
import urllib.robotparser

class robotsTxt:
    def __init__(self, parser, crawl_delay, request_rate):
        self.parser = parser
        self.crawl_delay = crawl_delay
        self.request_rate = request_rate


async def check_robots(client, link: str, robot_dict: dict):
    r_parser = urllib.robotparser.RobotFileParser()
    robots_text = None
    
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

    try:
        headers = {'User-Agent': 'iSearch'}
        response = await client.get(robotsUrl, follow_redirects=True, headers=headers)
    except Exception as e:
        robot_dict[domainUrl] = None
        print(f"Robots.txt failed to be grabbed at {link} with error {e}")
        return 

    if response.status_code == httpx.codes.OK:
        robots_text = response.content
    else:
        robot_dict[domainUrl] = None
        return

    if robots_text:
        try:
            lines = robots_text.decode("utf-8", errors="ignore").splitlines()
            r_parser.parse(lines)
        except Exception as e:
            print(f"Robots.txt failed to be parsed at {robotsUrl} with error {e}")
            return

        #get rate-limits to not get banned by websites
        crawl_delay = r_parser.crawl_delay("iSearch")
        request_rate = r_parser.request_rate("iSearch")

        if request_rate:
            request_rate = request_rate.seconds / request_rate.requests

        robot_dict[domainUrl] = robotsTxt(r_parser, crawl_delay, request_rate)
        return robot_dict[domainUrl]
    else:
        robot_dict[domainUrl] = None
        return 
