import urllib
import asyncio
import urllib.robotparser
import tldextract
from util import to_domain

class robotsTxt:
    def __init__(self, parser, crawl_delay, request_rate):
        self.parser = parser
        self.crawl_delay = crawl_delay
        self.request_rate = request_rate


async def check_robots(client, domain: str, robot_dict: dict):
    r_parser = urllib.robotparser.RobotFileParser()
    
    robots_text = None
    #check if this has already been grabbed
    if domain in robot_dict.keys():
        return robot_dict[domain]
        
    robots_url = domain + "/robots.txt"

    try:
        headers = {'User-Agent': 'iSearch'}
        async with client.get(robots_url, allow_redirects=True, headers=headers) as response:            
            if response.ok:
                robots_text = await response.text()
            else:
                robot_dict[domain] = None
                return

            if robots_text:
                try:
                    lines = robots_text.splitlines()
                    r_parser.parse(lines)
                except Exception as e:
                    with open("log.txt", "a") as f:
                        f.write("check_robots threw an error: ")
                        f.write(str(e) + '\n')
                        
                #get rate-limits to not get banned by websites
                crawl_delay = r_parser.crawl_delay("iSearch")
                request_rate = r_parser.request_rate("iSearch")

                if request_rate:
                    request_rate = request_rate.seconds / request_rate.requests

                robot_dict[domain] = robotsTxt(r_parser, crawl_delay, request_rate)
                return robot_dict[domainUrl]
            else:
                robot_dict[domain] = None
                return 
    except Exception as e:
        robot_dict[domain] = None