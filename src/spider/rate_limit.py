import urllib
import asyncio
import urllib.robotparser
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from util import to_top_domain

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
                lines = robots_text.splitlines()
                r_parser.parse(lines)
                        
                #get rate-limits to not get banned by websites
                crawl_delay = r_parser.crawl_delay("iSearch")
                request_rate = r_parser.request_rate("iSearch")

                if request_rate:
                    request_rate = request_rate.seconds / request_rate.requests

                robot_dict[domain] = robotsTxt(r_parser, crawl_delay, request_rate)
                return robot_dict[domain]
            else:
                robot_dict[domain] = None
                return 
    except Exception as e:
        robot_dict[domain] = None


def handle_limits(response, url, robot_rules, domain_wait_times):
    can_request_at = None

    if response.status == 429 or response.status== 503:
        if response.headers:
            retry_after = response.headers.get("Retry-After")

            if retry_after:
                print("Retry After:", retry_after)
                if retry_after.isdigit():
                    time_delta = timedelta(seconds=int(retry_after))
                    can_request_at = datetime.now(timezone.utc) + time_delta
                else:
                    try:
                        can_request_at = parsedate_to_datetime(retry_after)
                    except (TypeError, ValueError):
                        can_request_at = datetime.now(timezone.utc) + timedelta(seconds=15)
            else:
                can_request_at = datetime.now(timezone.utc) + timedelta(seconds=15)
        else:
            can_request_at = datetime.now(timezone.utc) + timedelta(seconds=15)
        
    elif robot_rules and (robot_rules.crawl_delay or robot_rules.request_rate):
        crawl_delay = 0 if robot_rules.crawl_delay == None else robot_rules.crawl_delay
        request_rate = 0 if robot_rules.request_rate == None else robot_rules.request_rate

        wait_time = 0
        if crawl_delay >= request_rate:
            wait_time = crawl_delay
        else:
            wait_time = request_rate

        if wait_time < 0.2:
            wait_time = 0.2

        wait_milliseconds = wait_time * 1000
        can_request_at = datetime.now(timezone.utc) + timedelta(milliseconds=wait_milliseconds)

    else:
        can_request_at = datetime.now(timezone.utc) + timedelta(milliseconds=200)

    domain_url = to_top_domain(url)
    domain_wait_times[domain_url] = can_request_at


def get_domain_lock(url, domain_locks):
    domain = to_top_domain(url)

    if domain not in domain_locks.keys():
        domain_locks[domain] = asyncio.Lock()
        return domain_locks[domain]
    else:
        return domain_locks[domain]
