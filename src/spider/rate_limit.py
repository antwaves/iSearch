import asyncio
import urllib
import urllib.robotparser
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

from util import to_top_domain, lock


class robotsTxt:
    def __init__(self, parser, crawl_delay, request_rate):
        self.parser = parser
        self.crawl_delay = crawl_delay
        self.request_rate = request_rate


async def check_robots(client, domain: str, robot_dict: dict, headers: dict):
    r_parser = urllib.robotparser.RobotFileParser()
    robots_text = None

    #check if this has already been grabbed
    if domain in robot_dict.keys():
        return robot_dict[domain]
    
    robots_url = domain + "/robots.txt"
    try:
        async with client.get(robots_url, allow_redirects=True, headers=headers) as response:   
            if not response.ok:
                robot_dict[domain] = None
                return

            robots_text = await response.text()

            if not robots_text:
                robot_dict[domain] = None
                return 

            lines = robots_text.splitlines()
            r_parser.parse(lines)
                    
            #get rate-limits to not get banned by websites
            crawl_delay = r_parser.crawl_delay("iSearch")
            request_rate = r_parser.request_rate("iSearch")

            if request_rate:
                request_rate = request_rate.seconds / request_rate.requests

            robot_dict[domain] = robotsTxt(r_parser, crawl_delay, request_rate)

            return robot_dict[domain]

    except Exception as e:
        robot_dict[domain] = None


def set_wait_time(url :str, time: int, domain_wait_times: dict): #in milliseconds
    can_request_at = datetime.now(timezone.utc) + timedelta(milliseconds=time)
    domain_wait_times[to_top_domain(url)] = can_request_at


def handle_limits(response, url, robot_rules, domain_wait_times):
    can_request_at = None

    if response.status == 429 or response.status == 503:
        if not response.headers:
            return set_wait_time(url, 15000, domain_wait_times)

        retry_after = response.headers.get("Retry-After")
        if not retry_after:
            return set_wait_time(url, 15000, domain_wait_times)

        print("Retry After:", retry_after)
        if retry_after.isdigit():
            return set_wait_time(url, int(retry_after) * 1000, domain_wait_times)

        try:
            can_request_at = parsedate_to_datetime(retry_after)
            domain_wait_times[to_top_domain(url)] = can_request_at
            return 

        except (TypeError, ValueError):
            return set_wait_time(url, 15000, domain_wait_times)
    
    if not (robot_rules and (robot_rules.crawl_delay or robot_rules.request_rate)):
        return set_wait_time(url, 200, domain_wait_times)
        
    crawl_delay = 0 if robot_rules.crawl_delay == None else robot_rules.crawl_delay
    request_rate = 0 if robot_rules.request_rate == None else robot_rules.request_rate

    wait_time = 0
    if crawl_delay >= request_rate:
        wait_time = crawl_delay
    else:
        wait_time = request_rate

    if wait_time < 0.2:
        wait_time = 0.2

    wait_milliseconds = int(wait_time * 1000)
    return set_wait_time(url, wait_milliseconds, domain_wait_times)


def get_domain_lock(url, domain_locks):
    domain = to_top_domain(url)

    if domain in domain_locks.keys():
        return domain_locks[domain]

    domain_lock = lock()
    domain_locks[domain] = domain_lock
    return domain_locks[domain]


def get_sleep_time(domain, domain_wait_times):
    if domain not in domain_wait_times.keys():
        return 0

    now = datetime.now(timezone.utc)
    sleep_time = (domain_wait_times[domain] - now)
    sleep_seconds = sleep_time.total_seconds()

    if sleep_seconds > 0:
        return sleep_seconds
    else:
        return 0
        