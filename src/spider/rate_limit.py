import asyncio
import urllib
import urllib.robotparser
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

from util import to_top_domain, lock, silent_log


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
            crawl_delay = r_parser.crawl_delay(headers["User-Agent"])
            request_rate = r_parser.request_rate(headers["User-Agent"])

            if request_rate:
                request_rate = request_rate.seconds / request_rate.requests

            robot_dict[domain] = robotsTxt(r_parser, crawl_delay, request_rate)

            return robot_dict[domain]

    except Exception as e:
        silent_log(e, "check_robots")
        robot_dict[domain] = None


def cannot_fetch(url, robot_rules):
    return robot_rules and (not robot_rules.parser.can_fetch("*", url))


def get_rate_limits(response, url, robot_rules, domain_wait_times):
    domain = to_top_domain(url)

    response_limit = get_rate_limit_from_response(response)
    if response_limit:
        domain_wait_times[domain] = response_limit
        return

    robots_limit = get_rate_limit_from_robots(robot_rules)
    if robots_limit:
        domain_wait_times[domain] = robots_limit
        return

    fallback_wait = 200
    domain_wait_times[domain] = datetime.now(timezone.utc) + timedelta(milliseconds=fallback_wait)


def get_rate_limit_from_response(response):
    fallback_time = datetime.now(timezone.utc) + timedelta(milliseconds=15000)

    if response.status != 429 and response.status != 503:
        return None

    if not response.headers:
        return fallback_time

    retry_after = response.headers.get("Retry-After")
    if not retry_after:
        return fallback_time

    if retry_after.isdigit():
        return datetime.now(timezone.utc) + timedelta(milliseconds=int(retry_after) * 1000)

    try:
        return parsedate_to_datetime(retry_after)
    except (TypeError, ValueError):
        return fallback_time


def get_rate_limit_from_robots(robot_rules):
    if not(robot_rules and (robot_rules.crawl_delay or robot_rules.request_rate)):
        return None
        
    crawl_delay = robot_rules.crawl_delay if robot_rules.crawl_delay else 0
    request_rate = robot_rules.request_rate if robot_rules.request_rate else 0

    wait_time = 0
    if crawl_delay >= request_rate:
        wait_time = crawl_delay
    else:
        wait_time = request_rate

    if wait_time < 0.2:
        wait_time = 0.2

    wait_time = datetime.now(timezone.utc) + timedelta(milliseconds=int(wait_time * 1000))
    return wait_time


def get_domain_lock(url, domain_locks):
    domain = to_top_domain(url)

    if domain not in domain_locks.keys():
        domain_lock = lock()
        domain_locks[domain] = domain_lock

    return domain_locks[domain]


def get_sleep_time(domain, domain_wait_times):
    if domain not in domain_wait_times.keys():
        return 0

    now = datetime.now(timezone.utc)
    sleep_time = (domain_wait_times[domain] - now)
    sleep_seconds = sleep_time.total_seconds()

    if sleep_seconds > 0.05:
        return sleep_seconds
    else:
        return 0
        