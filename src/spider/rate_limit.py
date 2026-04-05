import asyncio
import urllib
import urllib.robotparser
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

from spider.util import to_top_domain, lock, silent_log
from spider.async_request import fetch

import time


class robotsTxt:
    def __init__(self, parser, rate_limit):
        self.parser = parser
        self.rate_limit = rate_limit


class rate_limiter:
    def __init__(self, session, headers):
        self.session = session
        self.headers = headers

        self.domain_wait_times = {}
        self.domain_robot_rules = {}

        self.cache_hits = 0
        self.cache_misses = 0

        self.block_rate = 0
        self.not_blocked_rate = 0

    # use reppy?

    async def check_robots_for_batch(self, domains: list):
        to_grab = []
        robot_rules = {} # domain : robot_rules

        for domain in domains:
            if domain in self.domain_robot_rules.keys():
                self.cache_hits += 1
                robot_rules[domain] = self.domain_robot_rules[domain]
                continue
            to_grab.append(domain)
            self.cache_misses += 1
        
        try:
            robot_urls = [domain + "/robots.txt" for domain in to_grab]
            response_grabs = [fetch(self.session, url, self.headers) for url in robot_urls]
            responses = await asyncio.gather(*response_grabs)

            for response in responses:
                domain = response['url'][:-len("/robots.txt")] # not hardcoding to make this easier to read
                
                if not response:
                    continue

                if 'ok' not in response.keys() or not response['ok']:
                    self.block_rate += 1
                    self.domain_robot_rules[domain] = None
                    robot_rules[domain] = None
                    continue
                self.not_blocked_rate += 1

                if not 'text' in response.keys():
                    self.domain_robot_rules[domain] = None
                    robot_rules[domain] = None
                    continue

                robots_text = response['text']
                lines = robots_text.splitlines()
                r_parser = urllib.robotparser.RobotFileParser()
                r_parser.parse(lines)

                crawl_delay = r_parser.crawl_delay(self.headers["User-Agent"])
                request_rate = r_parser.request_rate(self.headers["User-Agent"])
                if request_rate:
                    request_rate = request_rate.seconds / request_rate.requests

                if crawl_delay and request_rate:
                    rate_limit = max(crawl_delay, request_rate)
                elif crawl_delay or request_rate:
                    rate_limit = crawl_delay if crawl_delay else request_rate
                else:
                    rate_limit = 0 #changed later

                robot_rule = robotsTxt(r_parser, rate_limit)
                self.domain_robot_rules[domain] = robot_rule
                robot_rules[domain] = robot_rule
            
            return robot_rules
                        
        except Exception as e:
            print(f"Robot parsing failed with exeception {e}")
            silent_log(e, f"check_robots: {robot_urls}")


    def set_rate_limits(self, response, url, robot_rules, domain = None):
        if not domain:
            domain = to_top_domain(url)

        response_limit = get_rate_limit_from_response(response)
        if response_limit:
            self.domain_wait_times[domain] = response_limit
            return

        robots_limit = get_rate_limit_from_robots(robot_rules)
        if robots_limit:
            self.domain_wait_times[domain] = robots_limit
            return

        fallback_wait = 500
        self.domain_wait_times[domain] = datetime.now(timezone.utc) + timedelta(milliseconds=fallback_wait)


    def get_sleep_time(self, domain):
        if domain not in self.domain_wait_times.keys():
            return 0

        now = datetime.now(timezone.utc)
        sleep_time = (self.domain_wait_times[domain] - now)
        sleep_seconds = sleep_time.total_seconds()

        if sleep_seconds > 0.05:
            return sleep_seconds
        else:
            return 0


def get_rate_limit_from_response(response):
    fallback_time = datetime.now(timezone.utc) + timedelta(milliseconds=15000)

    if response['status'] not in (403, 429, 503):
        return None

    headers = response['headers']
    if not headers:
        return fallback_time

    retry_after = headers.get("Retry-After")
    if not retry_after:
        return fallback_time

    if retry_after.isdigit():
        return datetime.now(timezone.utc) + timedelta(milliseconds=int(retry_after) * 1000)

    try:
        return parsedate_to_datetime(retry_after)
    except (TypeError, ValueError):
        return fallback_time


def get_rate_limit_from_robots(robot_rules):
    if not(robot_rules):
        return None

    wait_time = robot_rules.rate_limit
    if not wait_time:
        wait_time = 0
    if wait_time < 0.2:
        wait_time = 0.2

    wait_time = datetime.now(timezone.utc) + timedelta(milliseconds=int(wait_time * 1000))
    return wait_time

    
def can_fetch(url, robot_rules):
    if robot_rules:
        return robot_rules.parser.can_fetch("*", url)
    return True
