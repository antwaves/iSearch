import re
import time
import urllib
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup as soup

from db import connect_to_db, create_page
from robots import check_robots, robotsTxt
from util import unique_queue, to_domain


def get_response(startLink: str, queue: unique_queue):
    #send request
    headers = {'User-Agent': 'iSearch'}

    try:
        response = requests.get(startLink, headers=headers)
    except Exception as e:
        queue.popleft()
        print("Exception raised in get content:", e)
        return None

    if response.ok:
        print(f"Grabbed {startLink} with status code {response.status_code}{30 * " "}")
    else:
        print(f"Failed to grab {startLink} with response code {response.status_code}{30 * " "}")
    
    return response


def handle_limits(response, current_link, robot_rules, domains):
    can_request_at = None

    if response.status_code == 429 or response.status_code == 503:
        retry_after = response.headers["Retry-After"]

        if retry_after:
            if retry_after.isdigit():
                time_delta = timedelta(seconds=int(retry_after))
                can_request_at = datetime.now(timezone.utc) + time_delta
            else:
                try:
                    can_request_at = parsedate_to_datetime(retry_after)
                except (TypeError, ValueError):
                    can_request_at = datetime.now(timezone.utc) + timedelta(seconds=5)
        else:
            can_request_at = datetime.now(timezone.utc) + timedelta(seconds=5)

    elif robot_rules.crawl_delay or robot_rules.request_rate:
        crawl_delay = 0 if robot_rules.crawl_delay == None else robot_rules.crawl_delay
        request_rate = 0 if robot_rules.request_rate == None else robot_rules.request_rate

        wait_time = 0
        if crawl_delay >= request_rate:
            wait_time = crawl_delay
        else:
            wait_time = request_rate
    
        if wait_time < 0.1:
            wait_time = 0.1

        wait_milliseconds = wait_time * 100
        can_request_at = datetime.now(timezone.utc) + timedelta(milliseconds=wait_milliseconds)
    
    else:
        can_request_at = datetime.now(timezone.utc) + timedelta(milliseconds=100)

    domain_url = to_domain(current_link)
    domains[domain_url] = can_request_at


def parse_content(start_link, response, queue, session) -> None:
    content = soup(response.content, 'html.parser')     

    if not content:
        return None

    outlinks = []
    for link in content.find_all('a', href=True):
        link = link["href"] 
        if not link:
            continue

        if link[-1] == "/":
            link = link[:-1]
        
        if "https://" in link:
            outlinks.append(link)
            queue.append(link)
        else:
            link = urllib.parse.urljoin(start_link, link)
            outlinks.append(link)
            queue.append(link)

    headers = []
    title = content.title.text
    if title:
        for word in title.split(" "):
            if len(word) > 1:
                headers.append(word)

    content_headers = content.find_all(re.compile('^h[1-6]'))
    for header in content_headers:
        for word in header.text.split(" "):
            if len(word) > 1:
                headers.append(word)
    
    text = content.get_text()
    page = create_page(session, start_link, text, headers, outlinks)

    return None
    

def main() -> None:
    start_url: str = 'https://nodejs.org/en'
    website_robot_rules = {} 
    domain_wait_times = {}
    link_queue = unique_queue()

    session = connect_to_db()
    print("Connected to DB!")

    robot_rules = check_robots(start_url, website_robot_rules)
    response = get_response(start_url, link_queue)
    handle_limits(response, start_url, robot_rules, domain_wait_times)
    parse_content(start_url, response, link_queue, session)

    i = 0
    while i < 100:
        current_link = link_queue[0]
        domain = to_domain(current_link)

        if domain in domain_wait_times.keys():
            now = datetime.now(timezone.utc)
            sleep_time = (domain_wait_times[domain] - now)
            sleep_seconds = sleep_time.total_seconds()

            if sleep_seconds > 0:
                print(sleep_seconds)
                time.sleep(sleep_seconds)

        robot_rules = check_robots(current_link, website_robot_rules)
        if robot_rules and (not robot_rules.parser.can_fetch("*", current_link)):
            link_queue.popleft()
            continue
    
        response = get_response(current_link, link_queue)
        handle_limits(response, current_link, robot_rules, domain_wait_times)
        parse_content(current_link, response, link_queue, session)

        link_queue.popleft()
        i += 1


if __name__ == "__main__":
    t = time.time()

    main()

    print(f"{time.time() - t} seconds executed")