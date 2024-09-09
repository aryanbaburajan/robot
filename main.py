import os
import argparse
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from urllib.request import urlopen, Request
from collections import deque
from wakepy import keep

path = './data/'


def main():
    parser = argparse.ArgumentParser(
        description='robot (web crawler) by @aryanbaburajan')
    subparsers = parser.add_subparsers(
        dest='command', required=True, help='')

    init_parser = subparsers.add_parser(
        'init', help='create a crawl database')
    init_parser.add_argument(
        '-o', '--overwrite', help='overwrite existing database', action='store_true', required=False)

    crawl_parser = subparsers.add_parser(
        'crawl', help='start crawling')
    crawl_parser.add_argument(
        '-v', '--verbose', help='log crawled urls', action='store_true', required=False)
    crawl_parser.add_argument(
        '-s', '--seed', type=str, help='seed url', required=False)
    crawl_parser.add_argument(
        '-c', '--cap', type=int, help='maximum number of urls to crawl', required=False)

    args = parser.parse_args()

    match args.command:
        case 'init':
            init(args.overwrite)
        case 'crawl':
            crawl(args.seed, args.verbose, args.cap)


def init(overwrite=False):
    if os.path.isdir(path) and overwrite == False:
        answer = input(
            'crawl database already exists. would you like to overwrite? (y/n): ').strip().lower()
        if answer == 'y':
            pass
        elif answer == 'n':
            print('cancelled.')
            return
        else:
            print('invalid input.')
            return

    print('creating a crawl database ...', end=' ')

    os.makedirs(path, exist_ok=True)
    open(path + '/crawled_http.txt', 'w').close()
    open(path + '/crawled_https.txt', 'w').close()
    open(path + '/uncrawled_http.txt', 'w').close()
    open(path + '/uncrawled_https.txt', 'w').close()

    print('done')


def save(crawled_http, crawled_https, uncrawled_http, uncrawled_https):
    global logs
    with open(path + '/logs.txt', 'a') as file:
        file.write('\n')
        file.write('\n'.join(logs))
    with open(path + '/crawled_http.txt', 'w') as file:
        file.write('\n'.join(crawled_http))
    with open(path + '/crawled_https.txt', 'w') as file:
        file.write('\n'.join(crawled_https))
    with open(path + '/uncrawled_http.txt', 'w') as file:
        file.write('\n'.join(uncrawled_http))
    with open(path + '/uncrawled_https.txt', 'w') as file:
        file.write('\n'.join(uncrawled_https))


def load():
    with open(path + '/crawled_http.txt', 'r') as file:
        crawled_http = deque([line.strip() for line in file])
    with open(path + '/crawled_https.txt', 'r') as file:
        crawled_https = deque([line.strip() for line in file])
    with open(path + '/uncrawled_http.txt', 'r') as file:
        uncrawled_http = deque([line.strip() for line in file])
    with open(path + '/uncrawled_https.txt', 'r') as file:
        uncrawled_https = deque([line.strip() for line in file])
    return crawled_http, crawled_https, uncrawled_http, uncrawled_https


def open_url(url):
    return urlopen(Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
    }))


logs = []


def extract_links(url):
    global logs

    try:
        html = open_url(url).read()
    except Exception as e:
        print(e)
        logs.append(url + " " + xstr(e))
        return []

    soup = BeautifulSoup(html, "html.parser")
    a = soup.find_all('a')
    links = set()

    parse = urlparse(url)

    for tag in a:
        link = tag.get('href', None)
        if link is not None:
            p = urlparse(link)
            path = p.path
            while path.startswith(".") or path.startswith("/") or path.startswith(" "):
                path = path[1:]
            if p.scheme == "":
                link = f"{parse.scheme}://{parse.netloc}/{path}"
            else:
                link = f"{p.scheme}://{p.netloc}/{path}"
            links.add(link)
    return links


def crawl(seed, verbose, cap):
    crawled_http, crawled_https, uncrawled_http, uncrawled_https = load()

    def get_set(url, crawled=True):
        if crawled == True:
            if url.startswith("http://"):
                return crawled_http
            if url.startswith("https://"):
                return crawled_https
        else:
            if url.startswith("http://"):
                return uncrawled_http
            if url.startswith("https://"):
                return uncrawled_https
        return None

    if seed != None:
        get_set(seed, False).append(seed)

    crawls = 0

    def crawl_level():
        nonlocal crawls, crawled_http, crawled_https, uncrawled_http, uncrawled_https

        extracted_links = []

        urls = uncrawled_https.copy()
        for url in urls:
            if crawls >= cap:
                break
            if crawls % 100 == 0:
                save(crawled_http, crawled_https,
                     uncrawled_http, uncrawled_https)
            crawled_https.append(url)
            uncrawled_https.popleft()
            extracted_links += extract_links(url)
            if verbose:
                print(crawls, url)
            crawls += 1
        urls = uncrawled_http.copy()
        for url in urls:
            if crawls >= cap:
                break
            if crawls % 100 == 0:
                save(crawled_http, crawled_https,
                     uncrawled_http, uncrawled_https)
            crawled_http.append(url)
            uncrawled_http.popleft()
            extracted_links += extract_links(url)
            if verbose:
                print(crawls, url)
            crawls += 1

        for link in extracted_links:
            if link in crawled_http or link in crawled_https or link in uncrawled_http or link in uncrawled_https:
                continue
            if link.startswith("https://"):
                uncrawled_https.append(link)
            elif link.startswith("http://"):
                uncrawled_http.append(link)

    with keep.running():
        while crawls < cap:
            crawl_level()

    save(crawled_http, crawled_https, uncrawled_http, uncrawled_https)


if __name__ == '__main__':
    main()
