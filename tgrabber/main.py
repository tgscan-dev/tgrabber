import asyncio

from tgrabber.crawler.link_crawler import LinkCrawler

if __name__ == "__main__":
    crawler = LinkCrawler(2)
    asyncio.run(crawler.run())
