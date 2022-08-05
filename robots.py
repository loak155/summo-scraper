from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse


class Robots(object):
    def __init__(self, url, user_agent="*"):
        self.url = url
        self.user_agent = "*" if user_agent == "" else user_agent
        self.robots_url = self._get_robots_url()

        self.rp = RobotFileParser()
        self.rp.set_url(self.robots_url)
        self.rp.read()

    def _get_robots_url(self):
        parsed_url = urlparse(self.url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        return robots_url

    def can_fetch(self):
        return self.rp.can_fetch(self.user_agent, self.url)

    def crawl_delay(self):
        return self.rp.crawl_delay(self.user_agent)


def main():
    url = input("URL: ")
    user_agent = input("User-Agent: ")
    robots = Robots(url, user_agent)

    print(
        f"""
        URL: {robots.url}
        User-Agent: {robots.user_agent}
        Robots URL: {robots.robots_url}
        Can fetch: {robots.can_fetch()}
        Crawl delay: {robots.crawl_delay()}
    """
    )


if __name__ == "__main__":
    main()
