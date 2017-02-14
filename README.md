# acrawler

基于asyncio的仿scrapy爬虫框架

## Installation
    git clone https://github.com/s723138643/acrawler
    pip install -i requirements.txt
    cd acrawler && python setup.py install

## Usage
### base usage
编辑文件bspider.py
```
from acrawler.spider import Spider
from acrawler.cmdline import execute
from acrawler.model import Request


class BSpider(Spider):
    start_urls = ['https://www.baidu.com']
    hosts = {'www.baidu.com'}

    @classmethod
    def start_request(cls):
        requests = []
        for url in cls.start_urls:
            requests.append(Request(url))
        return requests

    def parse(self, response):
        pages = response.xpath('//a/@href')
        for i in pages:
            yield Request(i)


if __name__ == '__main__':
    execute(BSpider)
```
输入命令以下命令启动爬虫：
    python bspider.py

## License
acrawler使用[MIT license](LICENSE.txt)

