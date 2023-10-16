import os
from scrapy.cmdline import execute

os.chdir(os.path.dirname(os.path.realpath(__file__)))

try:
    execute(
        [
            'scrapy',
            'crawl',
            'mainspider',
            '-o',
            'out.json',
        ]
    )
except SystemExit:
    pass