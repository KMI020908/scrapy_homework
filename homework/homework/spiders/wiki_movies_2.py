from typing import Iterable
import scrapy
import re
from scrapy.http import Request


class WikiMovies2Spider(scrapy.Spider):
    name = "wiki_movies_2"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = ["https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B3%D0%BE%D0%B4%D0%B0%D0%BC"]
    forbidden_words = ['\n', '\xa0', '(съёмки),', ' ', ', ', '[…]', '/', ' /', '/ ', '[d]', ' и '] + [f'[{n}]' for n in range(1, 10)]
    attr_matches = {
        'genre': ['Жанры', 'Жанр', ' Жанр\n', ' Жанры\n', ' Жанр \n', ' Жанры \n', 'Жанр / тематика'],
        'director': ['Режиссёр', 'Режиссёры'],
        'country': ['Страна', 'Страны'],
        'year': ['Год', 'Премьера']
    }

    def start_requests(self) -> Iterable[Request]:
        URL = self.start_urls[0]
        yield scrapy.Request(url=URL, callback=self.year_parse)  

    def year_parse(self, response):
        for selector in response.css("div.CategoryTreeItem"):
            title = selector.css("a::attr(title)").extract()[0] 
            if re.search(r'по', title) is None:
                next_page = "https://" + self.allowed_domains[0] + selector.css("a::attr(href)").extract_first()
                if next_page:
                    yield response.follow(url=next_page, callback=self.movie_parse)

    def movie_parse(self, response):
        for selector in response.css("div.mw-category-group"):
            movie_pages = selector.css("ul > li > a::attr(href)").extract()
            for page in movie_pages:
                if page:
                    page = "https://" + self.allowed_domains[0] + str(page)
                    yield response.follow(url=page, callback=self.movie_page_parse)
        next_page = response.css("#mw-pages > a::attr(href)").extract()
        if next_page:
            if response.css("#mw-pages > a::text").extract()[-1] != "Предыдущая страница":
                yield response.follow(url=response.urljoin(next_page[-1]), callback=self.movie_parse)

    def movie_page_parse(self, response):
        title = response.css("span.mw-page-title-main::text").extract()
        if not title:
            title = response.css("#firstHeading > i::text").extract()
        genre, director, country, year = None, None, None, None
        for selector in response.css("div.mw-content-ltr.mw-parser-output > table > tbody > tr"):
            if selector.css("th.plainlist > a::text").extract_first() in self.attr_matches['genre']:
                for genre_selector in selector.css("td.plainlist"):
                    genre = genre_selector.css("::text").extract()
                    genre = [el for el in genre if el not in self.forbidden_words]
            if selector.css("th.plainlist::text").extract_first() in self.attr_matches['director']:
                for director_selector in selector.css("td.plainlist"):
                    director = director_selector.css("::text").extract()
                    director = [el for el in director if el not in self.forbidden_words]
            if selector.css("th.plainlist > a::text").extract_first() in self.attr_matches['year'] or selector.css("th.plainlist::text").extract_first() in self.attr_matches['year']:
                for year_selector in selector.css("td.plainlist"):
                    year = year_selector.css("::text").extract()
                    if year[-1] in [f'[{n}]' for n in range(10)]:
                        year = year[-2][-4:]
                    else:
                        year = year[-1][-4:]
            if selector.css("th.plainlist::text").extract_first() in self.attr_matches['country']:
                for country_selector in selector.css("td.plainlist"):
                    country = country_selector.css("::text").extract()
                    country = [el for el in country if el not in self.forbidden_words]
        yield {
            'title': title, 
            'genre': genre,
            'director': director,
            'country': country,
            'year': year
        }
                    