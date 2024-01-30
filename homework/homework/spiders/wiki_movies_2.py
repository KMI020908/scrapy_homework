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
        yield scrapy.Request(url=URL, callback=self.years_parse)  

    def years_parse(self, response):
        years_groups = response.css('div.CategoryTreeItem > a')
        for group in years_groups:
            if 'по' not in group.css('::text'):
                next_page = group.attrib['href']
                if next_page:
                    yield response.follow(url=response.urljoin(next_page), callback=self.movies_parse)
    
    def movies_parse(self, response):
        year = response.css('#mw-content-text > div.mw-content-ltr.mw-parser-output > table > tbody > tr > th::text').get()
        if year:
            year = year.replace('\n', '')
        movie_groups = response.css('#mw-pages > div > div > div')
        for group in movie_groups:
            if group.css('h3::text').get() != '*':
                for movie in group.css('ul > li'):
                    movie_page, title = [*movie.css('a').attrib.values()]
                    if movie_page:
                        yield response.follow(url=response.urljoin(movie_page), callback=self.movie_page_parse, meta={'year': year, 'title': title})

        next_page_buttons = response.css('#mw-pages > a')
        visited_next_page = False
        for button in next_page_buttons:
            if (not visited_next_page) and button.css('::text').get() == "Следующая страница":
                next_page = button.attrib['href']
                if next_page:
                    visited_next_page = True
                    yield response.follow(url=response.urljoin(next_page), callback=self.movies_parse)

    def movie_page_parse(self, response):
        
        genre, director, country = None, None, None
        for selector in response.css("div.mw-content-ltr.mw-parser-output > table > tbody > tr"):
            if selector.css("th.plainlist > a::text").extract_first() in self.attr_matches['genre']:
                for genre_selector in selector.css("td.plainlist"):
                    genre = genre_selector.css("::text").extract()
                    genre = [el for el in genre if el not in self.forbidden_words]
            if selector.css("th.plainlist::text").extract_first() in self.attr_matches['director']:
                for director_selector in selector.css("td.plainlist"):
                    director = director_selector.css("::text").extract()
                    director = [el for el in director if el not in self.forbidden_words]
            if selector.css("th.plainlist::text").extract_first() in self.attr_matches['country']:
                for country_selector in selector.css("td.plainlist"):
                    country = country_selector.css("::text").extract()
                    country = [el for el in country if el not in self.forbidden_words]
      
        yield {
            'title': response.meta['title'],
            'genre': genre,
            'director': director,
            'country': country,
            'year': response.meta['year']
        }
                    