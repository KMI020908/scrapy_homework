from typing import Iterable
import scrapy
import re
from scrapy.http import Request
from homework.items import MovieItem
import numpy as np

class WikiMovies2Spider(scrapy.Spider):
    name = "wiki_movies_2"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = ["https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B3%D0%BE%D0%B4%D0%B0%D0%BC"]

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
        table = response.css('div.mw-content-ltr.mw-parser-output > table > tbody > tr')
        for row_selector in table:
            words = [row.strip() for row in list(set(row_selector.css('::text').getall()))]

            genre_lack = [word.rfind('Жанр') == -1 for word in words]
            if not all(genre_lack):
                genre = [word.lower() for word in np.array(words)[genre_lack]]

            country_lack = [word.rfind('Стран') == -1 for word in words]
            if not all(country_lack):
                country = list(np.array(words)[country_lack])

            director_lack = [word.rfind('Режисс') == -1 for word in words]
            if not all(director_lack):
                director = list(np.array(words)[director_lack])

        movie_item = MovieItem()
        movie_item['title'] = response.meta['title']
        movie_item['genre'] = genre
        movie_item['director'] = director
        movie_item['country'] = country
        movie_item['year'] = response.meta['year']
        
        yield movie_item
                    