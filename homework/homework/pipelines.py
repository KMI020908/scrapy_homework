# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re

class MoviePipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        forbidden_words = ['', ',', 'и', '/', '-', '[', ']', '.рус', '(англ.)', '(', ')', '|', 'править', 'ru', 'en', '(нем.)', 'рус.']
        forbidden_patterns = [r'\[.*\]', r'{']
        proc_items_lst = ['genre', 'country', 'director']
        for proc_item in proc_items_lst:
            item_words = adapter.get(proc_item)
            words_to_remove = []
            if item_words:
                item_words = [word for word in item_words if word not in forbidden_words]
                for pattern in forbidden_patterns:
                    for word in item_words:
                        if re.search(pattern, word):
                            words_to_remove.append(word)
                item_words = [word for word in item_words if word not in words_to_remove]
                adapter[proc_item] = list(set(item_words))

        return item
