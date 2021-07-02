# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from whoosh.fields import Schema, TEXT, KEYWORD, ID, STORED, DATETIME
from whoosh.index import create_in
from whoosh.analysis import StemmingAnalyzer
from whoosh.qparser import MultifieldParser
from whoosh import scoring
import whoosh
import csv

from gensim.models import KeyedVectors
import numpy as np


# %%
imdb_dataset_path = "./data/IMDB-Movie-Data.csv"
imdb_dataset_path2 = "./data/movies.csv"
index_path = "./whoosh_index"
word2vec_model_path = 'E:/Users/Lucas xD/Downloads/GoogleNews-vectors-negative300.bin'


# %%
try:  # nich mehrmals in Speicher laden... sind 3gb
    model
except NameError:
    model = KeyedVectors.load_word2vec_format(word2vec_model_path, binary=True)


# %%
def read_in_csv(csv_file):
    with open(csv_file, 'r', encoding='utf-8') as fp:
        reader = csv.reader(fp, delimiter=',', quotechar='"')
        data_read = [row for row in reader]
    return data_read


csv_data = read_in_csv(imdb_dataset_path2)


# %%
schema = Schema(movie_id=ID(stored=True),
                title=TEXT(analyzer=StemmingAnalyzer(), stored=True),
                description=TEXT(analyzer=StemmingAnalyzer()),
                genre=KEYWORD,
                director=TEXT,
                actors=TEXT,
                year=DATETIME)

schema2 = Schema(id=ID(stored=True),
                 title=TEXT(analyzer=StemmingAnalyzer(), stored=True),
                 poster=TEXT,
                 overview=TEXT(analyzer=StemmingAnalyzer()),
                 release_date=TEXT)


# %%
create_new_index = True
if(create_new_index):
    index = create_in(index_path, schema2)
else:
    index = whoosh.index.open_dir(index_path)

writer = index.writer()


# %%
for row in csv_data[1:]:
    id = row[0]
    title = row[1]
    poster = row[2]
    overview = row[3]
    release_date = row[4]
    writer.add_document(id=id,
                        title=title,
                        poster=poster,
                        overview=overview,
                        release_date=release_date)
writer.commit()


# %%

use_synonyms = False

search_term = "Lightning"

if(use_synonyms):
    similarity_list = model.most_similar(search_term, topn=3)
    similar_words = [sim_tuple[0] for sim_tuple in similarity_list]
    keywords = " OR ".join([search_term] + similar_words)
else:
    keywords = search_term

results = []
print("Results with Word2Vec:")
print(f"Similar words used: {similar_words}")
with index.searcher(weighting=scoring.TF_IDF()) as searcher:
    query = MultifieldParser(["title", "description"],
                             index.schema).parse(keywords)
    results = searcher.search(query)
    for docnum, score in results.items():
        print(docnum+1, score)
    print(results)


print("________________________\n")
print("Results without Word2Vec:")
with index.searcher(weighting=scoring.TF_IDF()) as searcher:
    query = MultifieldParser(["title", "description"],
                             index.schema).parse(search_term)
    results = searcher.search(query)
    for docnum, score in results.items():
        print(docnum, score)
    for doc in results:
        print(doc['id'])


# %%
