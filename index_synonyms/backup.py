# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from whoosh.fields import Schema, TEXT, KEYWORD, ID, STORED, DATETIME
from whoosh.index import create_in
from whoosh.analysis import StemmingAnalyzer
from whoosh.qparser import MultifieldParser
from whoosh import scoring
import whoosh.index
import csv


# %%
imdb_dataset_path = "./data/IMDB-Movie-Data.csv"
index_path = "./whoosh_index"


# %%
def read_in_csv(csv_file):
    with open(csv_file, 'r', encoding='utf-8') as fp:
        reader = csv.reader(fp, delimiter=',', quotechar='"')
        data_read = [row for row in reader]
    return data_read

csv_data = read_in_csv(imdb_dataset_path)


# %%
schema = Schema(movie_id=ID(stored=True),
                title=TEXT(analyzer=StemmingAnalyzer()),
                description=TEXT(analyzer=StemmingAnalyzer()),
                genre=KEYWORD,
                director=TEXT,
                actors=TEXT,
                year=DATETIME)


# %%
create_new_index = True
if(create_new_index):
    index = create_in(index_path, schema)
else:    
    index = whoosh.index.open_dir(index_path)

writer = index.writer()


# %%
for row in data[1:]:
    movie_id = row[0]
    title = row[1]
    genre = row[2]
    description = row[3]
    director = row[4]
    actors = row[5]
    year = row[6]
    writer.add_document(movie_id=movie_id, title=title, description=description, genre=genre, director=director, actors=actors, year=year)
writer.commit()


# %%

search_term = "Blue Valentine"



# model = load_model(w2vec_model_path)
# similarity_list = model.most_similar(search_term, topn=3)
# similar_words = [sim_tuple[0] for sim_tuple in similarity_list]
# other_words = get_similar_words(model, search_term)

# keywords = " OR ".join([search_term] + other_words)
keywords  = search_term
results = []
with index.searcher(weighting=scoring.TF_IDF()) as searcher:
    query = MultifieldParser(["title", "description"], index.schema).parse(keywords)
    results = searcher.search(query)
    for docnum, score in results.items():
        print(docnum+1, score)
    print(results[0])


# %%
data[444]


# %%
import gensim


# %%



# %%



