import re
from sklearn.cluster import DBSCAN
import gensim

w2v_fpath = "./all.norm-sz100-w10-cb0-it1-min100.w2v"
w2v = gensim.models.KeyedVectors.load_word2vec_format(w2v_fpath, binary=True, unicode_errors='ignore')
w2v.init_sims(replace=True)

def cluster(fname_in, fname_out):
    count = 0
    f = open(fname_in)
    item_vectors = []
    texts = []
    from sklearn.feature_extraction.text import TfidfVectorizer

    original = []

    for line in f.readlines():
        count += 1
        print(count)
        result = re.search(r'\b[АБВГДЕЖЗИКЛМНОПРСТУФХЦЧШЩЭЮЯ][^КМОПС ]\w+', line)
        #print (result.group(0))
        line = line.rstrip()
        try:
            for word in w2v.most_similar(result.group(0).lower()):
                line = line+' '+word[0]
                #print(word[0])
        except KeyError:
            print('такого слова нет в словаре')
        # print(line)
        original.append(line)
        x = line.split(";")[0].lower()
        text = []
        for _word in x.split(" "):
            word = re.sub("\W+", "", _word)
            text.append(word)
        texts.append(' '.join(text))

    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(texts)
    #print(item_vectors)
    clustering = DBSCAN(eps=0.1, metric='cosine',  min_samples=2).fit(X)
    fout = open(fname_out, "w")

    for k in range(len(original)):
        fout.write(original[k])
        fout.write(";")
        fout.write(str(clustering.labels_[k]))
        fout.write("\n")

if __name__ == "__main__":
    cluster("test1.csv", "out_test1.csv")
