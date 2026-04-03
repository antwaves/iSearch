import re
import time
from collections import Counter
from math import log10

import numpy as np
import asyncio

from db import connect_to_db, count_pages, get_total_pages_for_terms, retrieve_term_pages


def to_terms(doc, translator, term_finder):
    doc = doc.translate(translator)
    doc = doc.lower()
    return term_finder.findall(doc)


def get_vector_tf(query_terms, doc_terms):
    length = len(doc_terms)
    count = Counter(doc_terms)
    return {term : count[term] / length for term in query_terms}


async def get_vector_idf(terms, term_total_pages, total_pages):
    idf_vector = {}
    term_pages = dict(term_total_pages)
    for term in terms:
        pages = term_pages.get(term, 0)
        if pages:
            idf_vector[term] = 1 + log10(total_pages / term_pages[term])
        else:
            idf_vector[term] = 1
    
    return idf_vector
    

def get_tf_idf(query_terms, doc_terms, vectorized_idf):
    tf_idf = {}

    vectorized_tf = get_vector_tf(query_terms, doc_terms)
    for term in vectorized_tf.keys():
        tf_idf[term] = vectorized_tf[term] * vectorized_idf[term]
    return tf_idf


def dict_dot_prod(dict_one, dict_two):
    return sum(dict_one[k] * dict_two[k] for k in dict_one)


def get_cosine_simmilarity(vec_one, vec_two):
    dot_prod = dict_dot_prod(vec_one, vec_two)
    vec_one_len = np.sqrt(sum(item * item for item in vec_one.values()))
    vec_two_len =  np.sqrt(sum(item * item for item in vec_two.values()))

    if vec_one_len == 0 or vec_two_len == 0:
        return 0
    return dot_prod / (vec_one_len * vec_two_len)


def link_rank(url, query_terms):
    score = 0
    for term in query_terms:
        if term in url.lower():
            score += 0.15
    return score


async def main():
    punctuation = {'.': ' ', '?': ' ', '!': ' ', ',': ' ', 
                    ':': ' ', ';': ' ', '—': ' ', '(': ' ', ')': ' ', 
                    '[': ' ', ']': ' ', '{': ' ', '}': ' ', '\\': ' ', "'": ' ',
                    '"': ' ', '/': ' ', '*': ' ', '&': ' ', '~': ' ', '+': ' '}
    translator = str.maketrans(punctuation)
    term_finder = re.compile(r"[A-Za-z0-9_\-#@]+")
    
    session_maker = await connect_to_db(15)

    async with session_maker() as session:
        total_pages = float(await count_pages(session))
    
    query = None
    while query != "(quit)":
        query = input("New input!: ")

        q_terms = to_terms(query, translator, term_finder)

        async with session_maker() as session:
            term_total_pages = await get_total_pages_for_terms(session, q_terms)

        q_idf = await get_vector_idf(q_terms, term_total_pages, total_pages)
        query_tf_idf = get_tf_idf(q_terms, q_terms, q_idf)

        all_pages = []
        async with session_maker() as session:
            for term in q_terms:
                all_pages.extend(await retrieve_term_pages(session, term))
       
        all_pages = set(all_pages)
        scores = []
        for item in all_pages:
            page_url, page_content = item
            terms = to_terms(page_content, translator, term_finder)

            tf_idf = get_tf_idf(q_terms, terms, q_idf)
            simmilarity = get_cosine_simmilarity(query_tf_idf, tf_idf)
            l_rank = 1 + link_rank(page_url, q_terms)

            score = (simmilarity ** 1.2) * (l_rank)

            scores.append([page_url, score, [simmilarity, l_rank]])

        scores = sorted(scores, key=lambda x: x[1], reverse=True)
        for score in scores[:20]:
            print(score[0], score[1], score[2][1])


if __name__ == "__main__":
    asyncio.run(main())

