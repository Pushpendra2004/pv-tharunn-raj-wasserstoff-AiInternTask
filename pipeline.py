import os
import logging
import fitz  # PyMuPDF
import concurrent.futures
from pymongo import MongoClient
import spacy
from collections import Counter, defaultdict

logging.basicConfig(filename='pdf_pipeline.log', level=logging.DEBUG)

def parse_pdf(file_path):
    try:
        doc = fitz.open(file_path)
        text = ""
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            text += page.get_text()
        doc.close()
        return text
    except Exception as e:
        logging.error(f"Error processing {file_path}: {str(e)}")
        return None

def import_pdfs(folder_path):
    pdf_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".pdf")]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(parse_pdf, pdf_files)
    return list(results)

client = MongoClient("mongodb://localhost:27017/")
db = client['pdf_summarization']
collection = db['pdf_documents']

def store_metadata(file_path, text):
    try:
        metadata = {
            "file_name": os.path.basename(file_path),
            "file_path": file_path,
            "size": os.path.getsize(file_path),
            "summary": None,
            "keywords": None
        }
        collection.insert_one(metadata)
        logging.info(f"Metadata stored for {metadata['file_name']}")
    except Exception as e:
        logging.error(f"Error storing metadata for {file_path}: {str(e)}")

def update_metadata(file_name, summary, keywords):
    try:
        collection.update_one(
            {"file_name": file_name},
            {"$set": {"summary": summary, "keywords": keywords}}
        )
    except Exception as e:
        logging.error(f"Error updating metadata for {file_name}: {str(e)}")

def summarize_text(text, num_sentences=3):
    doc = nlp(text)
    sentence_scores = defaultdict(int)
    word_frequencies = Counter(token.text.lower() for token in doc if not token.is_stop and token.is_alpha)
    for sent in doc.sents:
        for token in sent:
            if token.text.lower() in word_frequencies:
                sentence_scores[sent.text] += word_frequencies[token.text.lower()]
    summarized_sentences = sorted(sentence_scores, key=sentence_scores.get, reverse=True)[:num_sentences]
    return ' '.join(summarized_sentences)

def extract_keywords(text):
    doc = nlp(text.lower())
    keywords = [token.text for token in doc if token.pos_ in ["NOUN", "PROPN", "ADJ"] and not token.is_stop]
    return [keyword for keyword, _ in Counter(keywords).most_common(10)]

def process_pdf(file_path):
    text = parse_pdf(file_path)
    if text:
        summary = summarize_text(text)
        keywords = extract_keywords(text)
        store_metadata(file_path, text)
        update_metadata(os.path.basename(file_path), summary, keywords)

def main(folder_path):
    pdf_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".pdf")]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_pdf, pdf_files)

nlp = spacy.load("en_core_web_sm")

if __name__ == "__main__":
    main("pdfs")
