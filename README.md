# PDF Summarization and Keyword Extraction Pipeline

A Python-based pipeline for processing PDF documents. It extracts text, generates summaries, and extracts keywords, storing results in a MongoDB database.

## Features
- Extracts text from PDF documents.
- Summarizes text based on word frequency.
- Extracts keywords (nouns, adjectives, proper nouns).
- Stores metadata, summary, and keywords in MongoDB.
- Concurrently processes multiple PDFs using ThreadPoolExecutor.
