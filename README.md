# 🔍 Search Engine - High-Performance Web Crawler & Indexer

![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![Jetty](https://img.shields.io/badge/Jetty-FF6600?style=for-the-badge&logo=eclipse&logoColor=white)
![Multi-threading](https://img.shields.io/badge/Concurrent-Processing-blue)

A **multi-threaded search engine** built from scratch with web crawling, indexing, and ranking capabilities. Handles 100K+ documents with sub-200ms response times.

## 🚀 Key Features
- **Lightning-fast searches**: Average response time <200ms for 10K+ documents
- **Personalized results**: MySQL-backed user history/favorites system
- **Concurrent processing**: Jetty-embedded server handles 100+ simultaneous queries
- **Relevance ranking**: TF-IDF + PageRank hybrid algorithm

## 🛠️ Tech Stack
- **Backend**: Java 11, Jetty Server
- **Database**: MySQL (Indexing + User Data)
- **Frontend**: HTML/CSS 
- **Algorithms**: TF-IDF, PageRank, Multi-threading

## 📊 Performance Metrics
| Metric | Result |
|--------|--------|
| Index Size | 5,000+ documents |
| Avg. Query Time | <200ms |
| Concurrent Users | 100+ |
| Storage Efficiency | 40% reduction vs. brute-force |
