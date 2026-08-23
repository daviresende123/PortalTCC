"""Script auxiliar para contar embeddings no ChromaDB."""
from services.embedding_service import get_vector_store

store = get_vector_store()
count = store._collection.count()
print(f"Embeddings no ChromaDB: {count}")