import pandas as pd
import numpy as np
import re
from sklearn.cluster import AgglomerativeClustering
from sentence_transformers import SentenceTransformer

# Assuming 'df' is your DataFrame

# Step 1: Data Preprocessing
def preprocess_text(text):
    # Convert to lowercase
    text = str(text).lower()
    # Remove special characters and numbers
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def ProcessarDescricoesProdutos(df):
    df['DESCRICAO_PROD_PROCESSADO'] = df['DESCRICAO_PROD'].apply(preprocess_text)

    # Step 2: Generate Text Embeddings
    model = SentenceTransformer('all-MiniLM-L6-v2')
    embeddings = model.encode(df['DESCRICAO_PROD_PROCESSADO'].tolist())

    # Step 3: Clustering Similar Product Descriptions
    # Adjust the distance_threshold based on your data
    clustering_model = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=0.5,
        affinity='cosine',
        linkage='average'
    )
    clustering_model.fit(embeddings)
    df['Description_Cluster'] = clustering_model.labels_


    df['ID_PRODUTO_POR_DESCRICAO'] = ['Cluster' + line['Description_Cluster'] for _, line in df.iterrows()]
           

    return df
