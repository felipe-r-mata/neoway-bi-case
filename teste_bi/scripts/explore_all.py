import pandas as pd
import os
import csv

def quick_analysis(file_path):
    print(f"\n{'='*60}")
    print(f"📊 Arquivo: {os.path.basename(file_path)}")
    
    # Primeiro, detecta o separador
    with open(file_path, 'r', encoding='utf-8') as f:
        first_line = f.readline()
        if ';' in first_line and ',' not in first_line[:100]:
            sep = ';'
            print(f"🔧 Separador detectado: PONTO E VÍRGULA (;)")
        else:
            sep = ','
            print(f"🔧 Separador detectado: VÍRGULA (,)")
    
    try:
        # Tenta ler com encoding padrão
        df = pd.read_csv(file_path, sep=sep, nrows=1000, encoding='utf-8')
    except:
        # Se falhar, tenta outros encodings
        try:
            df = pd.read_csv(file_path, sep=sep, nrows=1000, encoding='latin-1')
        except:
            try:
                df = pd.read_csv(file_path, sep=sep, nrows=1000, encoding='cp1252')
            except Exception as e:
                print(f"❌ Erro ao ler arquivo: {e}")
                return []
    
    print(f"📏 Shape: {df.shape}")
    print(f"🏷️  Colunas ({len(df.columns)}):")
    for i, col in enumerate(df.columns[:10]):  # Mostra apenas 10 primeiras
        print(f"   {i+1:2d}. {col}")
    if len(df.columns) > 10:
        print(f"   ... e mais {len(df.columns) - 10} colunas")
    
    print(f"👀 Amostra (3 primeiras linhas):")
    print(df.head(3).to_string(max_cols=5))  # Limita colunas na exibição
    
    return df.columns.tolist()

# Analisar todos
csv_dir = '../files/spreadsheets/'  # Ajuste conforme sua estrutura

if not os.path.exists(csv_dir):
    print(f"❌ Pasta não encontrada: {csv_dir}")
    print("📁 Tentando encontrar CSVs...")
    # Procura recursivamente
    for root, dirs, files in os.walk('..'):
        for file in files:
            if file.endswith('.csv'):
                print(f"✅ Encontrado: {os.path.join(root, file)}")
else:
    files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
    print(f"✅ Encontrados {len(files)} arquivos CSV em {csv_dir}")
    
    for file in files:
        file_path = os.path.join(csv_dir, file)
        quick_analysis(file_path)