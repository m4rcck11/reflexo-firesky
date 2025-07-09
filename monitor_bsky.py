import sys
import json
from datetime import datetime
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, models
from atproto import CAR

#Palavras-chave para filtrar os posts salvos. Defina-as abaixo.
KEYWORDS = ["Brasil", "Pesquisa", "Hello World"    

]

#Estatísticas recomendadas. Você pode desativar se quiser, mas precisa mudar em todos os blocos (matching posts, na função on_message etc.)
stats = {
    'total_messages': 0,
    'total_posts': 0,
    'matching_posts': 0
}

# Lista para armazenar posts encontrados
found_posts = []

def save_posts_to_file():
    """Salva os posts encontrados em um arquivo JSON"""
    if found_posts:
        filename = f"posts_bluesky_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(found_posts, f, ensure_ascii=False, indent=2)
        print(f"\nPosts salvos em: {filename}")

def extract_embed_data(record):
    """Extrai dados de mídia incorporada no post"""
    embed_data = {}
    
    if hasattr(record, 'embed'):
        embed = record.embed
        
        # Imagens
        if hasattr(embed, 'images'):
            embed_data['images'] = []
            for img in embed.images:
                image_data = {
                    'alt': getattr(img, 'alt', ''),
                    'image_cid': getattr(img, 'image', {}).get('$link', '') if hasattr(img, 'image') else ''
                }
                embed_data['images'].append(image_data)
        
        # Links externos
        if hasattr(embed, 'external'):
            ext = embed.external
            embed_data['external'] = {
                'uri': getattr(ext, 'uri', ''),
                'title': getattr(ext, 'title', ''),
                'description': getattr(ext, 'description', '')
            }
            
        # Record (repost/quote)
        if hasattr(embed, 'record'):
            rec = embed.record
            embed_data['record'] = {
                'uri': getattr(rec, 'uri', ''),
                'cid': getattr(rec, 'cid', '')
            }
    
    return embed_data if embed_data else None

def extract_facets(record):
    """Extrai menções, hashtags e links do post"""
    facets_data = {
        'mentions': [],
        'links': [],
        'tags': []
    }
    
    if hasattr(record, 'facets') and record.facets:
        for facet in record.facets:
            if hasattr(facet, 'features'):
                for feature in facet.features:
                    # Menções
                    if hasattr(feature, 'did'):
                        facets_data['mentions'].append(feature.did)
                    # Links
                    elif hasattr(feature, 'uri'):
                        facets_data['links'].append(feature.uri)
                    # Tags
                    elif hasattr(feature, 'tag'):
                        facets_data['tags'].append(feature.tag)
    
    # Retorna apenas categorias não vazias
    return {k: v for k, v in facets_data.items() if v}

def on_message(message):
    """
    Callback que é chamado para cada mensagem recebida do Firehose.
    """
    stats['total_messages'] += 1
    
    commit = parse_subscribe_repos_message(message)
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return

    # Decodifica os blocos CAR
    car = CAR.from_bytes(commit.blocks)
    
    for op in commit.ops:
        # Filtra apenas a criação de posts
        if op.action == "create" and op.path.startswith("app.bsky.feed.post"):
            stats['total_posts'] += 1
            
            # Obtém o registro do CAR usando o CID
            record_raw = car.blocks.get(op.cid)
            if record_raw:
                try:
                    # Decodifica o registro
                    record = models.get_or_create(record_raw, strict=False)
                    
                    # Verifica se é um post e tem texto
                    if hasattr(record, 'text') and record.text:
                        post_text = record.text.lower()
                        
                        # Verifica se contém alguma palavra-chave
                        matching_keywords = [kw for kw in KEYWORDS if kw.lower() in post_text]
                        
                        if matching_keywords:
                            stats['matching_posts'] += 1
                            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            # Dados básicos do post
                            post_data = {
                                'timestamp_coletado': timestamp,
                                'author_did': commit.repo,
                                'text': record.text,
                                'keywords_found': matching_keywords,
                                'post_uri': f"at://{commit.repo}/{op.path}"
                            }
                            
                            # Adiciona timestamp original de criação
                            if hasattr(record, 'createdAt'):
                                post_data['created_at'] = record.createdAt
                            
                            # Adiciona dados de resposta (se for uma resposta)
                            if hasattr(record, 'reply'):
                                post_data['reply_to'] = {
                                    'root': getattr(record.reply, 'root', {}).get('uri', '') if hasattr(record.reply, 'root') else '',
                                    'parent': getattr(record.reply, 'parent', {}).get('uri', '') if hasattr(record.reply, 'parent') else ''
                                }
                            
                            # Adiciona dados de mídia incorporada (imagens, links, etc.)
                            embed_data = extract_embed_data(record)
                            if embed_data:
                                post_data['embeds'] = embed_data
                            
                            # Adiciona menções, hashtags e links
                            facets = extract_facets(record)
                            if facets:
                                post_data['facets'] = facets
                            
                            # Adiciona dados de idioma (se disponível)
                            if hasattr(record, 'langs'):
                                post_data['languages'] = record.langs
                            
                            # Adiciona informações técnicas
                            post_data['cid'] = op.cid
                            post_data['path'] = op.path
                            
                            # Adiciona à lista de posts encontrados
                            found_posts.append(post_data)
                            
                            # Exibe no terminal
                            print(f"\n{'='*60}")
                            print(f"[{timestamp}] Post relevante #{stats['matching_posts']}!")
                            print(f"Palavras-chave: {', '.join(matching_keywords)}")
                            print(f"Autor: @{commit.repo}")
                            print(f"Texto: {record.text[:300]}{'...' if len(record.text) > 300 else ''}")
                            print(f"{'='*60}")
                            
                            # Salva automaticamente a cada 10 posts
                            if stats['matching_posts'] % 10 == 0:
                                save_posts_to_file()
                            
                except Exception as e:
                    # Se houver erro ao decodificar, apenas continue
                    pass
    
    # Mostra estatísticas a cada 500 mensagens
    if stats['total_messages'] % 500 == 0:
        print(f"\n[STATS] Mensagens: {stats['total_messages']} | Posts: {stats['total_posts']} | Matches: {stats['matching_posts']}")


def main():
    """
    Configurando conexão...
    """
    client = FirehoseSubscribeReposClient()
    print("="*60)
    print("Monitor de Posts - Bluesky")
    print("="*60)
    print(f"Monitorando {len(KEYWORDS)} palavras-chave")
    print("Posts relevantes serão salvos automaticamente em JSON")
    print("(Kill com Ctrl+C)")
    print("="*60)
    
    client.start(on_message)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\nEncerrando o monitor...")
        print(f"Estatísticas finais:")
        print(f"  - Total de mensagens processadas: {stats['total_messages']}")
        print(f"  - Total de posts analisados: {stats['total_posts']}")
        print(f"  - Posts relevantes encontrados: {stats['matching_posts']}")
        
        # Salva os posts restantes
        save_posts_to_file()
        
        sys.exit(0)
    except Exception as e:
        print(f"\nErro: {e}")
        import traceback
        traceback.print_exc() 