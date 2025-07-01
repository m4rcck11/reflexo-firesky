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
                            
                            # Dados do post
                            post_data = {
                                'timestamp': timestamp,
                                'author': commit.repo,
                                'text': record.text,
                                'keywords_found': matching_keywords,
                                'post_uri': f"at://{commit.repo}/{op.path}"
                            }
                            
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