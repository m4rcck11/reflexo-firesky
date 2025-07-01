Automação simples para monitorar e coletar posts em tempo real no Bluesky.

Essa é uma automação simples para coleta de dados. O script conecta-se com a biblioteca atproto, que filtra os dados da firehose do BlueSky. 

Pontos interessantes: 
- Os eventos vem codificados
- e agrupados (curtidas e posts, por exemplo, estão no mesmo evento).

Para contornar isso, decodificamos e filtramos dentro do evento Create (evento que inclui os texto dos posts).

O uso é bem simples. Só baixar, instalar a biblioteca atproto e rodar o script.

A precisão dos Arrays, infelizmente, tem alguns bugs que precisam ser resolvidos: como meu intuito é encontrar urls (extremamente específicas), não contabilizei substrings. Uma correção para o futuro.
