# **Order Management System com Integração ao Google Cloud Pub/Sub**

Este projeto é um Order Management System que utiliza Node.js, PostgreSQL e Google Cloud Pub/Sub para gerenciar pedidos, clientes, produtos e itens de pedidos. O sistema escuta mensagens do Google Pub/Sub, processa essas mensagens e insere as informações no banco de dados PostgreSQL.
Tecnologias Utilizadas

    Node.js: Ambiente de execução JavaScript no lado do servidor.
    PostgreSQL: Banco de dados relacional utilizado para armazenar as informações.
    pg: Biblioteca para comunicação com o banco de dados PostgreSQL.
    UUID: Utilizado para gerar identificadores únicos para entidades como pedidos, clientes, e itens.
    Google Cloud Pub/Sub: Serviço de mensageria usado para processar e consumir mensagens de forma assíncrona.


## Estrutura do BD

![Sem título](https://github.com/user-attachments/assets/a5a5e8d1-1d2d-4f31-8d35-b5110a380160)
