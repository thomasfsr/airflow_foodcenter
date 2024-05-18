Relatório online: https://airflow-foodcenter.onrender.com  

#### Tarefas:  
- Numa ação de marketing, para atrair mais entregadores, vamos dar uma bonificação para os 20 entregadores que possuem maior distância percorrida ao todo. A bonificação vai variar de acordo com o tipo de profissional que ele é e o modelo que ele usa para se locomover (moto, bike, etc). Levante essas informações.  
  
- Além disso, o time de Pricing precisa ajustar os valores pagos aos entregadores. Para isso, eles precisam da distribuição da distância média percorrida pelos motoqueiros separada por estado, já que cada região terá seu preço.  
  
- Por fim, o CFO precisa de alguns indicadores de receita para apresentar para a diretoria executiva. Dentre esses indicadores, vocês precisarão levantar (1) a receita média e total separada por tipo (Food x Good), (2) A receita média e total por estado. Ou seja, são 4 tabelas ao todo.  


#### Passos para rodar a aplicação:  
- Clonar repo:  
```bash
git clone https://github.com/thomasfsr/airflow_foodcenter
cd airflow_foodcenter
```
- Instalar Astronomer:  
Para rodar este projeto será necessário instalar o astro-cli que facilitará a instalação do AirFlow:
```bash
brew install astro 
``` 
Antes de iniciar o Airflow via astro-cli algumas váriaveis de ambiente deverão ser passadas em um arquivo .env na raiz do projeto:  
  
```
#pasta do google drive
folder_id = 1ek2v3SC1aPAfWmkcbZUl9Cxg0uKNIase

#postgre 
username = ***  
password = ***  
host = ***  
port = 5432
database_name = ***  
external_url = ***  
```
  
Obs.: A variável de ambiente folder só está disponivel porque é uma pasta pública no google drive. Porém os mesmos dados podem ser encontrados no Kaggle: https://www.kaggle.com/datasets/nosbielcs/brazilian-delivery-center

- Start no servidor airflow:  
```bash
astro dev start
``` 
  
- Ordem de ativação:  
No painel do airflow deverá ser inicializado a DAG etldag primeiro e após concluído pode ser inicializado a DAG weekly_dag.  
  
-  A primeira dag irá carregar as tabelas do google drive, validar as colunas com Pandera e, após validado, vai carregar as tabelas para o schema "Raw" do banco de dados PostgreSQL.  
  
- Em seguida será criado outros dois schemas "Silver" e "Gold".  
Silver: Views e Materialized Views com os dados limpos e concatenando os dados necessários para o cálculos dos KPIs.  
Gold: Tabelas de KPIs utilizando valores atualizados até aquele periodo.  
  
A DAG weekly_dag é irá criar novas tabelas calculando os KPIs com os valores atualizados.  
  
#### Relatório com Streamlit:  
Para inicializar é necessário instalar as dependências com o poetry:  
```bash
poetry shell  
poetry install  
```
E por fim, inicializar a aplicação do streamlit:  
```bash  
streamlit run app/main.py  
```




