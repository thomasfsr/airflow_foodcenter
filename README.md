Online Report: https://airflow-foodcenter.onrender.com  
  
# Overview:
The dataset from this project can be found [here in kaggle](https://www.kaggle.com/datasets/nosbielcs/brazilian-delivery-center).  
The main idea was to do a EDA (Exploratory Data Analysis) on this data to answer some question, but I saw a good oportunity to flex some Airflow skills that I recently learned on the workshop of data engineering.  
  
## Tasks [EN/PT]: 
- Marketing Action: In a marketing action to attract more delivery drivers, we will offer a bonus to the top 20 drivers who have traveled the longest distances overall. The bonus will vary according to the type of professional they are and the mode of transportation they use (motorcycle, bike, etc). Gather this information.
  
- Pricing Team Requirement: Additionally, the Pricing team needs to adjust the payments to the delivery drivers. For this purpose, they require the distribution of the average distance traveled by motorcyclists separated by state since each region will have its own pricing.
  
- CFO's Requirement: Finally, the CFO needs some revenue indicators to present to the executive board. Among these indicators, you will need to gather (1) the average and total revenue separated by type (Food vs. Goods), (2) the average and total revenue per state. In other words, there are 4 tables in total. 
  

## Tools Overview:  
  
### Orchestration:
![](images/astro_air.png)(https://docs.astronomer.io/astro/cli/install-cli)  
  
For the ETL (Extract Transform Load) I used astronomer. Astronomer is a solution that builds in the whole infraestructure for Airflow server.  It uses **Docker Compose** to download the necessary images for Airflow and build them:
  
![](images/compose.png)  
  
### Validation:  
![](images/pandera_pandas.png)  
  
To validate each table that would be loaded to the database I choose pandera because it is fast and seamlessly integrates with pandas dataframes.  
  

# Installing and Start:
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
astro dev start wait=20m
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




