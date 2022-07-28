import streamlit as st
import sqlalchemy
import pymysql
import datetime

from sqlalchemy import and_, or_
from datetime import timedelta



class Singleton(type):

	# Referência: https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
	
	_instances = {}

	def __call__(cls, *args, **kwargs):

		if cls not in cls._instances:
			cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
		return cls._instances[cls]




class database(metaclass=Singleton):

	# Páginas importantes acessadas durante o desenvolvimento desta função.
	# 
	# Exemplos SQLAlchemy — Python Tutorial
	# https://towardsdatascience.com/sqlalchemy-python-tutorial-79a577141a91
	# 
	# Working with Engines and Connections [Acesso em 18 de julho de 2022]
	# https://docs.sqlalchemy.org/en/13/core/connections.html
	#
	# Using Connection Pools with Multiprocessing or os.fork() [Acesso em 18 de julho de 2022]
	# https://docs.sqlalchemy.org/en/13/core/pooling.html#pooling-multiprocessing


	__engine = None

	# Initializing 
	def __init__( self ):

		dialectdriver = 'mysql+pymysql'

		user     = st.secrets["user"]
		passwd   = st.secrets["passwd"]
		host     = st.secrets["host"]
		port     = st.secrets["port"]
		database = st.secrets["db"]

		connectionString = '{dialectdriver}://{user}:{passwd}@{host}:{port}/{db}'.format( dialectdriver=dialectdriver, user=user, passwd=passwd, host=host, port=port, db=database )

		self.__engine = sqlalchemy.create_engine( connectionString )


	def getEngine(self):
		return self.__engine





def consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras( ultimasHoras ):

	# Páginas importantes acessadas durante o desenvolvimento desta função.
	# Acesso em 27 de julho de 2022
	# 
	# Group by and count function in SQLAlchemy
	# https://www.geeksforgeeks.org/group-by-and-count-function-in-sqlalchemy/
	#
	# Query.scalar()
	# https://docs.sqlalchemy.org/en/14/orm/query.html#sqlalchemy.orm.query.Query.scalar
	#
	# SQLAlchemy: print the actual query
	# https://stackoverflow.com/questions/5631078/sqlalchemy-print-the-actual-query
	#
	# Conjunctions
	# https://docs.sqlalchemy.org/en/14/core/tutorial.html#conjunctions

	myDatabase = database()
	engine = myDatabase.getEngine()

	with engine.connect() as connection:
		metadata = sqlalchemy.MetaData()
		tbCeps   = sqlalchemy.Table('CEPs', metadata, autoload=True, autoload_with=engine)

		dt_now = datetime.datetime.now(datetime.timezone.utc)
		d = dt_now - timedelta(hours=ultimasHoras)

		query = sqlalchemy.select([sqlalchemy.func.count(sqlalchemy.func.distinct(tbCeps.columns.co_cep))])\
		          .where( and_( tbCeps.columns.statusProcessamento == 2,\
		                        tbCeps.columns.fimRaspagem >= d,\
		                        and_( or_( and_( tbCeps.columns.totalResultados > 0, tbCeps.columns.planoDisponivel == True ), \
		                        	       tbCeps.columns.planoDisponivel == False ) ) ) )
		
		count = connection.execute(query).scalar()

	return count




def consultaTotalCEPsDistintos():

	myDatabase = database()
	engine = myDatabase.getEngine()

	with engine.connect() as connection:
		metadata = sqlalchemy.MetaData()
		tbCeps   = sqlalchemy.Table('CEPs', metadata, autoload=True, autoload_with=engine)

		query = sqlalchemy.select([sqlalchemy.func.count(sqlalchemy.func.distinct(tbCeps.columns.co_cep))])
		count = connection.execute(query).scalar()

	return count




def consultaTotalPlanosColetados():

	myDatabase = database()
	engine = myDatabase.getEngine()

	with engine.connect() as connection:
		metadata = sqlalchemy.MetaData()
		tbPlanos   = sqlalchemy.Table('Planos', metadata, autoload=True, autoload_with=engine)

		query = sqlalchemy.select([sqlalchemy.func.count(tbPlanos.columns.idPlano)])
		count = connection.execute(query).scalar()

	return count






if __name__ == "__main__":

	# Páginas importantes acessadas durante o desenvolvimento desta função.
	# Acesso em 27 de julho de 2022
	# 
	# Deploying a web app using MySQL server via Streamlit
	# https://medium.com/@itssaad.muhammad/deploying-a-web-app-using-mysql-server-via-streamlit-ca28ecd02bb0

	ultima1h   = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(1)
	ultima2h   = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(2)
	ultima24h  = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(24)
	ultima48h  = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(24 * 2)
	ultima1s   = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(24 * 7)
	ultima2s   = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(24 * 7 * 2)
	ultimo365d = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(24 * 365)

	totalPlanosColetados = consultaTotalPlanosColetados()
	totalCEPsDistintos = consultaTotalCEPsDistintos()

	deltaUltima1h  = "{:.2f} %".format(round( ultima1h  / (ultima2h - ultima1h )   , 2)) 
	deltaUltima24h = "{:.2f} %".format(round( ultima24h / (ultima48h - ultima24h ) , 2)) 
	deltaUltima1s  = "{:.2f} %".format(round( ultima1s  / (ultima2s - ultima1s )   , 2)) 
	
	porcentagemConcluida = (ultimo365d / totalCEPsDistintos) * 100
	tempoEstimadoEmDias = (totalCEPsDistintos - ultimo365d) / ultima24h
	
	st.title("Melhor Plano")
	st.title("Status em tempo real da raspagem")
	st.subheader("Total de CEPs processados") 

	st.text("")
	
	col1, col2, col3 = st.columns(3)
	col1.metric(label = "Última hora"    , value = str(ultima1h) , delta = deltaUltima1h )
	col2.metric(label = "Últimas 24 horas", value = str(ultima24h), delta = deltaUltima24h)
	col3.metric(label = "Última semana"  , value = str(ultima1s) , delta = deltaUltima1s)
	
	st.text("")
	st.text("")
	st.subheader("{:.0f} CEPs raspados. {:.2f}% já concluído.".format( ultimo365d, porcentagemConcluida ) )
	
	my_bar = st.progress(0)
	my_bar.progress( int( porcentagemConcluida )  )
	
	st.text("")
	st.text("")
	st.subheader("Tempo estimado para conclusão: {:.1f} dias".format( round(tempoEstimadoEmDias,1) ) )
	st.text("A estimativa foi feita com o total de CEPs processados nas últimas 24 horas.")