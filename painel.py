import streamlit as st
import sqlalchemy
import pymysql
import datetime
import math

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
	#
	# Python String Format Cookbook
	# https://mkaz.blog/code/python-string-format-cookbook/

	ultima1h   = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(1)
	ultima2h   = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(2)
	ultima24h  = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(24)
	ultima48h  = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(24 * 2)
	ultima1s   = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(24 * 7)
	ultima2s   = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(24 * 7 * 2)
	ultimo365d = consultaTotalCEPsDistintosJaProcessadosNasUltimasHoras(24 * 365)

	totalPlanosColetados = consultaTotalPlanosColetados()
	totalCEPsDistintos = consultaTotalCEPsDistintos()


	#
	# Cálculo do percentual comparativo
	#

	totalHora1 = ultima1h
	totalHora2 = ultima2h - ultima1h

	if totalHora1 > totalHora2:
		deltaUltima1h = (totalHora1 / totalHora2 - 1) * 100      if 0 != totalHora2 else  math.inf
	else:
		deltaUltima1h = (1 - totalHora1 / totalHora2) * 100 * -1 if 0 != totalHora2 else 0

	deltaUltima1hStr  = "{:.2f} %".format(round( deltaUltima1h, 2)) 



	totalHora1a24  = ultima24h
	totalHora25a48 = ultima48h - ultima24h

	if totalHora1a24 > totalHora25a48:
		deltaUltima24h = (totalHora1a24 / totalHora25a48 - 1) * 100      if 0 != totalHora25a48 else  math.inf
	else:
		deltaUltima24h = (1 - totalHora1a24 / totalHora25a48) * 100 * -1 if 0 != totalHora25a48 else  0

	deltaUltima24hStr = "{:.2f} %".format(round( deltaUltima24h, 2))



	totalSemana1 = ultima1s
	totalSemana2 = ultima2s - ultima1s

	if totalSemana1 > totalSemana2:
		deltaUltima1s = (totalSemana1 / totalSemana2 - 1) * 100      if 0 != totalSemana2 else  math.inf
	else:
		deltaUltima1s = (1 - totalSemana1 / totalSemana2) * 100 * -1 if 0 != totalSemana2 else 0
	 
	deltaUltima1sStr  = "{:.2f} %".format(round( deltaUltima1s, 2)) 


	
	porcentagemConcluida = (ultimo365d / totalCEPsDistintos) * 100
	tempoEstimadoEmDias = (totalCEPsDistintos - ultimo365d) / ultima24h
	


	st.title("Melhor Plano")
	st.title("Status em tempo real da raspagem")
	st.text("")

	st.subheader("CEPs processados") 
	st.text("")
	
	l1col1, l1col2, l1col3 = st.columns(3)
	l1col1.metric(label = "Última hora"      , value = str(ultima1h) , delta = deltaUltima1hStr )
	l1col2.metric(label = "Últimas 24 horas" , value = str(ultima24h), delta = deltaUltima24hStr)
	l1col3.metric(label = "Última semana"    , value = str(ultima1s) , delta = deltaUltima1sStr )

	st.text("")
	st.subheader("Outras métricas") 
	st.text("")
	
	l2col1, l2col2, l2col3 = st.columns(3)
	l2col1.metric(label = "Planos coletados" , value = str(totalPlanosColetados) )
	l2col2.metric(label = "Total de CEPs"    , value = str(totalCEPsDistintos) )
	l2col3.metric(label = "CEPs processados" , value = str(ultimo365d) )
	

	st.text("")
	st.text("")
	st.subheader("{:.2f}% dos CEPS já foram processados.".format(porcentagemConcluida))

	my_bar = st.progress(0)
	my_bar.progress( int( porcentagemConcluida )  )
	
	st.text("")
	st.text("")
	st.subheader("Tempo estimado para conclusão: {:.1f} dias".format( round(tempoEstimadoEmDias,1) ) )
	st.caption("Estimativa feita utilizando o total de CEPs processados nas últimas 24 horas.")