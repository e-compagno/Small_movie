import pandas as pd 
import sqlalchemy as db
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String, Float, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm import sessionmaker
import os

SQL_USR, SQL_PSW= os.environ['SQL_USR'], os.environ['SQL_PSW']
mysql_str = 'mysql+mysqlconnector://'+SQL_USR+':'+SQL_PSW+'@localhost:3306/'

# Create Small_movie database
print('Create database Small_movie.')
print('-'*30)
engine = db.create_engine(mysql_str)
con=engine.connect()
con.execute('commit')
con.execute('CREATE DATABASE if NOT EXISTS Small_movie;')
con.close()
print('Done.\n')

print('Create tables')
print('-'*30)
# Load csv data
links = pd.read_csv('../data/links.csv',\
                    header=0)
movies = pd.read_csv('../data/movies.csv',\
                     header=0)
ratings = pd.read_csv('../data/ratings.csv',\
                     header=0)
tags = pd.read_csv('../data/tags.csv',\
                     header=0)

# Select Small_movie database and create tables
engine = db.create_engine(mysql_str+'Small_movie')
con=engine.connect()

Base=declarative_base()

class Links(Base):
    """
    Class for creating the links table.
    """
    __tablename__ = 'links'
    
    movieId=Column(Integer, primary_key=True) 
    imdbId=Column(Integer)
    tmdbId=Column(Float)

    def __init__(self, movieId, imdbId, tmdbId):
        self.movieId = movieId
        self.imdbId = imdbId
        self.tmdbId = tmdbId

class Movies(Base):
    """
    Class for creating the movies table.
    """
    __tablename__ = 'movies'
    
    id=Column(Integer, primary_key=True)
    movieId=Column(Integer)
    title=Column(Text)
    genres=Column(Text)
    
    def __init__(self, id, movieId, title, genres):
        self.movieId=movieId
        self.title=title
        self.genres=genres

class Ratings(Base):
    """
    Class for creating the ratings table.
    """
    __tablename__ = 'ratings'
    
    id=Column(Integer, primary_key=True)
    userId=Column(Integer)
    movieId=Column(Integer)
    rating=Column(Float)
    timestamp=Column(Integer)
    
    def __init__(self, userId, movieId, rating, timestamp):
        self.userId = userId
        self.movieId = movieId
        self.rating = rating
        self.timestamp=timestamp
    
def Tags(Base):
    """
    Class for creating the tags table.
    """
    __tablename__ = 'tags'
    
    id=Column(Integer, primary_key=True)
    userId=Column(Integer)
    movieId=Column(Integer)
    tag=Column(Text)
    timestamp=Column(Integer)
    
    def __init__(self, id, userId, movieId, tag, timestamp):
        self.id = id
        self.userId=userId
        self.movieId=movieId
        self.tag=tag
        self.timestamp=timestamp
    
    
Base.metadata.create_all(engine)
con.close()

print('Done.\n')

print('Insert data from csv to database.\n')
print('-'*30)
# Insert data to database
engine = db.create_engine(mysql_str+'Small_movie')
con=engine.connect()

links.to_sql(name='links',\
             con=engine,\
             if_exists='replace')

movies=movies.reset_index()
movies=movies.rename(columns={'index':'id'})
movies.to_sql(name='movies',\
             con=engine,\
             if_exists='replace')

ratings=ratings.reset_index()
ratings=movies.rename(columns={'index':'id'})
ratings.to_sql(name='ratings',\
             con=engine,\
             if_exists='replace')

tags=tags.reset_index()
tags=tags.rename(columns={'index':'id'})
tags.to_sql(name='tags',\
             con=engine,\
             if_exists='replace')

con.close()
print('Done.')