import re
import datetime
from unicodedata import decimal
from boto3.dynamodb.conditions import Key,Attr
from boto3 import resource
import config

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
REGION_NAME = config.REGION_NAME
ENDPOINT_URL = config.ENDPOINT_URL

resource = resource(
    'dynamodb',
    endpoint_url          = ENDPOINT_URL,
    aws_access_key_id     = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name           = REGION_NAME
)

def create_table_movie():    
    table = resource.create_table(
        TableName = 'Movie', #Name of the table
        KeySchema = [
            {
                'AttributeName': 'id',
                'KeyType'      : 'HASH' # HASH = partition key, RANGE = sort key
            }
        ],
        AttributeDefinitions = [
            {
                'AttributeName': 'id', # Name of the attribute
                'AttributeType': 'N'   # N = Number (S = String, B= Binary)
            }                         
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits'  : 20,
            'WriteCapacityUnits': 20
        }
    )
    return table



movieTable = resource.Table('Movie')
def write_movie_info(data): # UserTable = resource.Table('user')
    response = movieTable.put_item(
        Item = {
            'id'                     :  int(data['imdb_title_id'].split('tt')[1]),
            'imdb_title_id'          :  data['imdb_title_id'], 
            'title'                  :  data['title'], 
            'original_title'         :  data['original_title'],  
            'year'                   :  int(data['year']) if len(data['year']) > 0 else data['year'], 
            'date_published'         :  data['date_published'], 
            'genre'                  :  data['genre'], 
            'duration'               :  int(data['duration']) if len(data['duration']) > 0 else data['duration'], 
            'country'                :  data['country'], 
            'language'               :  data['language'], 
            'director'               :  data['director'], 
            'writer'                 :  data['writer'], 
            'production_company'     :  data['production_company'], 
            'actors'                 :  data['actors'],  
            'description'            :  data['description'], 
            'avg_vote'               :  data['avg_vote'],
            'votes'                  :  int(data['votes']) if len(data['votes']) > 0 else data['votes'],
            'budget'                 :  data['budget'],
            'usa_gross_income'       :  data['usa_gross_income'],
            'worlwide_gross_income'  :  data['worlwide_gross_income'],
            'metascore'              :  data['metascore'], 
            'reviews_from_users'     :  int(data['reviews_from_users']) if len(data['reviews_from_users']) > 0 else data['reviews_from_users'], 
            'reviews_from_critics'   :  int(data['reviews_from_critics']) if len(data['reviews_from_critics']) > 0 else data['reviews_from_critics']
        }
    )
    return response


def get_movie_info_wrt_director(director, yearFrom, yearTo):
    response = movieTable.scan(FilterExpression=Attr('director').eq(director) & Attr('year').between(yearFrom, yearTo))
    data = response['Items']
    return data

def get_movies_greater_than_given_user_review(user_review):
    response = movieTable.scan(FilterExpression=Attr('reviews_from_users').gte(user_review))
    data = response['Items']
    sorted_data = sorted(data,key = lambda x:x["reviews_from_users"], reverse=True)
    return sorted_data

def get_highest_budget_movies(country, year):
    response = movieTable.scan(FilterExpression=Attr('year').eq(year) & Attr('country').contains(country))
    data = response['Items']
    sorted_data = sorted(data,key = lambda x:  int(re.sub('[^0-9]', '', x["budget"])) if len(x["budget"]) > 0 else 0, reverse=True)
    return sorted_data

def delete_movie_information(id):
    response = movieTable.delete_item(
        Key = {
            'id': id
        }
    )

    return response
