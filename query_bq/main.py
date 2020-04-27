from google.cloud import bigquery
import json
from werkzeug.exceptions import BadRequest 

# A schema with the name of query parameter and the name that will be on bigquery
schema_query_translator =  {    
                "bairro":"bairro",
                "area_total":"area_total",
                "area_util":"area_util",
                "banheiros":"banheiros",
                "vagas":"vagas",
                "quartos":"quartos",
                "suites":"suites",
                "aluguel":"aluguel",
                "condominio":"condominio",
                "val_iptu":"iptu",
                "lat":"latitude",
                "long":"longitude",
                "distancia":"distancia"¸
                "outros":"additions",
                "venda":"venda",
                "cidade":"cidade",
                }

# The data types of the schema
schema_data_types = {
    "area_total": int,
    "area_util": int,
    "banheiros": int,
    "vagas": int ,
    "quartos": int,
    "suites": int,
    "condominio": int,
    "iptu": int,
    "aluguel": float,
    "latitude": float,
    "longitude": float,
    "additions": str,
    "bairro":str,
    "venda": str,
    "cidade": str,
    "distancia":float
}

# Operations translator from query string to 
operators_str = {
    'lt':'<'
    'gt':'>'
    'gte':'>='
    'lte':'<='
}           
            
def get_query_values(request, schema_dict):
    query_dict = {}
    for key,sql_col_name in schema_dict.items():
        if request.args.getlist(key):
            query_dict[sql_col_name] = request.args.getlist(key)
    return query_dict


def validate_operators(search_value:str, operator_dict:dict):
    """Receive a string value to check for query string operators
        If it's there returns the search value cleaned from the opetaro and the SQL operator acordingly

    Arguments:
        search_value [str] -- [description]
        operator_dict [dict[str]] -- [description]
    """     
    # Default value is ==
    search_parameter = (search_value,'==')
    for operator,replacer in operator_dict:
        if operator in search_value:
            search_parameter = (search_value.replace(operator,''),replacer)
    return search_parameter

def transform_into_sql(col_name: str, tuple_operator: tuple):
    where_string = '{0} {1} {2}'.format(col_name,tuple_operator[0],tuple_operator[1])
    return where_string

def get_distance_values(query_dict: dict):
    if "distancia" in query_dict and "latidute" in query_dict and "longitude" in query_dict:
        string = "ST_GEOGPOINT({0},{1})".format(query_dict["latitude"],query_dict["longitude"])
    else:
        raise BadRequest(descrition="Distancia, latitude ou longitude inválidas")
    geopoint_where = "ST_DISTANCE({0},ST_GEOGPOINT(longitude,latitude))0".format(query_dict['distancia'])
    
    del query_dict['distancia']
    del query_dict['latidute']
    del query_dict['longitude']

    return query_dict, geopoint_where
 

def query_bq(request):
    # Getting tables available on bigquery
    client = bigquery.Client(project='rental-organizer')
    table = client.get_table('{0}.{1}'.format(dataset, table_name))
    
    # Getting the values to query bigquery
    query_values = get_query_values(request,schema_dict)

    if query_values:
        return abort(422,description="Nenhum paramêtro válido feito na pesquisa.")

    if len(query_values.keys) <= 3:
        return abort(422,description="A pesquisa precisa ter pelo menos 3 filtros.")

    query_values_selected, geopoint_where = get_distance_values(query_values)

    # Replacing the query string for SQL comparators and validating the data types
    sql_dict = {}s
    for column_name, search_list in query_values_selected:
        # Return a list of tuples with (search_value, sql comparator)
        comparators_list = [validate_operators(search) for search in search_list]

        # Checking if it's invalid datatype
        if isinstance(schema_data_types[column_name],int) or isinstance(schema_data_types[column_name],float):
            if any([not search_val.replace('.','').isnumeric() for search_val,query_comparator])
                return abort(422,description="Existe Valor inválido na pesquisa.".format(column_name))
        
        sql_dict[column_name] = comparators_list

    operators_string = [transform_into_sql(col,op_tuple) for col, op_tuple in sql_dict.items]
    where_clause = ' AND '.join(operators_string) + ' AND ' + geopoint_where
    query = 'SELECT * FROM imoveis_online WHERE (%s)'%(, where_clause)

