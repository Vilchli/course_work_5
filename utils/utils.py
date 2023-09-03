import json
import psycopg2 as psycopg2
import requests as requests

URL_HH = 'https://api.hh.ru/vacancies'


def get_vacancies(employers):
    """Функция для получения вакансий"""
    list_ = []
    all_vacancies = []
    for employer in employers:
        params = {
            'employer_id': employer,
            'page': 0,
            'per_page': 100,
            'only_with_salary': True
        }

        response = requests.get(URL_HH, params)
        data = response.json()['items']
        list_.extend(data)
        if len(data) == 100:
            while True:
                params['page'] += 1
                response = requests.get(URL_HH, params)
                result_page = response.json()
                if result_page.get('items'):
                    new_data = response.json()['items']
                    list_.extend(new_data)
                    if len(new_data) == 100:
                        params['page'] += 1
                    else:
                        break
                else:
                    break
        all_vacancies.extend(list_)
    result = []
    for vacancy in all_vacancies:
        result.append({"id": vacancy["id"],
                       "name": vacancy["name"],
                       "salary_from": vacancy["salary"]["from"],
                       "salary_to": vacancy["salary"]["to"],
                       "URL": vacancy["alternate_url"],
                       "employer_id": vacancy["employer"]["id"]})
    return result


def create_database(DB_name, config):
    """Функция для создания базы данных"""
    connection = psycopg2.connect(dbname="postgres", **config)
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DB_name};")
    cursor.execute(f"CREATE DATABASE {DB_name}")
    cursor.close()
    connection.close()


def create_tables(cursor):
    """Функция для создания таблиц"""
    cursor.execute(f"CREATE TABLE companies (company_id VARCHAR(10) PRIMARY KEY,"
                   f"company VARCHAR(50));"
                   f"CREATE TABLE vacancies (id VARCHAR(10), company_id VARCHAR(10) REFERENCES companies (company_id),"
                   f"vacancy_name VARCHAR(100), salary_from INT, salary_to INT,"
                   f"URL TEXT)")


def fill_in_the_table_companies(cursor, employers):
    """Функция заполнения таблицы companies"""
    for company_id, company_name in employers.items():
        cursor.execute(f"INSERT INTO companies (company_id, company) VALUES (%s, %s)", (company_id, company_name))


def fill_in_the_table_vacancies(cursor, vacancies):
    """Функция заполнения таблицы vacancies"""
    for vacancy in vacancies:
        cursor.execute(f"INSERT INTO vacancies (id, company_id, vacancy_name, salary_from, salary_to, URL)"
                       f"VALUES (%s, %s, %s, %s, %s, %s)", (vacancy["id"], vacancy["employer_id"], vacancy["name"],
                                                            vacancy["salary_from"], vacancy["salary_to"],
                                                            vacancy["URL"]))
