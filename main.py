import json

from classes.DBManager import DBManager
from config.config import config

from utils.utils import *

employers = {864086: "getmatch",
             2324020: "Точка",
             2548771: "Brand Analytics",
             1918903: "Decart IT-production",
             2596438: "ООО ПКФ МЕТИНВЕСТ-СЕРВИС",
             3778: "АО ИнфоТеКС",
             2129243: "АО ОКБ",
             681319: "ЗАЗЕКС",
             1740: "Яндекс",
             3776: "МТС"}

DB_name = "vacancies"
config = config()

if __name__ == '__main__':
    vacancies = get_vacancies(employers)
    create_database(DB_name, config)
    print("База данных была создана успешно")
    config.update({"dbname": DB_name})

    try:
        with psycopg2.connect(**config) as connection:
            with connection.cursor() as cursor:
                create_tables(cursor)
                print("таблицы создана успешно")
                fill_in_the_table_companies(cursor, employers)
                print("Таблица companies заполнена")
                fill_in_the_table_vacancies(cursor, vacancies)
                print("Таблица vacancies заполнена")
    finally:
        connection.close()


    db_manager = DBManager(config)
    # print(db_manager.get_companies_and_vacancies_count())
    # print(db_manager.get_all_vacancies())
    # print(db_manager.get_avg_salary())
    # print(db_manager.get_vacancies_with_higher_salary())
    # print(db_manager.get_vacancies_with_keyword("Python"))

    db_manager.close()
