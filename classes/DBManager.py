import psycopg2


class DBManager:
    """Класс для работы с базой данных"""
    def __init__(self, config):
        self.connection = psycopg2.connect(**config)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()


    def get_companies_and_vacancies_count(self):
        """получает список всех компаний и количество вакансий у каждой компании."""
        self.cursor.execute("SELECT company, COUNT(vacancy_name) FROM vacancies JOIN companies USING(company_id) "
                            "GROUP BY company")
        result = self.cursor.fetchall()
        return result

    def get_all_vacancies(self):
        """получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию."""
        self.cursor.execute("SELECT company, vacancy_name, salary_from, salary_to, URL FROM vacancies "
                            "JOIN companies USING(company_id)")
        result = self.cursor.fetchall()
        return result
    def get_avg_salary(self):
        """получает среднюю зарплату по вакансиям."""
        self.cursor.execute("SELECT AVG(salary_from), AVG(salary_to) FROM vacancies")
        result = self.cursor.fetchall()
        return result

    def get_vacancies_with_higher_salary(self):
        """получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        self.cursor.execute("SELECT * FROM vacancies WHERE salary_from > (SELECT AVG(salary_from) "
                            "FROM vacancies) OR salary_to > (SELECT AVG(salary_to) FROM vacancies)")
        result = self.cursor.fetchall()
        return result
    def get_vacancies_with_keyword(self, query):
        """получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python."""
        for word in query.split():
            self.cursor.execute(f"SELECT * FROM vacancies WHERE vacancy_name LIKE '%{word}%'")
        result = self.cursor.fetchall()
        return result

    def close(self):
        self.connection.close()
        self.cursor.close()
