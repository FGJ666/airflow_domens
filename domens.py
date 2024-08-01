import pandas as pd  # Импортируем библиотеку pandas для работы с данными
# Импортируем timedelta для работы с временными интервалами
from datetime import timedelta
from datetime import datetime  # Импортируем datetime для работы с датами и временем

from airflow import DAG  # Импортируем библиотеку Airflow для создания DAG
# Импортируем PythonOperator для выполнения Python-функций в Airflow
from airflow.operators.python import PythonOperator  # type: ignore

# URL-адрес для загрузки данных
TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'


def get_data():
    """Загружает данные из CSV-файла по указанному URL."""
    domains = pd.read_csv(TOP_1M_DOMAINS, names=['top', 'domain'])
    return domains


def to_10_domains():
    """Возвращает топ-10 доменов по численности."""
    top_10 = get_data().sort_values('top').head(10)
    return top_10


def longest_domain():
    """Возвращает домен с самым длинным именем."""
    len_domens = get_data()
    len_domens['length'] = len_domens['domain'].apply(len)
    return len_domens.sort_values(['length', 'domain']).tail(1)


def airflow_top():
    """Возвращает позицию домена airflow.com в рейтинге."""
    return get_data().query("domain == 'airflow.com'").iat[0, 0]


def print_data(ds):
    """Выводит результаты анализа данных."""
    date = ds

    print(f'Топ-10 доменных зон на - {date}')
    print(f'Топ-10 доменных зон по численности доменов - \n{to_10_domains()}')

    print(f'Домен с самым длинным именем на - {date}')
    print(f'\nДомен с самым длинным именем - {longest_domain().iat[0, 1]}')

    print(f'На каком месте находится домен airflow.com на - {date}')
    print(f'\nНа каком месте находится домен airflow.com - {airflow_top()}')


# Настройки по умолчанию для DAG
default_args = {
    'owner': 'mi_sozonov',  # Владелец DAG
    'depends_on_past': False,  # Зависимость от предыдущих запусков
    'retries': 2,  # Количество попыток в случае ошибки
    'retry_delay': timedelta(minutes=5),  # Задержка между попытками
    'start_date': datetime(2024, 7, 18),  # Дата начала запуска
}

# Создание DAG
dag = DAG('domains_mi_sozonov', default_args=default_args,
          schedule_interval='30 9 * * *', tags=['mi_sozonov'])

# Операторы задач
t1_get_data = PythonOperator(task_id='get_data',
                             python_callable=get_data,
                             dag=dag)

t2_to_10_domains = PythonOperator(task_id='to_10_domains',
                                  python_callable=to_10_domains,
                                  dag=dag)

t2_longest_domain = PythonOperator(task_id='longest_domain',
                                   python_callable=longest_domain,
                                   dag=dag)

t2_airflow_top = PythonOperator(task_id='airflow_top',
                                python_callable=airflow_top,
                                dag=dag)

t4_print_data = PythonOperator(task_id='print_data',
                               python_callable=print_data,
                               dag=dag)

# Установка зависимостей между задачами
t1_get_data >> [t2_to_10_domains, t2_longest_domain,
                t2_airflow_top] >> t4_print_data
