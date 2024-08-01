## Описание

Этот проект представляет собой DAG (Directed Acyclic Graph) для Apache Airflow, который загружает данные о доменах из CSV-файла, анализирует их и выводит результаты. Основные функции включают получение топ-10 доменов, нахождение домена с самым длинным именем и определение позиции домена `airflow.com` в рейтинге.

## Установка

1. Убедитесь, что у вас установлен Apache Airflow. Если нет, вы можете установить его с помощью pip:

   ```bash
   pip install apache-airflow
   ```

2. Склонируйте этот репозиторий:

   ```bash
   git clone <URL_репозитория>
   cd <имя_папки_репозитория>
   ```

3. Убедитесь, что у вас установлены необходимые библиотеки:

   ```bash
   pip install pandas
   ```

## Использование

1. Запустите Airflow:

   ```bash
   airflow db init
   airflow webserver --port 8080
   airflow scheduler
   ```

2. Откройте веб-интерфейс Airflow в браузере по адресу `http://localhost:8080`.

3. Найдите DAG с именем `FGJ` и запустите его.

## Функции

- `get_data()`: Загружает данные из CSV-файла по указанному URL.
- `to_10_domains()`: Возвращает топ-10 доменов по численности.
- `longest_domain()`: Возвращает домен с самым длинным именем.
- `airflow_top()`: Возвращает позицию домена `airflow.com` в рейтинге.
- `print_data(ds)`: Выводит результаты анализа данных.

## Настройки DAG

- **owner**: FGJ
- **retries**: 2 (количество попыток в случае ошибки)
- **retry_delay**: 5 минут (задержка между попытками)
- **start_date**: 18 июля 2024 года
- **schedule_interval**: Запуск каждый день в 09:30

## Лицензия

Этот проект лицензирован на условиях MIT License. См. файл [LICENSE](LICENSE) для получения дополнительной информации.

## Контакты

Если у вас есть вопросы или предложения, пожалуйста, свяжитесь с автором проекта: FGJ.
