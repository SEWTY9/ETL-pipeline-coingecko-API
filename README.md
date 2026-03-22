# 🚀 Мой Первый ETL Проект

<div align="center">

**Полнофункциональный ETL-конвейер для анализа криптовалютных данных**

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.9.1-017CEE?logo=apache-airflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

</div>

---

## 📋 Описание

Этот проект демонстрирует полный цикл ETL-конвейера (Extract → Transform → Load) для работы с данными о криптовалютах:

- ⬇️ **Извлечение (Extract)** - получение реальных цен криптовалют из API CoinGecko
- 🔄 **Трансформация (Transform)** - обработка сырых данных в структурированный формат
- 💾 **Загрузка (Load)** - сохранение данных в PostgreSQL с разделением на слои (raw & gold)
- 📊 **Аналитика** - расчет метрик и анализ с использованием Pandas

---

## 🛠️ Установка и Запуск

### Требования

- ✅ Docker и Docker Compose (установите [отсюда](https://docs.docker.com/get-docker/))
- ✅ Git
- ✅ Понимание основ Python

### Быстрый старт

```bash
# 1️⃣ Клонируйте репозиторий
git clone https://github.com/YOUR_USERNAME/my-first-ETL.git
cd my-first-ETL

# 2️⃣ Запустите все сервисы
docker-compose up -d

# 3️⃣ Проверьте статус контейнеров
docker-compose ps
```

### 📍 Доступ к Сервисам

| Сервис | URL | Логин | Пароль |
|--------|-----|-------|--------|
| **Airflow UI** | http://localhost:8080 | airflow | airflow |
| **pgAdmin** | http://localhost:5050 | admin@admin.com | admin |
| **PostgreSQL** | localhost:5432 | postgres | postgres |

---

## 🚀 Использование

### Включение DAG в Airflow

1. Откройте http://localhost:8080
2. Найдите DAG `crypto_etl_pipeline`
3. Включите его (переключите toggle в ON)
4. Нажмите **Trigger DAG** для ручного запуска или ждите автоматического запуска (каждые 12 часов)

### Мониторинг выполнения

- 📝 Смотрите логи задач прямо в UI Airflow
- 📊 Проверяйте результаты в pgAdmin
- 🔍 Используйте SQL запросы для анализа данных

---

## 📁 Структура Проекта

```
my-first-ETL/
├── 📂 dags/
│   └── crypto_etl_dag.py           # Определение DAG для Airflow
├── 📂 db/
│   └── init.sql                    # SQL скрипт инициализации БД
├── 📂 etl/
│   ├── main.py                     # Основные функции ETL
│   ├── analytics.py                # Функции аналитики
│   ├── requirements.txt            # Python зависимости
│   └── Dockerfile                  # Docker образ для ETL контейнера
├── 📂 logs/                        # Логи Airflow
├── 📂 plugins/                     # Плагины Airflow
├── 📄 docker-compose.yml           # Настройка всех сервисов
├── 📄 .env                         # Переменные окружения
├── 📄 .gitignore                   # Файлы для исключения из Git
└── 📄 README.md                    # Этот файл
```

---

## 🛠️ Технологии

<table>
  <tr>
    <td align="center"><img src="https://img.shields.io/badge/Python-3.11-3776ab?logo=python&logoColor=white"/></td>
    <td align="center"><img src="https://img.shields.io/badge/Apache_Airflow-2.9.1-017CEE?logo=apache-airflow&logoColor=white"/></td>
    <td align="center"><img src="https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white"/></td>
  </tr>
  <tr>
    <td align="center"><img src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white"/></td>
    <td align="center"><img src="https://img.shields.io/badge/SQLAlchemy-ORM-red?logoColor=white"/></td>
    <td align="center"><img src="https://img.shields.io/badge/Pandas-Data_Analysis-150458?logo=pandas&logoColor=white"/></td>
  </tr>
</table>

---

## ✨ Особенности

- ✅ **Type Hints** - полная типизация для лучшей читаемости кода
- ✅ **Логирование** - детальные логи на каждом этапе
- ✅ **Обработка ошибок** - retry механизм и graceful error handling
- ✅ **Health Checks** - проверка доступности БД перед началом
- ✅ **Модульная архитектура** - чистый, переиспользуемый код
- ✅ **Docker Ready** - полностью контейнеризировано
- ✅ **Airflow Интеграция** - профессиональная оркестрация

---

## 🔧 Команды Docker

```bash
# Запуск всех сервисов
docker-compose up -d

# Остановка сервисов
docker-compose down

# Просмотр логов
docker-compose logs -f airflow-webserver

# Перезапуск конкретного сервиса
docker-compose restart postgres

# Проверка статуса
docker-compose ps
```

---

## 🎯 Что Дальше?

Идеи для расширения проекта:

- 📧 Добавить уведомления по email при обновлении данных
- 📱 Реализовать API для доступа к данным
- 📊 Добавить визуализацию с помощью Grafana/Plotly
- 🔐 Настроить авторизацию и права доступа
- 🌍 Расширить список криптовалют и источников данных
- ⚡ Оптимизировать производительность для больших объемов

---

## 🤝 Контрибьютинг

Приветствуются pull requests! Для больших изменений сначала откройте issue для обсуждения.

---

## 📝 Лицензия

MIT License - см. файл LICENSE для деталей

---

## 👨‍💻 Автор

Создано с ❤️ для изучения ETL и Airflow

---

<div align="center">

**Сделано с ❤️ | Звёзды приветствуются ⭐**

</div>