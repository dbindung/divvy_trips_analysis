# Анализ открытых данных поездок Divvy

Этот репозиторий содержит итоговый проект по анализу данных, выполненный как итоговый проект в рамках специализации "Большие данные" Яндекс Лицея.

В проекте проведён анализ открытых данных о поездках сервиса велопроката Divvy (Чикаго): от сбора и очистки данных до расчёта юнит-экономики и построения дашборда.


## Что сделано

- Объединены десятки CSV-файлов с поездками за разные годы
- Структура данных приведена к единому формату
- Проведена очистка данных (выбросы, дубликаты, некорректные значения)
- Восстановлены координаты и названия станций
- Рассчитаны доходы с каждой поездки
- Оценены расходы (аренда станций, сотрудники)
- Рассчитана прибыль и юнит-экономика по типам велосипедов
- Построен интерактивный дашборд


```plaintext
├── data/                # итоговые данные и расчёты
├── notebooks/           # пайплайн анализа
├── download.py          # скрипт для скачивания данных
├── divvy_analysis_presentation.pdf  # презентация
```

## Как воспроизвести

1. Скачать данные:
   python download.py

2. Запустить ноутбуки по порядку:
   01 → 02 → 03 → 04 → 05


## Дашборд

[Интерактивный дашборд с результатами анализа](https://datalens.yandex/090o6gpwsjhcm


## Презентация

Итоговая презентация проекта:
divvy_analysis_presentation.pdf


## Примечания

- Сырые данные не хранятся в репозитории и скачиваются через download.py
- Часть данных о тарифах и их ценах была восстановлена по открытым источникам и оценкам


## 🌍 English Summary

This repository contains a final project completed as part of the Yandex Data Science specialization.

The project analyzes Divvy bike-sharing data: from raw data collection and cleaning to revenue estimation, cost modeling, and unit economics analysis. The pipeline is split into several notebooks, each responsible for a specific stage of the analysis.
