# Сборка кубов ClickHouse

## 1. Общая схема слоёв

- raw — сырой слой из Kafka
- stage — нормализованные источники
- tmp — временные таблицы сборки
- precalc — предрасчётные таблицы
- cube — финальные кубы

## 2. Куб cube.basic_metrics

### 2.1 Источники

- stage.disp_info
- stage.dispans_person
- stage.dispensary_plan
- stage.event_task

### 2.2 Таблицы сборки

| Таблица | Назначение | БД |
|---|---|---|
| tmp.for_basic_metrics | широкая таблица-источник | tmp |
| tmp.dim_basic_filters | набор фильтров | tmp |
| cube.basic_metrics | финальный куб | cube |

### 2.3 Порядок сборки

1. Пересоздать `tmp.for_basic_metrics`
2. Пересоздать `tmp.dim_basic_filters`
3. Очистить `cube.basic_metrics`
4. Рассчитать `cube.basic_metrics`

### 2.4 Где менять логику метрик

| Метрика | Где менять |
|---|---|
| sl_dn_cnt | INSERT в cube.basic_metrics, выражение `uniqExact(disp_id)` |
| zl_on_dn_total | INSERT в cube.basic_metrics, выражение `uniqExactIf(mpioip, out_dt IS NULL)` |
| death_new | INSERT в cube.basic_metrics, выражение `uniqExactIf(mpioip, death_dt IS NOT NULL)` |
| all_tasks | INSERT в cube.basic_metrics, выражение `uniqExactIf(task_id, task_status_cd != 5)` |
| main_execution | INSERT в cube.basic_metrics, выражение `uniqExactIf(task_id, task_status_cd IN (1, 2))` |

---

## 3. Куб cube.risk

### 3.1 Источники

- stage.risk
- stage.dispans_person
- tmp.for_basic_metrics

### 3.2 Таблицы сборки

| Таблица | Назначение | БД |
|---|---|---|
| tmp.calc_periods | периоды расчёта | tmp |
| tmp.risk_period_source | последний риск по ЗЛ, риску и периоду | tmp |
| tmp.risk_precalc | риск + пол/возраст + фильтры | tmp |
| tmp.dim_risk_filters | набор фильтров риска | tmp |
| cube.risk_final | финальный куб риска | cube |

### 3.3 Порядок сборки

1. Пересоздать `tmp.calc_periods`
2. Пересоздать `tmp.risk_period_source`
3. Пересоздать `tmp.risk_precalc`
4. Пересоздать `tmp.dim_risk_filters`
5. Очистить `cube.risk_final`
6. Рассчитать `cube.risk_final`

### 3.4 Где менять логику расчётов

| Блок | Где менять |
|---|---|
| выбор последнего риска | INSERT в `tmp.risk_period_source`, `argMax(risk_level_id, tuple(risk_dt, risk_level_id))` |
| возраст | INSERT в `tmp.risk_precalc`, выражение `dateDiff('year', birth_dt, period_end)` |
| фильтры риска | INSERT в `tmp.dim_risk_filters`, блок `bitTest(mask, ...)` |
| половозрастные группы | INSERT в `cube.risk_final`, блоки `countIf(pol_cd = ... AND age_on_period_end ...)` |
| уровни риска low/average/high | INSERT в `cube.risk_final`, блоки `risk_level_id < 3`, `= 3`, `> 3` |
| проценты | INSERT в `cube.risk_final`, блоки `*_percent` |

---

## 4. Правила изменения логики

Любое изменение логики должно фиксироваться в двух местах:

1. SQL-блок расчёта
2. Раздел `Где менять логику расчётов`

Перед изменением нужно проверить:
- не меняется ли зерно таблицы;
- не появляется ли размножение строк;
- не нужно ли пересоздавать фильтры;
- не меняется ли структура финального куба.

---

## 5. Проверки после сборки

### basic_metrics

```sql
SELECT count() FROM cube.basic_metrics;

## 3. Куб `cube.done` (Выполнение мероприятий по месяцам)

### 3.1 Источники данных

| Таблица | Назначение |
|---------|-----------|
| `tmp.for_basic_metrics` | Широкая таблица, содержащая задачи ДН (`task_id`, `task_status_cd`, `closed_dt`, `smo_cd`, `profile_cd`, `group_cd`, `attach_mo_cd`). |
| `stage.event_task` | Альтернативный источник, но в данном ETL используется `tmp.for_basic_metrics`. |
| `stage.dispans_person` | (Не используется в текущей логике, но может быть задействована при расширении.) |

### 3.2 Таблицы сборки

| Таблица | Назначение | БД |
|---------|-----------|-----|
| `tmp.done_task_month_source` | Источник задач, закрытых в отчётном периоде, с извлечёнными годом, кварталом, месяцем закрытия и вычисленными фильтрами (регион, профиль, группа, СМО, МО). | tmp |
| `cube.done` | Финальный куб, содержащий для каждой комбинации фильтров и года/квартала 48 полей: выполненные (статус 1) и условно выполненные (статус 2) мероприятия по месяцам прошлого и текущего года. | cube |

### 3.3 Порядок сборки

1. **Пересоздать `tmp.done_task_month_source`**  
   – Заполнить данными из `tmp.for_basic_metrics`.  
   – Отобрать задачи с `task_status_cd IN (1,2)` и непустым `closed_dt`.  
   – Вычислить `closed_y` (год закрытия), `closed_q` (квартал), `closed_m` (месяц).  
   – Вычислить фильтры:  
     * `region_smo_cd` – первые 2 цифры `smo_cd` (или `'0'` если нет)  
     * `profile_cd` – преобразовать в строку (NULL → `'0'`)  
     * `dn_group_cd` – преобразовать в строку (NULL → `'0'`)  
     * `smo_cd` – значение или `'0'`  
     * `mo_cd` – значение `attach_mo_cd` или `'0'`  
   – Ограничить диапазон лет (например, 2018–2027).

2. **Очистить `cube.done`** (выполнить `TRUNCATE`).

3. **Рассчитать `cube.done`**  
   – В подзапросе `prepared` выполнить `CROSS JOIN` с двумя массивами:  
     * `metric_kind IN ('p', 'c')` – разделение на прошлый (p) и текущий (c) год.  
     * `mask` от 0 до 31 (5 бит) – для генерации всех комбинаций замены фильтров на `'0'` (означает «все значения»).  
   – Для каждой строки из `tmp.done_task_month_source` создаются 2 (metric_kind) × 32 (mask) = 64 копии.  
   – Вычислить `f_y` для метрики: для `'p'` используется `closed_y + 1` (прошлый год относительно года закрытия задачи), для `'c'` – `closed_y`.  
   – Применить битовую маску к фильтрам (`if(bitTest(mask, ...), '0', original)`).  
   – Сгруппировать по `f_region_smo_cd`, `f_profile_cd`, `f_dn_group_cd`, `f_y`, `f_q`, `f_smo`, `f_mo`.  
   – Для каждого месяца (1–12) посчитать 4 метрики:  
     * `p_done_mN_v` – выполненные в прошлом году (`metric_kind='p'`, статус 1, месяц N).  
     * `p_assumpt_mN_v` – условно выполненные в прошлом году (статус 2).  
     * `c_done_mN_v` – выполненные в текущем году.  
     * `c_assumpt_mN_v` – условно выполненные в текущем году.  
   – Добавить служебные поля `insert_dttm`, `update_dttm`.  
   – Исключить строки, где все 48 полей равны 0 (HAVING сумма = 0).

### 3.4 Где менять логику расчётов

| Блок | Где менять | Описание |
|------|-----------|----------|
| **Источник задач** | `INSERT INTO tmp.done_task_month_source` | Изменение фильтрации задач (статусы, период отбора). |
| **Вычисление `closed_y`, `closed_q`, `closed_m`** | `INSERT INTO tmp.done_task_month_source` | Смещение года или квартала. |
| **Фильтры (регион, профиль, группа, СМО, МО)** | `INSERT INTO tmp.done_task_month_source` | Логика получения кода региона, обработка NULL/0. |
| **Генерация `metric_kind`** | `CROSS JOIN (SELECT arrayJoin(['p','c']) AS metric_kind)` | Добавление новых видов сравнения. |
| **Битовая маска** | `CROSS JOIN (SELECT arrayJoin(range(32)) AS mask)` | Добавление новых фильтров (увеличение числа масок). |
| **Вычисление `f_y` для `metric_kind='p'`** | `if(metric_kind='p', closed_y+1, closed_y)` | Изменение сдвига года для прошлого периода. |
| **Агрегатные выражения для месяцев** | `uniqExactIf(task_id, ...)` в основном SELECT | Изменение статусов или добавление новых метрик. |
| **Итоговое HAVING** | `HAVING sum(...) <> 0` | Изменение условия отсеивания пустых строк. |

### 3.5 Правила изменения логики

- Любое изменение логики должно фиксироваться в двух местах:
  1. Сам SQL-блок в ETL-скрипте.
  2. Раздел «Где менять логику расчётов» данной документации.

- Перед изменением необходимо проверить:
  - Не меняется ли зерно таблицы (уникальность строк по комбинации фильтров, года, квартала).
  - Не появляется ли размножение строк из‑за `CROSS JOIN` (допустимо, но должно быть контролируемо).
  - Нужно ли пересоздавать временную таблицу `tmp.done_task_month_source`.
  - Не меняется ли структура финального куба (добавление/удаление полей).

### 3.6 Проверки после сборки

```sql
-- Общее количество записей в кубе
SELECT count() FROM cube.done;

-- Проверка для конкретного года и квартала
SELECT * FROM cube.done WHERE f_y = 2025 AND f_q = 1 LIMIT 10;

-- Проверка наличия данных по регионам
SELECT f_region_smo_cd, count() FROM cube.done GROUP BY f_region_smo_cd;

-- Проверка, что нет нулевых метрик там, где должны быть данные
SELECT * FROM cube.done WHERE p_done_m1_v = 0 AND p_assumpt_m1_v = 0 AND c_done_m1_v = 0 AND c_assumpt_m1_v = 0 LIMIT 10;

Правила изменения логики (общие)
Любое изменение логики должно фиксироваться в двух местах:

SQL-блок расчёта

Раздел «Где менять логику расчётов»
Перед изменением нужно проверить:
не меняется ли зерно таблицы;
не появляется ли размножение строк;
не нужно ли пересоздавать фильтры;
не меняется ли структура финального куба.
