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
| death_new_percent | INSERT в cube.basic_metrics, выражение `if(sl_dn_cnt = 0, 0, round(death_new * 100 / sl_dn_cnt))` |
| all_tasks | INSERT в cube.basic_metrics, выражение `uniqExactIf(task_id, task_status_cd != 5)` |
| main_execution | INSERT в cube.basic_metrics, выражение `uniqExactIf(task_id, task_status_cd IN (1, 2))` |
| main_execution_percent | INSERT в cube.basic_metrics, выражение `if(all_tasks = 0, 0, round(main_execution * 100 / all_tasks))` |

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

SELECT
    countIf(sl_dn_cnt = 0 AND death_new_percent != 0) AS wrong_death_percent,
    countIf(all_tasks = 0 AND main_execution_percent != 0) AS wrong_execution_percent
FROM cube.basic_metrics;


