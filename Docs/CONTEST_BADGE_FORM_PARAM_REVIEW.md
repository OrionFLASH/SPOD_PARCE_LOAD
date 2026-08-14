# Каталог параметров формы BADGE

> **Удобный ввод:** откройте [`param_review_editor/index.html`](param_review_editor/index.html)  
> (перед этим: `python src/Tools/build_param_review_editor.py`).  
> Правки → экспорт **JSON** → в чат: «примени каталог».

Ниже — снимок/архив в Markdown. Рабочий цикл — через HTML-редактор.

---

Рабочая таблица правок. После правок — **«примени каталог»** → `field_meta` + BLANK Excel.

## Как заполнять

| Колонка | Что править |
|---------|-------------|
| **Ст** | `[ ]` · `[v]` · `[w]` |
| **Подпись** / **Описание** | Широкие колонки (18% + 30%) |
| **Тип** | `dropdown` / `text` / `list` / `json` / `date` |
| **Варианты** | Список через запятую; `—` если нет |
| **Дефолт** | Предзаполнение BLANK; `—` = пусто |
| **Пусто** | `да` / `нет` |
| **JSON** | `CONTEST_FEATURE` / `REWARD_ADD_DATA` / `ячейка JSON` / `—` |
| **Заметка** | Комментарий |

Смотрите в **превью** Markdown. Файл в `.prettierignore`.

---

## CONTEST

Плоские поля конкурса + массивы.

<table>
<colgroup>
  <col width="3%" />
  <col width="4%" />
  <col width="11%" />
  <col width="18%" />
  <col width="30%" />
  <col width="6%" />
  <col width="12%" />
  <col width="6%" />
  <col width="4%" />
  <col width="4%" />
  <col width="2%" />
</colgroup>
<thead>
<tr>
  <th>#</th>
  <th>Ст</th>
  <th>Ключ</th>
  <th>Подпись</th>
  <th>Описание</th>
  <th>Тип</th>
  <th>Варианты</th>
  <th>Дефолт</th>
  <th>Пусто</th>
  <th>JSON</th>
  <th>Заметка</th>
</tr>
</thead>
<tbody>

<tr>
  <td>1</td>
  <td><code>[v]</code></td>
  <td><code>CONTEST_CODE</code></td>
  <td>Код конкурса</td>
  <td>Уникальный код конкурса (ключ связей). Пример: 01_2025-0_11-1_1</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>нет</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>2</td>
  <td><code>[w]</code></td>
  <td><code>FULL_NAME</code></td>
  <td>Название конкурса</td>
  <td>Отображаемое название конкурса/турнира (на странице Турниры/Детальная карточка турнира).</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>нет</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>3</td>
  <td><code>[w]</code></td>
  <td><code>CREATE_DT</code></td>
  <td>Дата создания конкурса</td>
  <td>Дата начала YYYY-MM-DD. Почти всегда начало года</td>
  <td>date</td>
  <td>—</td>
  <td>—</td>
  <td>нет</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>4</td>
  <td><code>[v]</code></td>
  <td><code>CLOSE_DT</code></td>
  <td>Дата закрытия</td>
  <td>Дата окончания YYYY-MM-DD; 4000-01-01 = без срока.</td>
  <td>date</td>
  <td>—</td>
  <td>4000-01-01</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>5</td>
  <td><code>[w]</code></td>
  <td><code>BUSINESS_STATUS</code></td>
  <td>Бизнес-статус</td>
  <td>Статус: АКТИВНЫЙ  &#124;  АРХИВНЫЙ. (Всегда ставим АКТИВНЫЙ)</td>
  <td>dropdown</td>
  <td>АКТИВНЫЙ, АРХИВНЫЙ</td>
  <td>АКТИВНЫЙ</td>
  <td>нет</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>6</td>
  <td><code>[w]</code></td>
  <td><code>CONTEST_TYPE</code></td>
  <td>Тип конкурса</td>
  <td>ТУРНИРНЫЙ (соревнование "будь лучше других") (разыгрываем от 1 до 3 сезонных наград Золото Серебро Бронза)  &#124;  ИНДИВИДУАЛЬНЫЙ  &#124;  ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ (режим "достигни результат", получи одну награду).</td>
  <td>dropdown</td>
  <td>ТУРНИРНЫЙ, ИНДИВИДУАЛЬНЫЙ, ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ</td>
  <td>ТУРНИРНЫЙ</td>
  <td>нет</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>7</td>
  <td><code>[w]</code></td>
  <td><code>CONTEST_DESCRIPTION</code></td>
  <td>Описание турнира</td>
  <td>Текст описания для конкурса/турнира (на странице Детальная карточка турнира показываем).</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>8</td>
  <td><code>[w]</code></td>
  <td><code>SHOW_INDICATOR</code></td>
  <td>Отображаемое название единиц показателя</td>
  <td>Единица/подпись индикатора: шт.  &#124;  Факт  &#124;  %  &#124;  … на списке показателей подпись к единицам данных</td>
  <td>dropdown</td>
  <td>%, %%, пт., шт., Факт, балл, Темп %, Ранг %%, ФЛ, шт., клиенты, Ср. балл, млн руб., К-во, шт., категория, тыс. руб., Анкет, шт., Сумма, руб., Пакеты услуг, Договора, шт., Сумма УС, шт., Процент (х100), Факт, млн руб., Сумма, млн руб., сборы, млн руб., Сумма, тыс. руб., Интегральный ранг, Прирост, млн руб., Прирост, тыс. руб., Комиссия, тыс. руб., Прирост ОСЗ, млн руб., нетто-притоки, млн руб.</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>9</td>
  <td><code>[w]</code></td>
  <td><code>PRODUCT_GROUP</code></td>
  <td>Группа продукта</td>
  <td>Группа продукта (общее направление)</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>10</td>
  <td><code>[w]</code></td>
  <td><code>PRODUCT</code></td>
  <td>Продукт</td>
  <td>Продукт / тематика конкурса.</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>11</td>
  <td><code>[w]</code></td>
  <td><code>CONTEST_SUBJECT</code></td>
  <td>Кто соревнуется</td>
  <td>Кто участник конкурса. Обычно: EMPLOYEE (сотрудники).</td>
  <td>dropdown</td>
  <td>EMPLOYEE</td>
  <td>EMPLOYEE</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>12</td>
  <td><code>[w]</code></td>
  <td><code>FACTOR_MARK_TYPE</code></td>
  <td>Принцип отбора победителей</td>
  <td>CRITERION  &#124;  RATING_MAX  &#124;  RATING_MIN. (способ выбора победителей: достиг показателя, сделал больше других или меньше других — меньше, например, для ранга)</td>
  <td>dropdown</td>
  <td>CRITERION, RATING_MAX, RATING_MIN</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>13</td>
  <td><code>[w]</code></td>
  <td><code>CONTEST_INDICATOR_METHOD</code></td>
  <td>Метод индикатора</td>
  <td>INTEGRAL  &#124;  RELATION. Метод расчета показателя (фактический / расчетный)</td>
  <td>dropdown</td>
  <td>INTEGRAL, RELATION</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>14</td>
  <td><code>[w]</code></td>
  <td><code>CONTEST_FACTOR_METHOD</code></td>
  <td>Метод расчета показателя</td>
  <td>FACT  &#124;  FACT0-FACT1  &#124;  FACT0-RUN_RATE1_DOWN  &#124;  RUN_RATE. (для автоматических турниров способ расчета на данных источников)</td>
  <td>dropdown</td>
  <td>FACT, FACT0-FACT1, FACT0-RUN_RATE1_DOWN, RUN_RATE</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>15</td>
  <td><code>[w]</code></td>
  <td><code>PLAN_METHOD_CODE</code></td>
  <td>Как вычисляется план</td>
  <td>DEPENDS_PREVIOUS_PERIOD  &#124;  PRESET_VALUE. (Метод расчета планового показателя: из прошлого периода / фиксированное значение)</td>
  <td>dropdown</td>
  <td>DEPENDS_PREVIOUS_PERIOD, PRESET_VALUE</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>16</td>
  <td><code>[w]</code></td>
  <td><code>PLAN_MOD_METOD</code></td>
  <td>Метод модификации плана</td>
  <td>Модификатор плана. Обычно: MULTIPLIER.</td>
  <td>dropdown</td>
  <td>MULTIPLIER</td>
  <td>MULTIPLIER</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>17</td>
  <td><code>[w]</code></td>
  <td><code>PLAN_MOD_VALUE</code></td>
  <td>Значение плана</td>
  <td>Значение планового показателя (0, 1, 1000, …).</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>18</td>
  <td><code>[w]</code></td>
  <td><code>FACTOR_MATCH</code></td>
  <td>Символ сравнения с планом</td>
  <td>Сравнение фактора: =  &#124;  &gt;  &#124;  &gt;=  &#124;  &lt;  &#124;  &lt;=.</td>
  <td>dropdown</td>
  <td>=, &gt;, &gt;=, &lt;, &lt;=</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>19</td>
  <td><code>[w]</code></td>
  <td><code>TARGET_TYPE</code></td>
  <td>Среда конкурса</td>
  <td>Среда конкурса: ПРОМ  &#124;  ТЕСТ.</td>
  <td>dropdown</td>
  <td>ПРОМ, ТЕСТ</td>
  <td>ПРОМ</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>20</td>
  <td><code>[v]</code></td>
  <td><code>SOURCE_UPD_FREQUENCY</code></td>
  <td>Частота обновления источника</td>
  <td>Частота обновления источника: 1  &#124;  7  &#124;  10 (дни). (не используется)</td>
  <td>dropdown</td>
  <td>1, 7, 10</td>
  <td>1</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>21</td>
  <td><code>[v]</code></td>
  <td><code>CALC_TYPE</code></td>
  <td>Тип расчёта</td>
  <td>Тип расчёта: 0  &#124;  1. (не используется) 0 — промышленный расчет / 1 — ручной расчет</td>
  <td>dropdown</td>
  <td>0, 1</td>
  <td>0</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>22</td>
  <td><code>[v]</code></td>
  <td><code>FACT_POST_PROCESSING</code></td>
  <td>Постобработка факта</td>
  <td>Постобработка факта (код/флаг; часто пусто). Правило постобработки показателя конкурса. PERCENTILE — вычисление перцентиля от фактического показателя конкурса</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>23</td>
  <td><code>[w]</code></td>
  <td><code>BUSINESS_BLOCK</code></td>
  <td>Бизнес-блок (через ;)</td>
  <td>Бизнес-блок(и) через ; . Примеры: KMMMB, KMKKSB, CSM, AKMKKSB.</td>
  <td>dropdown</td>
  <td>KMMMB, KMKKSB, CSM, AKMKKSB</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>24</td>
  <td><code>[ ]</code></td>
  <td><code>CONTEST_PERIOD</code></td>
  <td>Периоды расчета конкурса (через ;)</td>
  <td>Периоды через ; или пусто → []. Обычно пусто.</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

</tbody>
</table>


## FEATURE

Листья → JSON `CONTEST_FEATURE`.

<table>
<colgroup>
  <col width="3%" />
  <col width="4%" />
  <col width="11%" />
  <col width="18%" />
  <col width="30%" />
  <col width="6%" />
  <col width="12%" />
  <col width="6%" />
  <col width="4%" />
  <col width="4%" />
  <col width="2%" />
</colgroup>
<thead>
<tr>
  <th>#</th>
  <th>Ст</th>
  <th>Ключ</th>
  <th>Подпись</th>
  <th>Описание</th>
  <th>Тип</th>
  <th>Варианты</th>
  <th>Дефолт</th>
  <th>Пусто</th>
  <th>JSON</th>
  <th>Заметка</th>
</tr>
</thead>
<tbody>

<tr>
  <td>25</td>
  <td><code>[w]</code></td>
  <td><code>FEATURE.vid</code></td>
  <td>FEATURE.Среда конкурса</td>
  <td>Среда конкурса: ПРОМ  &#124;  ТЕСТ (как TARGET_TYPE).</td>
  <td>dropdown</td>
  <td>ПРОМ, ТЕСТ</td>
  <td>ПРОМ</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td>ТЕСТ (как TARGET_TYPE).</td>
</tr>

<tr>
  <td>26</td>
  <td><code>[w]</code></td>
  <td><code>FEATURE.accuracy</code></td>
  <td>FEATURE.Округление до...</td>
  <td>Точность/разрядность: 0  &#124;  1  &#124;  2 . (число знаков после запятой для отображения)</td>
  <td>dropdown</td>
  <td>0, 1, 2</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>27</td>
  <td><code>[w]</code></td>
  <td><code>FEATURE.capacity</code></td>
  <td>FEATURE.Приведение к млн / тыс.</td>
  <td>Масштаб: пусто  &#124;  MILLIONS  &#124;  THOUSANDS. (приведение отображаемого показателя к млн, к тыс.)</td>
  <td>dropdown</td>
  <td>MILLIONS, THOUSANDS</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>28</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.masking</code></td>
  <td>FEATURE.masking</td>
  <td>Маскирование: Y  &#124;  N (часто N).</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>N</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td>N (часто N).</td>
</tr>

<tr>
  <td>29</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.minNumber</code></td>
  <td>FEATURE.minNumber</td>
  <td>Мин. число участников чтобы считать победителей (исключаем соревнование сам с собой): 1  &#124;  2  &#124;  3.</td>
  <td>dropdown</td>
  <td>1, 2, 3</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>30</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.momentRewarding</code></td>
  <td>FEATURE.momentRewarding</td>
  <td>Момент награждения: AFTER  &#124;  DURIN (после закрытия турнира / во время турнира)</td>
  <td>dropdown</td>
  <td>AFTER, DURIN</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td>DURIN (после закрытия турнира / во время турнира)</td>
</tr>

<tr>
  <td>31</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.typeRewarding</code></td>
  <td>FEATURE.typeRewarding</td>
  <td>Вручаем одну из 3 наград или все (one  &#124;  all).</td>
  <td>dropdown</td>
  <td>one, all</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td>all).</td>
</tr>

<tr>
  <td>32</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.avatarShow</code></td>
  <td>FEATURE.avatarShow</td>
  <td>Показ аватара: Y  &#124;  N.</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>Y</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td>N.</td>
</tr>

<tr>
  <td>33</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.tournamentTeam</code></td>
  <td>FEATURE.tournamentTeam</td>
  <td>Командный турнир: Y  &#124;  N.</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>N</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td>N.</td>
</tr>

<tr>
  <td>34</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.persomanNumberVisible</code></td>
  <td>FEATURE.persomanNumberVisible (через ;)</td>
  <td>Если указаны табельные, то только эти сотрудники увидят турнир</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>35</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.persomanNumberHidden</code></td>
  <td>FEATURE.persomanNumberHidden (через ;)</td>
  <td>Если указаны табельные, то эти сотрудники НЕ увидят турнир</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>36</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.tournamentStartMailing</code></td>
  <td>FEATURE.tournamentStartMailing</td>
  <td>Рассылка старта: Y  &#124;  N.</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td>N.</td>
</tr>

<tr>
  <td>37</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.tournamentEndMailing</code></td>
  <td>FEATURE.tournamentEndMailing</td>
  <td>Рассылка финиша: Y  &#124;  N.</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td>N.</td>
</tr>

<tr>
  <td>38</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.tournamentLikeMailing</code></td>
  <td>FEATURE.tournamentLikeMailing</td>
  <td>Рассылка лайков: Y  &#124;  N.</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td>N.</td>
</tr>

<tr>
  <td>39</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.tournamentListMailing</code></td>
  <td>FEATURE.tournamentListMailing (через ;)</td>
  <td>Список рассылки через ; .</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>40</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.tournamentRewardingMailing</code></td>
  <td>FEATURE.tournamentRewardingMailing</td>
  <td>Рассылка награждения: Y  &#124;  N.</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td>N.</td>
</tr>

<tr>
  <td>41</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.feature</code></td>
  <td>FEATURE.feature (через ;)</td>
  <td>Тексты особенностей турнира. Показываем в детальной карточке турнира</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>42</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.businessBlock</code></td>
  <td>FEATURE.businessBlock (через ;)</td>
  <td>Блоки в FEATURE через ; (как BUSINESS_BLOCK).</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>43</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.helpCodeList</code></td>
  <td>FEATURE.helpCodeList (через ;)</td>
  <td>Коды для вывода окна с доп описанием конкурса</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>44</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.preferences</code></td>
  <td>FEATURE.preferences (через ;)</td>
  <td>Преференции за получение награды если предусмотрены</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>45</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.tbVisible</code></td>
  <td>FEATURE.tbVisible (через ;)</td>
  <td>Коды ТБ видимые через ; .</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>46</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.tbHidden</code></td>
  <td>FEATURE.tbHidden (через ;)</td>
  <td>Коды ТБ скрытые через ; .</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>47</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.gosbVisible</code></td>
  <td>FEATURE.gosbVisible (через ;)</td>
  <td>Коды ГОСБ видимые через ; .</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

<tr>
  <td>48</td>
  <td><code>[ ]</code></td>
  <td><code>FEATURE.gosbHidden</code></td>
  <td>FEATURE.gosbHidden (через ;)</td>
  <td>Коды ГОСБ скрытые через ; .</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>CONTEST_FEATURE</td>
  <td></td>
</tr>

</tbody>
</table>


## BADGE

Плоские поля слота награды.

<table>
<colgroup>
  <col width="3%" />
  <col width="4%" />
  <col width="11%" />
  <col width="18%" />
  <col width="30%" />
  <col width="6%" />
  <col width="12%" />
  <col width="6%" />
  <col width="4%" />
  <col width="4%" />
  <col width="2%" />
</colgroup>
<thead>
<tr>
  <th>#</th>
  <th>Ст</th>
  <th>Ключ</th>
  <th>Подпись</th>
  <th>Описание</th>
  <th>Тип</th>
  <th>Варианты</th>
  <th>Дефолт</th>
  <th>Пусто</th>
  <th>JSON</th>
  <th>Заметка</th>
</tr>
</thead>
<tbody>

<tr>
  <td>49</td>
  <td><code>[ ]</code></td>
  <td><code>REWARD_CODE</code></td>
  <td>Код награды</td>
  <td>Уникальный код награды, напр. r_01_2025-0_11-1_1_1.</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>нет</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>50</td>
  <td><code>[ ]</code></td>
  <td><code>REWARD_TYPE</code></td>
  <td>Тип награды (BADGE)</td>
  <td>Для этой формы всегда BADGE.</td>
  <td>dropdown</td>
  <td>BADGE</td>
  <td>BADGE</td>
  <td>нет</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>51</td>
  <td><code>[ ]</code></td>
  <td><code>FULL_NAME</code></td>
  <td>Название награды</td>
  <td>Краткое название бейджа</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>нет</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>52</td>
  <td><code>[ ]</code></td>
  <td><code>REWARD_DESCRIPTION</code></td>
  <td>Описание награды</td>
  <td>Полное описание награды.</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>53</td>
  <td><code>[ ]</code></td>
  <td><code>REWARD_CONDITION</code></td>
  <td>Условие награды</td>
  <td>Класс/код условия начисления (часто пусто или код).</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>54</td>
  <td><code>[ ]</code></td>
  <td><code>REWARD_COST</code></td>
  <td>Стоимость</td>
  <td>Стоимость в кристаллах (часто 0…14).</td>
  <td>dropdown</td>
  <td>0, 2, 3, 4, 5, 6, 7, 8, 10, 14</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

</tbody>
</table>


## ADD

Листья → JSON `REWARD_ADD_DATA`.

<table>
<colgroup>
  <col width="3%" />
  <col width="4%" />
  <col width="11%" />
  <col width="18%" />
  <col width="30%" />
  <col width="6%" />
  <col width="12%" />
  <col width="6%" />
  <col width="4%" />
  <col width="4%" />
  <col width="2%" />
</colgroup>
<thead>
<tr>
  <th>#</th>
  <th>Ст</th>
  <th>Ключ</th>
  <th>Подпись</th>
  <th>Описание</th>
  <th>Тип</th>
  <th>Варианты</th>
  <th>Дефолт</th>
  <th>Пусто</th>
  <th>JSON</th>
  <th>Заметка</th>
</tr>
</thead>
<tbody>

<tr>
  <td>55</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.nftFlg</code></td>
  <td>ADD.nftFlg</td>
  <td>NFT-флаг: Y  &#124;  N (обычно N).</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>N</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td>N (обычно N).</td>
</tr>

<tr>
  <td>56</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.outstanding</code></td>
  <td>ADD.outstanding</td>
  <td>Выдающийся: Y  &#124;  N.</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>N</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td>N.</td>
</tr>

<tr>
  <td>57</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.rewardRule</code></td>
  <td>ADD.rewardRule</td>
  <td>Текст правила получения бейджа.</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>58</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.rewardAgainGlobal</code></td>
  <td>ADD.rewardAgainGlobal</td>
  <td>Повтор глобально: Y  &#124;  N.</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>N</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td>N.</td>
</tr>

<tr>
  <td>59</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.rewardAgainTournament</code></td>
  <td>ADD.rewardAgainTournament</td>
  <td>Повтор в турнире: Y  &#124;  N (часто N).</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>N</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td>N (часто N).</td>
</tr>

<tr>
  <td>60</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.hidden</code></td>
  <td>ADD.hidden</td>
  <td>Скрыт: Y  &#124;  N.</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>N</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td>N.</td>
</tr>

<tr>
  <td>61</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.fileName</code></td>
  <td>ADD.fileName</td>
  <td>Имя файла арта/иконки (код); часто пусто.</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>62</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.teamNews</code></td>
  <td>ADD.teamNews</td>
  <td>Текст командной новости (шаблон с [Имя] и т.п.).</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>63</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.singleNews</code></td>
  <td>ADD.singleNews</td>
  <td>Текст индивидуальной новости.</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>64</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.masterBadge</code></td>
  <td>ADD.masterBadge</td>
  <td>Мастер-бейдж: Y  &#124;  N. (Y — для награды / N — для турнира)</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>N</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td>N. (Y — для награды / N — для турнира)</td>
</tr>

<tr>
  <td>65</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.parentRewardCode</code></td>
  <td>ADD.parentRewardCode</td>
  <td>Код родительской награды (если есть).</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>66</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.priority</code></td>
  <td>ADD.priority</td>
  <td>Приоритет слота: 1  &#124;  2  &#124;  3.</td>
  <td>dropdown</td>
  <td>1, 2, 3</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>67</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.recommendationLevel</code></td>
  <td>ADD.recommendationLevel</td>
  <td>Уровень: BANK  &#124;  TB  &#124;  GOSB  &#124;  NON.</td>
  <td>dropdown</td>
  <td>BANK, TB, GOSB, NON</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>68</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.refreshOldNews</code></td>
  <td>ADD.refreshOldNews</td>
  <td>Обновлять старые новости: Y  &#124;  N.</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>N</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td>N.</td>
</tr>

<tr>
  <td>69</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.tournamentTeam</code></td>
  <td>ADD.tournamentTeam</td>
  <td>Командный режим награды: Y  &#124;  N.</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>N</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td>N.</td>
</tr>

<tr>
  <td>70</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.seasonItem</code></td>
  <td>ADD.seasonItem</td>
  <td>Код сезонного ITEM (если связан).</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>71</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.newsType</code></td>
  <td>ADD.newsType</td>
  <td>Тип новости: AIPROMPT  &#124;  TEMPLATE. (генерит ИИ / по шаблону)</td>
  <td>dropdown</td>
  <td>AIPROMPT, TEMPLATE</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td>TEMPLATE. (генерит ИИ / по шаблону)</td>
</tr>

<tr>
  <td>72</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.winCriterion</code></td>
  <td>ADD.winCriterion</td>
  <td>Текст критерия победы для ИИ создания новости</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>73</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.preferences</code></td>
  <td>ADD.preferences</td>
  <td>Преференции если предусмотрены за награду</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>74</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.feature</code></td>
  <td>ADD.feature (через ;)</td>
  <td>Особенности награды через ; . (показываем в Награде)</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>75</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.businessBlock</code></td>
  <td>ADD.businessBlock (через ;)</td>
  <td>Блоки награды через ; .</td>
  <td>list</td>
  <td>KMMMB, KMKKSB, CSM, AKMKKSB</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>76</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.helpCodeList</code></td>
  <td>ADD.helpCodeList (через ;)</td>
  <td>Коды help через ; .</td>
  <td>list</td>
  <td>через ;</td>
  <td>—</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td></td>
</tr>

<tr>
  <td>77</td>
  <td><code>[ ]</code></td>
  <td><code>ADD.hiddenRewardList</code></td>
  <td>ADD.hiddenRewardList</td>
  <td>Скрыт в списке наград: Y  &#124;  N.</td>
  <td>dropdown</td>
  <td>Y, N</td>
  <td>N</td>
  <td>да</td>
  <td>REWARD_ADD_DATA</td>
  <td>N.</td>
</tr>

</tbody>
</table>


## TABLE:REWARD-LINK

Колонки таблицы `REWARD-LINK`.

<table>
<colgroup>
  <col width="3%" />
  <col width="4%" />
  <col width="11%" />
  <col width="18%" />
  <col width="30%" />
  <col width="6%" />
  <col width="12%" />
  <col width="6%" />
  <col width="4%" />
  <col width="4%" />
  <col width="2%" />
</colgroup>
<thead>
<tr>
  <th>#</th>
  <th>Ст</th>
  <th>Ключ</th>
  <th>Подпись</th>
  <th>Описание</th>
  <th>Тип</th>
  <th>Варианты</th>
  <th>Дефолт</th>
  <th>Пусто</th>
  <th>JSON</th>
  <th>Заметка</th>
</tr>
</thead>
<tbody>

<tr>
  <td>78</td>
  <td><code>[ ]</code></td>
  <td><code>CONTEST_CODE</code></td>
  <td>CONTEST_CODE</td>
  <td>Код конкурса (= CONTEST_CODE на листе)</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>79</td>
  <td><code>[ ]</code></td>
  <td><code>GROUP_CODE</code></td>
  <td>GROUP_CODE</td>
  <td>BANK  &#124;  TB  &#124;  GOSB  &#124;  GROUPING</td>
  <td>dropdown</td>
  <td>BANK, TB, GOSB, GROUPING</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>80</td>
  <td><code>[ ]</code></td>
  <td><code>REWARD_CODE</code></td>
  <td>REWARD_CODE</td>
  <td>Код BADGE из слота</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

</tbody>
</table>


## TABLE:GROUP

Колонки таблицы `GROUP`.

<table>
<colgroup>
  <col width="3%" />
  <col width="4%" />
  <col width="11%" />
  <col width="18%" />
  <col width="30%" />
  <col width="6%" />
  <col width="12%" />
  <col width="6%" />
  <col width="4%" />
  <col width="4%" />
  <col width="2%" />
</colgroup>
<thead>
<tr>
  <th>#</th>
  <th>Ст</th>
  <th>Ключ</th>
  <th>Подпись</th>
  <th>Описание</th>
  <th>Тип</th>
  <th>Варианты</th>
  <th>Дефолт</th>
  <th>Пусто</th>
  <th>JSON</th>
  <th>Заметка</th>
</tr>
</thead>
<tbody>

<tr>
  <td>81</td>
  <td><code>[ ]</code></td>
  <td><code>CONTEST_CODE</code></td>
  <td>CONTEST_CODE</td>
  <td>Код конкурса</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>82</td>
  <td><code>[ ]</code></td>
  <td><code>GROUP_CODE</code></td>
  <td>GROUP_CODE</td>
  <td>BANK  &#124;  TB  &#124;  GOSB  &#124;  GROUPING</td>
  <td>dropdown</td>
  <td>BANK, TB, GOSB, GROUPING</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>83</td>
  <td><code>[ ]</code></td>
  <td><code>GROUP_VALUE</code></td>
  <td>GROUP_VALUE</td>
  <td>* или [код] / JSON</td>
  <td>json</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>ячейка JSON</td>
  <td></td>
</tr>

<tr>
  <td>84</td>
  <td><code>[ ]</code></td>
  <td><code>GET_CALC_METHOD</code></td>
  <td>GET_CALC_METHOD</td>
  <td>1  &#124;  2  &#124;  3</td>
  <td>dropdown</td>
  <td>1, 2, 3</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>85</td>
  <td><code>[ ]</code></td>
  <td><code>GET_CALC_CRITERION</code></td>
  <td>GET_CALC_CRITERION</td>
  <td>Число/порог</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>86</td>
  <td><code>[ ]</code></td>
  <td><code>ADD_CALC_CRITERION</code></td>
  <td>ADD_CALC_CRITERION</td>
  <td>Число/порог</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>87</td>
  <td><code>[ ]</code></td>
  <td><code>ADD_CALC_CRITERION_2</code></td>
  <td>ADD_CALC_CRITERION_2</td>
  <td>Число/порог</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>88</td>
  <td><code>[ ]</code></td>
  <td><code>BASE_CALC_CODE</code></td>
  <td>BASE_CALC_CODE</td>
  <td>BANK  &#124;  TB  &#124;  GOSB  &#124;  GROUPING</td>
  <td>dropdown</td>
  <td>BANK, TB, GOSB, GROUPING</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

</tbody>
</table>


## TABLE:INDICATOR

Колонки таблицы `INDICATOR`.

<table>
<colgroup>
  <col width="3%" />
  <col width="4%" />
  <col width="11%" />
  <col width="18%" />
  <col width="30%" />
  <col width="6%" />
  <col width="12%" />
  <col width="6%" />
  <col width="4%" />
  <col width="4%" />
  <col width="2%" />
</colgroup>
<thead>
<tr>
  <th>#</th>
  <th>Ст</th>
  <th>Ключ</th>
  <th>Подпись</th>
  <th>Описание</th>
  <th>Тип</th>
  <th>Варианты</th>
  <th>Дефолт</th>
  <th>Пусто</th>
  <th>JSON</th>
  <th>Заметка</th>
</tr>
</thead>
<tbody>

<tr>
  <td>89</td>
  <td><code>[ ]</code></td>
  <td><code>CONTEST_CODE</code></td>
  <td>CONTEST_CODE</td>
  <td>Код конкурса</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>90</td>
  <td><code>[ ]</code></td>
  <td><code>INDICATOR_CALC_TYPE</code></td>
  <td>INDICATOR_CALC_TYPE</td>
  <td>Обычно 1</td>
  <td>dropdown</td>
  <td>1</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>91</td>
  <td><code>[ ]</code></td>
  <td><code>INDICATOR_ADD_CALC_TYPE</code></td>
  <td>INDICATOR_ADD_CALC_TYPE</td>
  <td>NUMERATOR  &#124;  DIVIDER</td>
  <td>dropdown</td>
  <td>NUMERATOR, DIVIDER</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td>DIVIDER</td>
</tr>

<tr>
  <td>92</td>
  <td><code>[ ]</code></td>
  <td><code>FULL_NAME</code></td>
  <td>FULL_NAME</td>
  <td>Имя индикатора</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>93</td>
  <td><code>[ ]</code></td>
  <td><code>INDICATOR_CODE</code></td>
  <td>INDICATOR_CODE</td>
  <td>Код (WAIT, RATING, …)</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>94</td>
  <td><code>[ ]</code></td>
  <td><code>INDICATOR_AGG_FUNCTION</code></td>
  <td>INDICATOR_AGG_FUNCTION</td>
  <td>SUM  &#124;  MAX  &#124;  COUNT_DISTINCT  &#124;  …</td>
  <td>dropdown</td>
  <td>SUM, MAX, COUNT_DISTINCT, COUNT_DISTINCT_CUSTOMER, COUNT_DISTINCT_DEAL</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>95</td>
  <td><code>[ ]</code></td>
  <td><code>INDICATOR_WEIGHT</code></td>
  <td>INDICATOR_WEIGHT</td>
  <td>1  &#124;  -1  &#124;  1000</td>
  <td>dropdown</td>
  <td>1, -1, 1000</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>96</td>
  <td><code>[ ]</code></td>
  <td><code>INDICATOR_OBJECT</code></td>
  <td>INDICATOR_OBJECT</td>
  <td>Часто пусто</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>97</td>
  <td><code>[ ]</code></td>
  <td><code>INDICATOR_MARK_TYPE</code></td>
  <td>INDICATOR_MARK_TYPE</td>
  <td>CRITERION  &#124;  GAIN  &#124;  RATING</td>
  <td>dropdown</td>
  <td>CRITERION, GAIN, RATING</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>98</td>
  <td><code>[ ]</code></td>
  <td><code>INDICATOR_MATCH</code></td>
  <td>INDICATOR_MATCH</td>
  <td>=  &#124;  &gt;=  &#124;  MAX  &#124;  MIN  &#124;  X2…</td>
  <td>dropdown</td>
  <td>=, &gt;=, MAX, MIN, X2, X3, X4</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>99</td>
  <td><code>[ ]</code></td>
  <td><code>INDICATOR_VALUE</code></td>
  <td>INDICATOR_VALUE</td>
  <td>Порог/константа</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>100</td>
  <td><code>[ ]</code></td>
  <td><code>CONTEST_CRITERION</code></td>
  <td>CONTEST_CRITERION</td>
  <td>Часто пусто</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>101</td>
  <td><code>[ ]</code></td>
  <td><code>INDICATOR_FILTER</code></td>
  <td>INDICATOR_FILTER</td>
  <td>SPOD-JSON фильтр или пусто</td>
  <td>json</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>ячейка JSON</td>
  <td></td>
</tr>

<tr>
  <td>102</td>
  <td><code>[ ]</code></td>
  <td><code>CONTESTANT_SELECTION</code></td>
  <td>CONTESTANT_SELECTION</td>
  <td>0  &#124;  1</td>
  <td>dropdown</td>
  <td>0, 1</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td>1</td>
</tr>

<tr>
  <td>103</td>
  <td><code>[ ]</code></td>
  <td><code>CALC_TYPE</code></td>
  <td>CALC_TYPE</td>
  <td>0  &#124;  1</td>
  <td>dropdown</td>
  <td>0, 1</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td>1</td>
</tr>

<tr>
  <td>104</td>
  <td><code>[ ]</code></td>
  <td><code>N</code></td>
  <td>N</td>
  <td>Параметр N</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

</tbody>
</table>


## TABLE:SCHEDULE

Колонки таблицы `SCHEDULE`.

<table>
<colgroup>
  <col width="3%" />
  <col width="4%" />
  <col width="11%" />
  <col width="18%" />
  <col width="30%" />
  <col width="6%" />
  <col width="12%" />
  <col width="6%" />
  <col width="4%" />
  <col width="4%" />
  <col width="2%" />
</colgroup>
<thead>
<tr>
  <th>#</th>
  <th>Ст</th>
  <th>Ключ</th>
  <th>Подпись</th>
  <th>Описание</th>
  <th>Тип</th>
  <th>Варианты</th>
  <th>Дефолт</th>
  <th>Пусто</th>
  <th>JSON</th>
  <th>Заметка</th>
</tr>
</thead>
<tbody>

<tr>
  <td>105</td>
  <td><code>[ ]</code></td>
  <td><code>TOURNAMENT_CODE</code></td>
  <td>TOURNAMENT_CODE</td>
  <td>Код слота расписания</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>106</td>
  <td><code>[ ]</code></td>
  <td><code>PERIOD_TYPE</code></td>
  <td>PERIOD_TYPE</td>
  <td>Текст периода (турнир месяца, …)</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>107</td>
  <td><code>[ ]</code></td>
  <td><code>START_DT</code></td>
  <td>START_DT</td>
  <td>Дата старта турнира</td>
  <td>date</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>108</td>
  <td><code>[ ]</code></td>
  <td><code>END_DT</code></td>
  <td>END_DT</td>
  <td>Дата окончания турнира</td>
  <td>date</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>109</td>
  <td><code>[ ]</code></td>
  <td><code>RESULT_DT</code></td>
  <td>RESULT_DT</td>
  <td>Дата подведения итогов турнира</td>
  <td>date</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>110</td>
  <td><code>[ ]</code></td>
  <td><code>PLAN_PERIOD_START_DT</code></td>
  <td>PLAN_PERIOD_START_DT</td>
  <td>YYYY-MM-DD</td>
  <td>date</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>111</td>
  <td><code>[ ]</code></td>
  <td><code>PLAN_PERIOD_END_DT</code></td>
  <td>PLAN_PERIOD_END_DT</td>
  <td>YYYY-MM-DD</td>
  <td>date</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>112</td>
  <td><code>[ ]</code></td>
  <td><code>CRITERION_MARK_TYPE</code></td>
  <td>CRITERION_MARK_TYPE</td>
  <td>&gt;  &#124;  &gt;=</td>
  <td>dropdown</td>
  <td>&gt;, &gt;=</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>113</td>
  <td><code>[ ]</code></td>
  <td><code>CRITERION_MARK_VALUE</code></td>
  <td>CRITERION_MARK_VALUE</td>
  <td>Число (0, 50000, …)</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>114</td>
  <td><code>[ ]</code></td>
  <td><code>FILTER_PERIOD_ARR</code></td>
  <td>FILTER_PERIOD_ARR</td>
  <td>JSON или пусто</td>
  <td>json</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>ячейка JSON</td>
  <td></td>
</tr>

<tr>
  <td>115</td>
  <td><code>[ ]</code></td>
  <td><code>TOURNAMENT_STATUS</code></td>
  <td>TOURNAMENT_STATUS</td>
  <td>АКТИВНЫЙ  &#124;  ЗАВЕРШЕН  &#124;  ОТМЕНЕН  &#124;  ПОДВЕДЕНИЕ ИТОГОВ  &#124;  УДАЛЕН</td>
  <td>dropdown</td>
  <td>АКТИВНЫЙ, ЗАВЕРШЕН, ОТМЕНЕН, ПОДВЕДЕНИЕ ИТОГОВ, УДАЛЕН</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>116</td>
  <td><code>[ ]</code></td>
  <td><code>CONTEST_CODE</code></td>
  <td>CONTEST_CODE</td>
  <td>Код конкурса</td>
  <td>text</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>117</td>
  <td><code>[ ]</code></td>
  <td><code>TARGET_TYPE</code></td>
  <td>TARGET_TYPE</td>
  <td>JSON seasonCode или пусто</td>
  <td>json</td>
  <td>ПРОМ, ТЕСТ</td>
  <td>—</td>
  <td>да</td>
  <td>ячейка JSON</td>
  <td></td>
</tr>

<tr>
  <td>118</td>
  <td><code>[ ]</code></td>
  <td><code>CALC_TYPE</code></td>
  <td>CALC_TYPE</td>
  <td>0  &#124;  1</td>
  <td>dropdown</td>
  <td>0, 1</td>
  <td>—</td>
  <td>да</td>
  <td>—</td>
  <td></td>
</tr>

<tr>
  <td>119</td>
  <td><code>[ ]</code></td>
  <td><code>TRN_INDICATOR_FILTER</code></td>
  <td>TRN_INDICATOR_FILTER</td>
  <td>Часто пусто</td>
  <td>json</td>
  <td>—</td>
  <td>—</td>
  <td>да</td>
  <td>ячейка JSON</td>
  <td></td>
</tr>

</tbody>
</table>


## Сводка

- Всего параметров: **119**
- HTML-таблицы: **Подпись 18%**, **Описание 30%**; варианты через запятую

