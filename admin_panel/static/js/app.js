// Глобальные переменные
let currentFile = null;
let currentPage = 1;
let currentSort = { field: null, order: 'asc' };
let currentSearch = '';
let currentRecordId = null;
let currentJsonField = null;
let fileColumns = {};

// Инициализация
document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM загружен, инициализация...');
    
    // Небольшая задержка для гарантии готовности DOM
    setTimeout(() => {
        const select = document.getElementById('sessionSelect');
        if (select) {
            console.log('Элемент sessionSelect найден, загрузка сессий...');
            loadSessions();
            loadFiles();
        } else {
            console.error('Элемент sessionSelect не найден!');
            // Повторная попытка через секунду
            setTimeout(() => {
                const select = document.getElementById('sessionSelect');
                if (select) {
                    console.log('Элемент sessionSelect найден (повторная попытка), загрузка сессий...');
                    loadSessions();
                    loadFiles();
                } else {
                    console.error('Элемент sessionSelect все еще не найден');
                }
            }, 1000);
        }
    }, 100);
});// Небольшая задержка для гарантии готовности DOM
    setTimeout(() => {
        const select = document.getElementById('sessionSelect');
        if (select) {
            loadSessions();
            loadFiles();
        } else {
            console.error('Элемент sessionSelect не найден');
            // Повторная попытка через секунду
            setTimeout(() => {
                const select = document.getElementById('sessionSelect');
                if (select) {
                    loadSessions();
                    loadFiles();
                } else {
                    console.error('Элемент sessionSelect все еще не найден');
                }
            }, 1000);
        }
    }, 100);
// Работа с сессиями
async function loadSessions() {
    console.log('loadSessions вызвана');
    try {
        // Показываем индикатор загрузки
        const select = document.getElementById('sessionSelect');
        if (select) {
            select.innerHTML = '<option value="">Загрузка сессий...</option>';
        } else {
            console.error('Элемент sessionSelect не найден!');
            return;
        }
        
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 секунд таймаут
        
        console.log('Отправка запроса /api/sessions...');
        const response = await fetch('/api/sessions', {
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        console.log('Ответ получен:', response.status);
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        const columnOrder = data.columns || (data.records && data.records.length > 0 ? Object.keys(data.records[0]) : []);;
        console.log('Данные сессий:', data);
        
        select.innerHTML = '';
        
        if (data.sessions && data.sessions.length > 0) {
            console.log(`Найдено сессий: ${data.sessions.length}`);
            data.sessions.forEach(session => {
                const option = document.createElement('option');
                option.value = session;
                option.textContent = session;
                select.appendChild(option);
            });
            
            // Устанавливаем текущую сессию
            console.log('Запрос текущей сессии...');
            const currentResponse = await fetch('/api/session/current', {
                signal: controller.signal
            });
            const currentData = await currentResponse.json();
            console.log('Текущая сессия:', currentData);
            
            if (currentData.session) {
                select.value = currentData.session;
            } else if (data.sessions.length > 0) {
                // Если нет текущей, выбираем последнюю
                select.value = data.sessions[0];
            }
        } else {
            console.log('Сессии не найдены');
            select.innerHTML = '<option value="">Нет сессий</option>';
        }
    } catch (error) {
        console.error('Ошибка загрузки сессий:', error);
        const select = document.getElementById('sessionSelect');
        if (select) {
            select.innerHTML = '<option value="">Ошибка загрузки</option>';
        }
        alert('Ошибка загрузки сессий: ' + error.message);
    }
}async function switchSession() {
    const select = document.getElementById('sessionSelect');
    const sessionName = select.value;
    
    if (!sessionName) {
        return;
    }
    
    try {
        const response = await fetch(`/api/session/${sessionName}`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.error) {
            throw new Error(result.error);
        }
        
        // Перезагружаем данные
        await loadRecords();
        alert('Сессия изменена: ' + sessionName);
    } catch (error) {
        alert('Ошибка переключения сессии: ' + error.message);
    }
}

async function refreshSessions() {
    await loadSessions();
    alert('Список сессий обновлен');
}

// Обновляем createNewSession


// Удаление сессии
async function deleteSession() {
    const select = document.getElementById('sessionSelect');
    const sessionName = select.value;
    
    if (!sessionName) {
        alert('Выберите сессию для удаления');
        return;
    }
    
    // Запрашиваем информацию о файлах в сессии
    try {
        const infoResponse = await fetch(`/api/session/${encodeURIComponent(sessionName)}/info`);
        let filesList = [];
        
        if (infoResponse.ok) {
            const info = await infoResponse.json();
            filesList = info.files || [];
        }
        
        // Формируем сообщение подтверждения
        let confirmMessage = `Вы уверены, что хотите удалить сессию "${sessionName}"?

`;
        confirmMessage += `Файлы в сессии (${filesList.length}):
`;
        if (filesList.length > 0) {
            filesList.slice(0, 10).forEach(file => {
                confirmMessage += `  • ${file}
`;
            });
            if (filesList.length > 10) {
                confirmMessage += `  ... и еще ${filesList.length - 10} файлов
`;
            }
        } else {
            confirmMessage += `  (файлы не найдены)
`;
        }
        confirmMessage += `
Это действие нельзя отменить!`;
        
        if (!confirm(confirmMessage)) {
            return;
        }
        
        // Удаляем сессию
        const response = await fetch(`/api/session/${encodeURIComponent(sessionName)}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.error) {
            throw new Error(result.error);
        }
        
        // Обновляем список сессий
        await loadSessions();
        
        // Выбираем другую сессию если есть
        const selectAfter = document.getElementById('sessionSelect');
        if (selectAfter.options.length > 0) {
            selectAfter.value = selectAfter.options[0].value;
            await switchSession();
        }
        
        alert(`Сессия "${sessionName}" успешно удалена`);
    } catch (error) {
        alert('Ошибка удаления сессии: ' + error.message);
    }
}


async function createNewSession() {
    console.log('=== createNewSession вызвана ===');
    
    if (!confirm('Создать новую сессию редактирования? Текущие изменения будут сохранены.')) {
        console.log('Пользователь отменил создание сессии');
        return;
    }
    
    try {
        console.log('Отправка POST запроса на /api/session/new...');
    loadFiles();
}const response = await fetch('/api/session/new', { 
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        console.log('Ответ получен:', response.status, response.ok);
        
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`HTTP ${response.status}: ${errorText}`);
        }
        
        const result = await response.json();
        console.log('Результат создания сессии:', result);
        
        if (result.error) {
            throw new Error(result.error);
        }
        
        if (!result.session) {
            throw new Error('Сессия не создана: нет имени сессии в ответе');
        }
        
        console.log('Обновление списка сессий...');
        // Обновляем список сессий
        await loadSessions();
        
        // Выбираем новую сессию
        const select = document.getElementById('sessionSelect');
        if (select && result.session) {
            select.value = result.session;
            console.log('Переключение на новую сессию:', result.session);
            await switchSession();
        }
        
        alert('Новая сессия создана: ' + result.session);
    } catch (error) {
        console.error('Ошибка создания сессии:', error);
        alert('Ошибка создания сессии: ' + error.message);
    }
}async function loadFiles() {
    console.log('loadFiles вызвана');
    try {
        const response = await fetch('/api/files');
        console.log('Ответ /api/files:', response.status);
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        const columnOrder = data.columns || (data.records && data.records.length > 0 ? Object.keys(data.records[0]) : []);;
        console.log('Данные файлов:', data);
        
        if (!data || !data.file_names) {
            throw new Error('Неверный формат ответа: ожидается file_names');
        }
        
        const fileNames = data.file_names;
        const fileKeys = Object.keys(fileNames);
        console.log('Ключи файлов:', fileKeys);
        
        // Ищем контейнер для вкладок (пробуем разные варианты)
        let tabsContainer = document.getElementById('fileTabs') || 
                           document.getElementById('tabsNav') ||
                           document.querySelector('.tabs-nav') ||
                           document.querySelector('.tabs-container');
        
        if (!tabsContainer) {
            console.error('Контейнер вкладок не найден');
            alert('Контейнер вкладок не найден в HTML');
            return;
        }
        
        console.log('Контейнер найден:', tabsContainer.id || tabsContainer.className);
        
        // Очищаем существующие вкладки
        tabsContainer.innerHTML = '';
        
        // Создаем вкладки для каждого файла
        fileKeys.forEach((fileKey, index) => {
            const tab = document.createElement('div');
            tab.className = 'tab' + (index === 0 ? ' active' : '');
            tab.textContent = fileKey;
            tab.onclick = () => {
                console.log('Клик по вкладке:', fileKey);
                switchFile(fileKey);
            };
            tabsContainer.appendChild(tab);
        });
        
        console.log(`Создано вкладок: ${fileKeys.length}`);
        
        // Загружаем первый файл
        if (fileKeys.length > 0) {
            currentFile = fileKeys[0];
            console.log('Переключение на первый файл:', currentFile);
            await switchFile(fileKeys[0]);
        }
    } catch (error) {
        console.error('Ошибка загрузки файлов:', error);
        alert('Ошибка загрузки файлов: ' + error.message);
    }
}async function switchFile(fileKey) {
    currentFile = fileKey;
        loadFieldsForFilter(fileKey);
    currentPage = 1;
    currentSort = { field: null, order: 'asc' };
    currentSearch = '';
    
    // Обновляем активную вкладку
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.toggle('active', tab.textContent === fileKey);
    });
    
    // Загружаем данные
    await loadRecords();
}

// Загрузка записей


// Загрузка значений для поля (для списков и автодополнения)
async function loadFieldValues(fileKey, fieldName) {
    try {
        const response = await fetch(`/api/files/${encodeURIComponent(fileKey)}/field/${encodeURIComponent(fieldName)}/values`);
        const data = await response.json();
        const columnOrder = data.columns || (data.records && data.records.length > 0 ? Object.keys(data.records[0]) : []);;
        return data.values || [];
    } catch (error) {
        console.error(`Ошибка загрузки значений для ${fieldName}:`, error);
        return [];
    }
}

// Создание поля с выбором из списка
function createSelectField

// Создание поля формы с учетом типа (обычное, список, JSON)



(fieldName, fieldValue, values, isMulti = false) {
    const select = document.createElement('select');
    select.name = fieldName;
    select.className = 'form-control';
    select.id = `field_${fieldName}`;
    
    if (isMulti) {
        select.multiple = true;
        select.size = Math.min(values.length, 5);
    }
    
    // Добавляем опции
    values.forEach(value => {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = value;
        
        // Выбираем текущее значение
        if (isMulti) {
            // Для множественного выбора проверяем если значение содержит разделители
            const currentValues = fieldValue ? fieldValue.split(',').map(v => v.trim()) : [];
            if (currentValues.includes(value)) {
                option.selected = true;
            }
        } else {
            if (value === fieldValue) {
                option.selected = true;
            }
        }
        
        select.appendChild(option);
    });
    
    return select;
}


async function loadRecords() {
    if (!currentFile) return;
    
    const tableBody = document.getElementById('tableBody');
    tableBody.innerHTML = '<tr><td colspan="100%" class="loading"><i class="fas fa-spinner"></i> Загрузка...</td></tr>';
    
    try {
        const params = new URLSearchParams({
            page: currentPage,
            per_page: 50,
            search: (() => {
            const searchInput = document.getElementById('searchInput');
            const filterSelect = document.getElementById('filterFieldSelect');
            const searchText = searchInput ? searchInput.value.trim() : '';
            const filterField = filterSelect ? filterSelect.value.trim() : '';
            
            if (filterField && searchText) {
                // Фильтрация по конкретному полю
                return `${filterField}:${searchText}`;
            } else if (searchText) {
                // Поиск по всем полям
                return searchText;
            }
            return '';
        })(),
            sort_by: currentSort.field || '',
            sort_order: currentSort.order
        });

        const response = await fetch(`/api/files/${currentFile}/records?${params.toString()}`);


// Загрузка списка полей для фильтра
function loadFieldsForFilter(fileKey) {
    const select = document.getElementById('filterFieldSelect');
    if (!select) return;
    
    // Очищаем и добавляем "Все поля"
    select.innerHTML = '<option value="">Все поля</option>';
    
    // Получаем колонки из fileColumns или из первой записи
    if (fileColumns[fileKey] && fileColumns[fileKey].length > 0) {
        fileColumns[fileKey].forEach(field => {
            const option = document.createElement('option');
            option.value = field;
            option.textContent = field;
            select.appendChild(option);
        });
    }
}

// Очистка поиска
function clearSearch() {
    const searchInput = document.getElementById('searchInput');
    const filterSelect = document.getElementById('filterFieldSelect');
    
    if (searchInput) searchInput.value = '';
    if (filterSelect) filterSelect.value = '';
    
    currentSearch = '';
    loadRecords();
}
        
        const data = await response.json();
        const columnOrder = data.columns || (data.records && data.records.length > 0 ? Object.keys(data.records[0]) : []);
        const columnOrder = data.columns || (data.records && data.records.length > 0 ? Object.keys(data.records[0]) : []);
        const columnOrder = data.columns || (data.records && data.records.length > 0 ? Object.keys(data.records[0]) : []);
        const columnOrder = data.columns || (data.records && data.records.length > 0 ? Object.keys(data.records[0]) : []);;
        
        if (data.error) {
            throw new Error(data.error);
        }
        
        displayRecords(data.records);
        displayPagination(data);
        updateRecordsCount(data.total);
        updateSortSelect(data.records);
    } catch (error) {
        console.error('Ошибка загрузки записей:', error);
        tableBody.innerHTML = `<tr><td colspan="100%">Ошибка: ${error.message}</td></tr>`;
    }
}

// Отображение записей
function displayRecords(records) {
    const tableHead = document.getElementById('tableHead');
    const tableBody = document.getElementById('tableBody');
    
    if (records.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="100%">Нет данных</td></tr>';
        return;
    }
    
    // Получаем колонки из первой записи
    const columns = columnOrder;
    fileColumns[currentFile] = columns;
    
    // Создаем заголовок таблицы
    tableHead.innerHTML = '<tr>' + 
        columns.map(col => `<th>${col}</th>`).join('') + 
        '<th>Действия</th>' +
        '</tr>';
    
    // Создаем строки таблицы
    tableBody.innerHTML = records.map((record, index) => {
        const recordIndex = (currentPage - 1) * 50 + index;
        return '<tr>' + 
            columns.map(col => {
                const value = record[col];
                if (value === null || value === undefined) return '<td></td>';
                
                // Проверяем, является ли поле JSON
                if (isJsonField(col)) {
                    return `<td><span class="json-badge" onclick="editJsonField(${recordIndex}, '${col}')">[JSON]</span></td>`;
                }
                
                const displayValue = String(value).length > 50 ? 
                    String(value).substring(0, 50) + '...' : String(value);
                return `<td title="${value}">${displayValue}</td>`;
            }).join('') +
            `<td class="action-buttons">
                <button class="btn btn-primary btn-icon" onclick="editRecord(${recordIndex})" title="Редактировать">
                    <i class="fas fa-edit"></i>
                </button>
                <button class="btn btn-danger btn-icon" onclick="deleteRecord(${recordIndex})" title="Удалить">
                    <i class="fas fa-trash"></i>
                </button>
            </td>` +
            '</tr>';
    }).join('');
}

// Проверка, является ли поле JSON
function isJsonField(field) {
    if (!currentFile) return false;
    // Это будет проверяться через конфигурацию на сервере
    // Пока используем простую проверку по имени
    return field.includes('FEATURE') || field.includes('ADD_DATA') || 
           field.includes('PERIOD') || field.includes('FILTER') || 
           field === 'TARGET_TYPE' || field === 'GROUP_VALUE' || field === 'BUSINESS_BLOCK';
}

// Обновление счетчика записей
function updateRecordsCount(total) {
    document.getElementById('recordsCount').textContent = `Всего записей: ${total}`;
}

// Обновление селекта сортировки
function updateSortSelect(records) {
    if (records.length === 0) return;
    
    const sortSelect = document.getElementById('sortSelect');
    const columns = columnOrder;
    
    sortSelect.innerHTML = '<option value="">Сортировка...</option>' +
        columns.map(col => `<option value="${col}">${col}</option>`).join('');
}

// Пагинация
function displayPagination(data) {
    const pagination = document.getElementById('pagination');
    
    if (data.pages <= 1) {
        pagination.innerHTML = '';
        return;
    }
    
    let html = '';
    
    // Кнопка "Предыдущая"
    html += `<button ${currentPage === 1 ? 'disabled' : ''} onclick="changePage(${currentPage - 1})">
        <i class="fas fa-chevron-left"></i>
    </button>`;
    
    // Номера страниц
    for (let i = 1; i <= data.pages; i++) {
        if (i === 1 || i === data.pages || (i >= currentPage - 2 && i <= currentPage + 2)) {
            html += `<button class="${i === currentPage ? 'active' : ''}" onclick="changePage(${i})">${i}</button>`;
        } else if (i === currentPage - 3 || i === currentPage + 3) {
            html += '<span>...</span>';
        }
    }
    
    // Кнопка "Следующая"
    html += `<button ${currentPage === data.pages ? 'disabled' : ''} onclick="changePage(${currentPage + 1})">
        <i class="fas fa-chevron-right"></i>
    </button>`;
    
    html += `<span class="page-info">Страница ${currentPage} из ${data.pages}</span>`;
    
    pagination.innerHTML = html;
}

function changePage(page) {
    currentPage = page;
    loadRecords();
}

// Поиск
function handleSearch() {
    currentSearch = document.getElementById('searchInput').value;
    currentPage = 1;
    loadRecords();
}

// Сортировка
function handleSort() {
    const field = document.getElementById('sortSelect').value;
    if (field) {
        if (currentSort.field === field) {
            currentSort.order = currentSort.order === 'asc' ? 'desc' : 'asc';
        } else {
            currentSort.field = field;
            currentSort.order = 'asc';
        }
        loadRecords();
    }
}

// Показать форму добавления
function showAddForm() {
    currentRecordId = null;
    showEditForm({});
}

// Редактирование записи
async function editRecord(recordId) {
    try {
        const response = await fetch(`/api/files/${currentFile}/records/${recordId}`);
        const record = await response.json();
        
        if (record.error) {
            throw new Error(record.error);
        }
        
        currentRecordId = recordId;
        showEditForm(record);
    } catch (error) {
        alert('Ошибка загрузки записи: ' + error.message);
    }
}

// Показать форму редактирования
async function showEditForm(record) {
    const modal = document.getElementById('editModal');
    const modalTitle = document.getElementById('modalTitle');
    const modalBody = document.getElementById('modalBody');
    
    modalTitle.textContent = currentRecordId === null ? 'Добавление записи' : 'Редактирование записи';
    
    // Получаем колонки
    const columns = fileColumns[currentFile] || Object.keys(record);
    
    // Создаем форму
    let formHtml = '<form id="recordForm">';
    
    columns.forEach(col => {
        if (isJsonField(col)) {
            formHtml += `
                <div class="form-group">
                    <label class="form-label">${col}</label>
                    <div class="json-field-group">
                        <span class="json-badge">JSON поле</span>
                        <button type="button" class="btn btn-primary" onclick="editJsonField(${currentRecordId || 'null'}, '${col}')">
                            <i class="fas fa-code"></i> Редактировать JSON
                        </button>
                    </div>
                </div>
            `;
        } else {
            const value = record[col] || '';
            const inputType = getInputType(col);
            
            if (inputType === 'select') {
                formHtml += createSelectField(col, value);
            } else if (inputType === 'textarea') {
                formHtml += `
                    <div class="form-group">
                        <label class="form-label">${col}</label>
                        <textarea class="form-textarea" name="${col}">${value}</textarea>
                    </div>
                `;
            } else {
                // Специальная обработка для GROUP_CODE в REWARD-LINK
                if (currentFile === 'REWARD-LINK' && col === 'GROUP_CODE') {
                    // Создаем select с множественным выбором (значения загрузим после создания формы)
                    formHtml += `
                        <div class="form-group">
                            <label class="form-label">${col}</label>
                            <select id="field_GROUP_CODE" class="form-input" name="${col}" multiple size="4">
                                <option value="">Загрузка значений...</option>
                            </select>
                        </div>
                    `;
                } else {
                    formHtml += `
                        <div class="form-group">
                            <label class="form-label">${col}</label>
                            <input type="${inputType}" class="form-input" name="${col}" value="${value}">
                        </div>
                    `;
                }
            }
        }
    });
    
    formHtml += '</form>';
    modalBody.innerHTML = formHtml;
    
    // Загружаем значения для GROUP_CODE если нужно
    if (currentFile === 'REWARD-LINK') {
        const groupCodeSelect = document.getElementById('field_GROUP_CODE');
        if (groupCodeSelect) {
            loadFieldValues(currentFile, 'GROUP_CODE').then(values => {
                groupCodeSelect.innerHTML = '';
                const currentValues = (record['GROUP_CODE'] || '').toString().split(',').map(v => v.trim()).filter(v => v);
                values.forEach(val => {
                    const option = document.createElement('option');
                    option.value = val;
                    option.textContent = val;
                    if (currentValues.includes(val)) {
                        option.selected = true;
                    }
                    groupCodeSelect.appendChild(option);
                });
            }).catch(error => {
                console.error('Ошибка загрузки значений GROUP_CODE:', error);
                groupCodeSelect.innerHTML = '<option value="">Ошибка загрузки</option>';
            });
        }
    }
    modal.style.display = 'block';
}

// Определение типа поля
function getInputType(field) {
    // Проверяем списки значений
    // Это будет расширено через конфигурацию
    if (field.includes('DATE') || field.includes('DT')) {
        return 'date';
    }
    if (field.includes('DESCRIPTION') || field.includes('CONDITION')) {
        return 'textarea';
    }
    // Проверяем на выпадающие списки
    if (field === 'BUSINESS_STATUS' || field === 'CONTEST_TYPE' || field === 'REWARD_TYPE') {
        return 'select';
    }
    return 'text';
}

// Создание select поля
function createSelectField

// Создание поля формы с учетом типа (обычное, список, JSON)



(field, value) {
    // Это будет загружаться динамически через API
    let options = '';
    
    // Временные значения
    if (field === 'BUSINESS_STATUS') {
        options = '<option value="АКТИВНЫЙ">АКТИВНЫЙ</option><option value="АРХИВНЫЙ">АРХИВНЫЙ</option>';
    } else if (field === 'CONTEST_TYPE') {
        options = '<option value="ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ">ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ</option>' +
                  '<option value="ТУРНИРНЫЙ">ТУРНИРНЫЙ</option>' +
                  '<option value="ИНДИВИДУАЛЬНЫЙ">ИНДИВИДУАЛЬНЫЙ</option>';
    } else if (field === 'REWARD_TYPE') {
        options = '<option value="ITEM">ITEM</option><option value="BADGE">BADGE</option>' +
                  '<option value="LABEL">LABEL</option><option value="CRYSTAL">CRYSTAL</option>';
    }
    
    return `
        <div class="form-group">
            <label class="form-label">${field}</label>
            <select class="form-select" name="${field}">
                <option value="">Выберите...</option>
                ${options}
            </select>
        </div>
    `;
}

// Сохранение записи
async function saveRecord() {
    const form = document.getElementById('recordForm');
    const formData = new FormData(form);
    const data = {};
    
    formData.forEach((value, key) => {
        data[key] = value;
    });
    
    try {
        let response;
        if (currentRecordId === null) {
            response = await fetch(`/api/files/${currentFile}/records`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });
        } else {
            response = await fetch(`/api/files/${currentFile}/records/${currentRecordId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });
        }
        
        const result = await response.json();
        
        if (result.error) {
            throw new Error(result.error);
        }
        
        closeModal();
        loadRecords();
        alert('Запись сохранена');
    } catch (error) {
        alert('Ошибка сохранения: ' + error.message);
    }
}

// Удаление записи
async function deleteRecord(recordId) {
    if (!confirm('Вы уверены, что хотите удалить эту запись?')) {
        return;
    }
    
    try {
        const response = await fetch(`/api/files/${currentFile}/records/${recordId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.error) {
            throw new Error(result.error);
        }
        
        if (result.dependencies && Object.keys(result.dependencies).length > 0) {
            let depText = 'Удаление этой записи также удалит:\n';
            for (const [file, ids] of Object.entries(result.dependencies)) {
                depText += `- ${ids.length} записей в ${file}\n`;
            }
            if (!confirm(depText + '\nПродолжить?')) {
                return;
            }
        }
        
        loadRecords();
        alert('Запись удалена');
    } catch (error) {
        alert('Ошибка удаления: ' + error.message);
    }
}

// Закрытие модального окна
function closeModal() {
    document.getElementById('editModal').style.display = 'none';
}

// Редактирование JSON поля
async function editJsonField(recordId, field) {
    currentRecordId = recordId;
    currentJsonField = field;
    
    const modal = document.getElementById('jsonModal');
    const modalTitle = document.getElementById('jsonModalTitle');
    const modalBody = document.getElementById('jsonModalBody');
    
    modalTitle.textContent = `Редактирование ${field}`;
    
    try {
        let jsonData = null;
        
        if (recordId !== null) {
            const response = await fetch(`/api/files/${currentFile}/records/${recordId}/json/${field}`);
            const result = await response.json();
            jsonData = result.data;
        }
        
        // Создаем JSON редактор
        modalBody.innerHTML = createJsonEditor(jsonData, field);
        modal.style.display = 'block';
    } catch (error) {
        alert('Ошибка загрузки JSON: ' + error.message);
    }
}

// Создание JSON редактора
function createJsonEditor(jsonData, field) {
    if (!jsonData) {
        return '<p>JSON поле пустое. Нажмите "Добавить ключ" для создания.</p>' +
               '<button class="btn btn-primary" onclick="addJsonKey()">Добавить ключ</button>';
    }
    
    if (Array.isArray(jsonData)) {
        return createJsonArrayEditor(jsonData, field);
    } else if (typeof jsonData === 'object') {
        return createJsonObjectEditor(jsonData, field);
    } else {
        return '<p>Неизвестный тип JSON</p>';
    }
}

// Редактор JSON объекта
function createJsonObjectEditor(obj, field) {
    let html = '<div class="json-editor">';
    
    for (const [key, value] of Object.entries(obj)) {
        html += createJsonKeyValue(key, value);
    }
    
    html += '<button class="btn btn-primary" onclick="addJsonKey()">Добавить ключ</button>';
    html += '</div>';
    
    return html;
}

// Редактор JSON массива
function createJsonArrayEditor(arr, field) {
    // Упрощенная версия - для массивов объектов показываем таблицу
    if (arr.length > 0 && typeof arr[0] === 'object') {
        let html = '<table class="json-array-table"><thead><tr>';
        const keys = Object.keys(arr[0]);
        keys.forEach(key => {
            html += `<th>${key}</th>`;
        });
        html += '<th>Действия</th></tr></thead><tbody>';
        
        arr.forEach((item, index) => {
            html += '<tr>';
            keys.forEach(key => {
                html += `<td><input type="text" value="${item[key] || ''}" data-array-index="${index}" data-key="${key}"></td>`;
            });
            html += `<td><button class="btn btn-danger btn-icon" onclick="removeJsonArrayItem(${index})"><i class="fas fa-trash"></i></button></td>`;
            html += '</tr>';
        });
        
        html += '</tbody></table>';
        html += '<button class="btn btn-primary" onclick="addJsonArrayItem()">Добавить элемент</button>';
        return html;
    } else {
        // Простой массив
        let html = '<div class="json-editor">';
        arr.forEach((item, index) => {
            html += `<div class="json-array-item">
                <input type="text" value="${item}" data-array-index="${index}">
                <button class="btn btn-danger" onclick="removeJsonArrayItem(${index})">Удалить</button>
            </div>`;
        });
        html += '<button class="btn btn-primary" onclick="addJsonArrayItem()">Добавить элемент</button>';
        html += '</div>';
        return html;
    }
}

// Создание ключ-значение для JSON объекта
function createJsonKeyValue(key, value) {
    const valueType = typeof value;
    let valueInput = '';
    
    if (valueType === 'boolean') {
        valueInput = `<select><option value="true" ${value ? 'selected' : ''}>true</option><option value="false" ${!value ? 'selected' : ''}>false</option></select>`;
    } else if (valueType === 'number') {
        valueInput = `<input type="number" value="${value}">`;
    } else {
        valueInput = `<input type="text" value="${value}">`;
    }
    
    return `
        <div class="json-key-value">
            <input type="text" value="${key}" placeholder="Ключ">
            ${valueInput}
            <button class="btn btn-danger" onclick="removeJsonKey(this)">Удалить</button>
        </div>
    `;
}

// Сохранение JSON поля
async function saveJsonField() {
    // Собираем данные из формы
    // Это упрощенная версия, нужно доработать
    alert('Сохранение JSON - в разработке');
    closeJsonModal();
}

function closeJsonModal() {
    document.getElementById('jsonModal').style.display = 'none';
}

// Создание новой сессии


// Удаление сессии
async function deleteSession() {
    const select = document.getElementById('sessionSelect');
    const sessionName = select.value;
    
    if (!sessionName) {
        alert('Выберите сессию для удаления');
        return;
    }
    
    // Запрашиваем информацию о файлах в сессии
    try {
        const infoResponse = await fetch(`/api/session/${encodeURIComponent(sessionName)}/info`);
        let filesList = [];
        
        if (infoResponse.ok) {
            const info = await infoResponse.json();
            filesList = info.files || [];
        }
        
        // Формируем сообщение подтверждения
        let confirmMessage = `Вы уверены, что хотите удалить сессию "${sessionName}"?

`;
        confirmMessage += `Файлы в сессии (${filesList.length}):
`;
        if (filesList.length > 0) {
            filesList.slice(0, 10).forEach(file => {
                confirmMessage += `  • ${file}
`;
            });
            if (filesList.length > 10) {
                confirmMessage += `  ... и еще ${filesList.length - 10} файлов
`;
            }
        } else {
            confirmMessage += `  (файлы не найдены)
`;
        }
        confirmMessage += `
Это действие нельзя отменить!`;
        
        if (!confirm(confirmMessage)) {
            return;
        }
        
        // Удаляем сессию
        const response = await fetch(`/api/session/${encodeURIComponent(sessionName)}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.error) {
            throw new Error(result.error);
        }
        
        // Обновляем список сессий
        await loadSessions();
        
        // Выбираем другую сессию если есть
        const selectAfter = document.getElementById('sessionSelect');
        if (selectAfter.options.length > 0) {
            selectAfter.value = selectAfter.options[0].value;
            await switchSession();
        }
        
        alert(`Сессия "${sessionName}" успешно удалена`);
    } catch (error) {
        alert('Ошибка удаления сессии: ' + error.message);
    }
}


async function createNewSession() {
    if (!confirm('Создать новую сессию редактирования? Текущие изменения будут сохранены.')) {
        return;
    }
    
    try {
        const response = await fetch('/api/session/new', { method: 'POST' });
        const result = await response.json();
        
        if (result.error) {
            throw new Error(result.error);
        }
        
        location.reload();
    } catch (error) {
        alert('Ошибка создания сессии: ' + error.message);
    }
}

// Закрытие модальных окон при клике вне их
window.onclick = function(event) {
    const editModal = document.getElementById('editModal');
    const jsonModal = document.getElementById('jsonModal');
    
    if (event.target === editModal) {
        closeModal();
    }
    if (event.target === jsonModal) {
        closeJsonModal();
    }
}


// Открытие редактора JSON
function openJsonEditor(fileKey, fieldName, currentValue) {
    // Загружаем структуру JSON поля
    fetch(`/api/files/${encodeURIComponent(fileKey)}/json-field/${encodeURIComponent(fieldName)}/structure`)
        .then(response => response.json())
        .then(structure => {
            // Показываем модальное окно для редактирования JSON
            // TODO: Реализовать модальное окно с автодополнением
            const newValue = prompt('Редактирование JSON (в разработке):', currentValue);
            if (newValue !== null) {
                // Обновляем значение в форме
                const fieldElement = document.querySelector(`[name="${fieldName}"]`);
                if (fieldElement) {
                    fieldElement.value = newValue;
                }
            }
        })
        .catch(error => {
            console.error('Ошибка загрузки структуры JSON:', error);
            const newValue = prompt('Редактирование JSON:', currentValue);
            if (newValue !== null) {
                const fieldElement = document.querySelector(`[name="${fieldName}"]`);
                if (fieldElement) {
                    fieldElement.value = newValue;
                }
            }
        });
}


// Создание поля формы с учетом типа (обычное, список, JSON)
async function createFormField(fileKey, fieldName, fieldValue) {
    // Проверяем MULTI_VALUE_FIELDS
    const isMultiValueField = (fileKey === 'REWARD-LINK' && fieldName === 'GROUP_CODE');
    
    if (isMultiValueField) {
        const values = await loadFieldValues(fileKey, fieldName);
        if (values.length > 0) {
            return createSelectField(fieldName, fieldValue, values, true);
        }
    }
    
    // Обычное текстовое поле
    const input = document.createElement('input');
    input.type = 'text';
    input.name = fieldName;
    input.className = 'form-control';
    input.value = fieldValue || '';
    return input;
}


// Делаем функции глобально доступными
if (typeof window !== 'undefined') {
    window.createNewSession = createNewSession;
    window.loadSessions = loadSessions;
    window.switchSession = switchSession;
    window.deleteSession = deleteSession;
    window.loadFiles = loadFiles;
    window.loadRecords = loadRecords;
}
